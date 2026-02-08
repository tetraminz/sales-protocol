package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const createAnnotationsTableSQL = `
CREATE TABLE IF NOT EXISTS annotations (
	conversation_id TEXT NOT NULL,
	utterance_index INTEGER NOT NULL,
	utterance_text TEXT NOT NULL,
	ground_truth_speaker TEXT NOT NULL,
	predicted_speaker TEXT NOT NULL,
	predicted_speaker_confidence REAL NOT NULL,
	speaker_is_correct_raw INTEGER NOT NULL,
	speaker_is_correct_final INTEGER NOT NULL,
	speaker_quality_decision TEXT NOT NULL,
	farewell_is_current_utterance INTEGER NOT NULL,
	farewell_is_conversation_closing INTEGER NOT NULL,
	farewell_context_source TEXT NOT NULL,
	speaker_evidence_quote TEXT NOT NULL,
	speaker_evidence_is_valid INTEGER NOT NULL,
	empathy_applicable INTEGER NOT NULL,
	empathy_present INTEGER NOT NULL,
	empathy_confidence REAL NOT NULL,
	empathy_evidence_quote TEXT NOT NULL,
	empathy_review_status TEXT NOT NULL DEFAULT 'pending',
	empathy_reviewer_note TEXT NOT NULL DEFAULT '',
	model TEXT NOT NULL,
	annotated_at_utc TEXT NOT NULL,
	PRIMARY KEY (conversation_id, utterance_index)
)`

const createLLMEventsTableSQL = `
CREATE TABLE IF NOT EXISTS llm_events (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	created_at_utc TEXT NOT NULL,
	conversation_id TEXT NOT NULL,
	utterance_index INTEGER NOT NULL,
	unit_name TEXT NOT NULL,
	attempt INTEGER NOT NULL,
	model TEXT NOT NULL,
	request_json TEXT NOT NULL,
	response_http_status INTEGER NOT NULL,
	response_json TEXT NOT NULL,
	extracted_content_json TEXT NOT NULL,
	parse_ok INTEGER NOT NULL,
	validation_ok INTEGER NOT NULL,
	error_message TEXT NOT NULL
)`

var createAnnotationsIndexesSQL = []string{
	`CREATE INDEX IF NOT EXISTS idx_annotations_speaker_is_correct_raw ON annotations(speaker_is_correct_raw)`,
	`CREATE INDEX IF NOT EXISTS idx_annotations_speaker_is_correct_final ON annotations(speaker_is_correct_final)`,
	`CREATE INDEX IF NOT EXISTS idx_annotations_speaker_quality_decision ON annotations(speaker_quality_decision)`,
	`CREATE INDEX IF NOT EXISTS idx_annotations_empathy_review_status ON annotations(empathy_review_status, empathy_applicable)`,
}

var createLLMEventsIndexesSQL = []string{
	`CREATE INDEX IF NOT EXISTS idx_llm_events_lookup ON llm_events(conversation_id, utterance_index, unit_name, attempt)`,
	`CREATE INDEX IF NOT EXISTS idx_llm_events_parse_validation ON llm_events(parse_ok, validation_ok)`,
}

const dropAnnotationsSQL = `DROP TABLE IF EXISTS annotations`
const dropLLMEventsSQL = `DROP TABLE IF EXISTS llm_events`
const dropLegacyAnnotateLogsSQL = `DROP TABLE IF EXISTS annotate_logs`
const dropLegacyRunsSQL = `DROP TABLE IF EXISTS annotation_runs`
const deleteAnnotationsSQL = `DELETE FROM annotations`
const deleteLLMEventsSQL = `DELETE FROM llm_events`

const insertAnnotationSQL = `
INSERT INTO annotations (
	conversation_id,
	utterance_index,
	utterance_text,
	ground_truth_speaker,
	predicted_speaker,
	predicted_speaker_confidence,
	speaker_is_correct_raw,
	speaker_is_correct_final,
	speaker_quality_decision,
	farewell_is_current_utterance,
	farewell_is_conversation_closing,
	farewell_context_source,
	speaker_evidence_quote,
	speaker_evidence_is_valid,
	empathy_applicable,
	empathy_present,
	empathy_confidence,
	empathy_evidence_quote,
	empathy_review_status,
	empathy_reviewer_note,
	model,
	annotated_at_utc
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

const insertLLMEventSQL = `
INSERT INTO llm_events (
	created_at_utc,
	conversation_id,
	utterance_index,
	unit_name,
	attempt,
	model,
	request_json,
	response_http_status,
	response_json,
	extracted_content_json,
	parse_ok,
	validation_ok,
	error_message
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

// SQLiteStore — минимальная обертка для записи annotations и llm_events.
type SQLiteStore struct {
	db *sql.DB
}

func OpenSQLiteStore(dbPath string) (*SQLiteStore, error) {
	db, err := openSQLite(dbPath)
	if err != nil {
		return nil, err
	}
	if err := ensureStoreSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *SQLiteStore) ResetForAnnotateRun() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sqlite store is not initialized")
	}
	if _, err := s.db.Exec(deleteAnnotationsSQL); err != nil {
		return fmt.Errorf("clear annotations: %w", err)
	}
	if _, err := s.db.Exec(deleteLLMEventsSQL); err != nil {
		return fmt.Errorf("clear llm_events: %w", err)
	}
	return nil
}

func (s *SQLiteStore) InsertAnnotation(row AnnotationRow) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sqlite store is not initialized")
	}
	if strings.TrimSpace(row.AnnotatedAtUTC) == "" {
		row.AnnotatedAtUTC = time.Now().UTC().Format(time.RFC3339)
	}
	if strings.TrimSpace(row.EmpathyReviewStatus) == "" {
		if row.EmpathyApplicable {
			row.EmpathyReviewStatus = reviewStatusPending
		} else {
			row.EmpathyReviewStatus = reviewStatusNotApplicable
		}
	}
	row.ConversationID = strings.TrimSpace(row.ConversationID)
	row.UtteranceText = strings.TrimSpace(row.UtteranceText)
	row.GroundTruthSpeaker = canonicalSpeakerLabel(row.GroundTruthSpeaker)
	row.PredictedSpeaker = canonicalSpeakerLabel(row.PredictedSpeaker)
	row.PredictedSpeakerConfidence = clamp01(row.PredictedSpeakerConfidence)
	row.FarewellContextSource = normalizeFarewellContextSource(row.FarewellContextSource)
	row.SpeakerEvidenceQuote = strings.TrimSpace(row.SpeakerEvidenceQuote)
	row.EmpathyConfidence = clamp01(row.EmpathyConfidence)
	row.EmpathyEvidenceQuote = strings.TrimSpace(row.EmpathyEvidenceQuote)
	row.Model = strings.TrimSpace(row.Model)
	row.EmpathyReviewerNote = strings.TrimSpace(row.EmpathyReviewerNote)

	if _, err := s.db.Exec(
		insertAnnotationSQL,
		row.ConversationID,
		row.UtteranceIndex,
		row.UtteranceText,
		row.GroundTruthSpeaker,
		row.PredictedSpeaker,
		row.PredictedSpeakerConfidence,
		boolToInt(row.SpeakerIsCorrectRaw),
		boolToInt(row.SpeakerIsCorrectFinal),
		row.SpeakerQualityDecision,
		boolToInt(row.FarewellIsCurrentUtterance),
		boolToInt(row.FarewellIsConversationClosing),
		row.FarewellContextSource,
		row.SpeakerEvidenceQuote,
		boolToInt(row.SpeakerEvidenceIsValid),
		boolToInt(row.EmpathyApplicable),
		boolToInt(row.EmpathyPresent),
		row.EmpathyConfidence,
		row.EmpathyEvidenceQuote,
		row.EmpathyReviewStatus,
		row.EmpathyReviewerNote,
		row.Model,
		row.AnnotatedAtUTC,
	); err != nil {
		return fmt.Errorf("insert annotation: %w", err)
	}
	return nil
}

func (s *SQLiteStore) InsertLLMEvent(event LLMEvent) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sqlite store is not initialized")
	}
	if strings.TrimSpace(event.CreatedAtUTC) == "" {
		event.CreatedAtUTC = time.Now().UTC().Format(time.RFC3339)
	}
	if event.Attempt < 1 {
		event.Attempt = 1
	}
	event.ConversationID = strings.TrimSpace(event.ConversationID)
	event.UnitName = strings.TrimSpace(event.UnitName)
	event.Model = strings.TrimSpace(event.Model)
	event.ErrorMessage = strings.TrimSpace(event.ErrorMessage)
	if strings.TrimSpace(event.RequestJSON) == "" {
		event.RequestJSON = "{}"
	}
	if strings.TrimSpace(event.ResponseJSON) == "" {
		event.ResponseJSON = "{}"
	}

	if _, err := s.db.Exec(
		insertLLMEventSQL,
		event.CreatedAtUTC,
		event.ConversationID,
		event.UtteranceIndex,
		event.UnitName,
		event.Attempt,
		event.Model,
		event.RequestJSON,
		event.ResponseHTTPStatus,
		event.ResponseJSON,
		event.ExtractedContentJSON,
		boolToInt(event.ParseOK),
		boolToInt(event.ValidationOK),
		event.ErrorMessage,
	); err != nil {
		return fmt.Errorf("insert llm event: %w", err)
	}
	return nil
}

func openSQLite(dbPath string) (*sql.DB, error) {
	if strings.TrimSpace(dbPath) == "" {
		return nil, fmt.Errorf("db path is required")
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite db: %w", err)
	}
	return db, nil
}

func ensureStoreSchema(db *sql.DB) error {
	if _, err := db.Exec(createAnnotationsTableSQL); err != nil {
		return fmt.Errorf("create annotations table: %w", err)
	}
	if _, err := db.Exec(createLLMEventsTableSQL); err != nil {
		return fmt.Errorf("create llm_events table: %w", err)
	}

	missingAnnotations, err := missingTableColumns(db, "annotations", requiredAnnotationColumns())
	if err != nil {
		return err
	}
	if len(missingAnnotations) > 0 {
		sort.Strings(missingAnnotations)
		return fmt.Errorf(
			"incompatible annotations schema, missing columns: %s; run `go run . setup --db <path>`",
			strings.Join(missingAnnotations, ", "),
		)
	}

	missingEvents, err := missingTableColumns(db, "llm_events", requiredLLMEventColumns())
	if err != nil {
		return err
	}
	if len(missingEvents) > 0 {
		sort.Strings(missingEvents)
		return fmt.Errorf(
			"incompatible llm_events schema, missing columns: %s; run `go run . setup --db <path>`",
			strings.Join(missingEvents, ", "),
		)
	}

	for _, stmt := range createAnnotationsIndexesSQL {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("create annotations index: %w", err)
		}
	}
	for _, stmt := range createLLMEventsIndexesSQL {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("create llm_events index: %w", err)
		}
	}
	return nil
}

func requiredAnnotationColumns() []string {
	return []string{
		"conversation_id",
		"utterance_index",
		"utterance_text",
		"ground_truth_speaker",
		"predicted_speaker",
		"predicted_speaker_confidence",
		"speaker_is_correct_raw",
		"speaker_is_correct_final",
		"speaker_quality_decision",
		"farewell_is_current_utterance",
		"farewell_is_conversation_closing",
		"farewell_context_source",
		"speaker_evidence_quote",
		"speaker_evidence_is_valid",
		"empathy_applicable",
		"empathy_present",
		"empathy_confidence",
		"empathy_evidence_quote",
		"empathy_review_status",
		"empathy_reviewer_note",
		"model",
		"annotated_at_utc",
	}
}

func requiredLLMEventColumns() []string {
	return []string{
		"id",
		"created_at_utc",
		"conversation_id",
		"utterance_index",
		"unit_name",
		"attempt",
		"model",
		"request_json",
		"response_http_status",
		"response_json",
		"extracted_content_json",
		"parse_ok",
		"validation_ok",
		"error_message",
	}
}

func missingTableColumns(db *sql.DB, tableName string, required []string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info(%s)`, tableName))
	if err != nil {
		return nil, fmt.Errorf("inspect %s schema: %w", tableName, err)
	}
	defer rows.Close()

	existing := map[string]struct{}{}
	for rows.Next() {
		var cid int
		var name string
		var colType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultValue, &pk); err != nil {
			return nil, fmt.Errorf("scan %s schema: %w", tableName, err)
		}
		existing[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s schema: %w", tableName, err)
	}

	var missing []string
	for _, col := range required {
		if _, ok := existing[col]; !ok {
			missing = append(missing, col)
		}
	}
	return missing, nil
}

func SetupSQLite(dbPath string) error {
	if strings.TrimSpace(dbPath) == "" {
		return fmt.Errorf("db path is required")
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return fmt.Errorf("create db directory: %w", err)
	}
	db, err := openSQLite(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec(dropAnnotationsSQL); err != nil {
		return fmt.Errorf("drop annotations table: %w", err)
	}
	if _, err := db.Exec(dropLLMEventsSQL); err != nil {
		return fmt.Errorf("drop llm_events table: %w", err)
	}
	if _, err := db.Exec(dropLegacyAnnotateLogsSQL); err != nil {
		return fmt.Errorf("drop legacy annotate_logs table: %w", err)
	}
	if _, err := db.Exec(dropLegacyRunsSQL); err != nil {
		return fmt.Errorf("drop legacy annotation_runs table: %w", err)
	}
	if err := ensureStoreSchema(db); err != nil {
		return err
	}
	return nil
}
