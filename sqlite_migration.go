package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "modernc.org/sqlite"
)

const (
	defaultInputJSONLPath = "out/annotations.jsonl"
	defaultSQLitePath     = "out/annotations.db"
)

type reportTypeCount struct {
	Type  string
	Count int
}

type reportErrorCount struct {
	Error string
	Count int
}

type reportMetrics struct {
	TotalRows                   int
	TotalConversations          int
	SpeakerMatchCount           int
	SpeakerAccuracyPercent      float64
	SpeakerOKFalseCount         int
	QualitySpeakerMismatchCount int
	EmpathyRanCount             int
	EmpathyPresentCount         int
	EmpathyPresentPercent       float64
	EmpathyTypeDistribution     []reportTypeCount
	TopSpeakerValidationErrors  []reportErrorCount
	TopEmpathyValidationErrors  []reportErrorCount
}

type annotationJSONLRecord struct {
	ConversationID string `json:"conversation_id"`
	ReplicaID      int    `json:"replica_id"`
	SpeakerTrue    string `json:"speaker_true"`
	ReplicaText    string `json:"replica_text"`
	TurnIDs        []int  `json:"turn_ids"`
	Guided         struct {
		UnitSpeaker struct {
			OK               bool            `json:"ok"`
			Attempts         int             `json:"attempts"`
			ValidationErrors []string        `json:"validation_errors"`
			Output           json.RawMessage `json:"output"`
		} `json:"unit_speaker"`
		UnitEmpathy struct {
			Ran              bool            `json:"ran"`
			OK               bool            `json:"ok"`
			Attempts         int             `json:"attempts"`
			ValidationErrors []string        `json:"validation_errors"`
			Output           json.RawMessage `json:"output"`
		} `json:"unit_empathy"`
	} `json:"guided"`
	Meta struct {
		Model            string   `json:"model"`
		TimestampUTC     string   `json:"timestamp_utc"`
		OpenAIRequestIDs []string `json:"openai_request_ids"`
	} `json:"meta"`
}

type mappedAnnotationRow struct {
	conversationID              string
	replicaID                   int
	speakerTrue                 string
	speakerPredicted            string
	speakerMatch                int
	speakerOK                   int
	speakerAttempts             int
	speakerValidationErrorsJSON string
	speakerOutputJSON           string
	empathyRan                  int
	empathyOK                   int
	empathyPresent              int
	empathyType                 string
	empathyConfidence           float64
	empathyAttempts             int
	empathyValidationErrorsJSON string
	empathyOutputJSON           string
	replicaText                 string
	turnIDsJSON                 string
	model                       string
	timestampUTC                string
	requestIDsJSON              string
	rawJSON                     string
}

func MigrateJSONLToSQLite(inJSONLPath, outDBPath string) (int, error) {
	if strings.TrimSpace(inJSONLPath) == "" {
		return 0, fmt.Errorf("in_jsonl is required")
	}
	if strings.TrimSpace(outDBPath) == "" {
		return 0, fmt.Errorf("out_db is required")
	}
	if err := os.MkdirAll(filepath.Dir(outDBPath), 0o755); err != nil {
		return 0, fmt.Errorf("create db directory: %w", err)
	}

	db, err := openSQLite(outDBPath)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	if err := resetAnnotationsSchema(db); err != nil {
		return 0, err
	}

	inFile, err := os.Open(inJSONLPath)
	if err != nil {
		return 0, fmt.Errorf("open jsonl: %w", err)
	}
	defer inFile.Close()

	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(insertAnnotationSQL)
	if err != nil {
		return 0, fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	scanner := bufio.NewScanner(inFile)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)

	inserted := 0
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		rawLine := strings.TrimSpace(scanner.Text())
		if rawLine == "" {
			continue
		}

		var rec annotationJSONLRecord
		if err := json.Unmarshal([]byte(rawLine), &rec); err != nil {
			return 0, fmt.Errorf("parse json line %d: %w", lineNo, err)
		}

		row := mapRecordToRow(rec, rawLine)
		if _, err := stmt.Exec(
			row.conversationID,
			row.replicaID,
			row.speakerTrue,
			row.speakerPredicted,
			row.speakerMatch,
			row.speakerOK,
			row.speakerAttempts,
			row.speakerValidationErrorsJSON,
			row.speakerOutputJSON,
			row.empathyRan,
			row.empathyOK,
			row.empathyPresent,
			row.empathyType,
			row.empathyConfidence,
			row.empathyAttempts,
			row.empathyValidationErrorsJSON,
			row.empathyOutputJSON,
			row.replicaText,
			row.turnIDsJSON,
			row.model,
			row.timestampUTC,
			row.requestIDsJSON,
			row.rawJSON,
		); err != nil {
			return 0, fmt.Errorf("insert line %d: %w", lineNo, err)
		}
		inserted++
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("scan jsonl: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit transaction: %w", err)
	}
	return inserted, nil
}

func BuildReport(dbPath string) (reportMetrics, error) {
	db, err := openSQLite(dbPath)
	if err != nil {
		return reportMetrics{}, err
	}
	defer db.Close()

	return buildReportFromDB(db)
}

func PrintReport(r reportMetrics) {
	fmt.Printf("total_rows=%d\n", r.TotalRows)
	fmt.Printf("total_conversations=%d\n", r.TotalConversations)
	fmt.Printf("speaker_accuracy_percent=%.2f (%d/%d)\n", r.SpeakerAccuracyPercent, r.SpeakerMatchCount, r.TotalRows)
	fmt.Printf("speaker_ok_false_count=%d\n", r.SpeakerOKFalseCount)
	fmt.Printf("quality_speaker_mismatch_count=%d\n", r.QualitySpeakerMismatchCount)
	fmt.Printf("empathy_ran_count=%d\n", r.EmpathyRanCount)
	fmt.Printf("empathy_present_count=%d (%.2f%% of empathy_ran)\n", r.EmpathyPresentCount, r.EmpathyPresentPercent)

	fmt.Println("empathy_type_distribution:")
	if len(r.EmpathyTypeDistribution) == 0 {
		fmt.Println("  none")
	} else {
		for _, item := range r.EmpathyTypeDistribution {
			fmt.Printf("  %s=%d\n", item.Type, item.Count)
		}
	}

	fmt.Println("top_speaker_validation_errors:")
	if len(r.TopSpeakerValidationErrors) == 0 {
		fmt.Println("  none")
	} else {
		for _, item := range r.TopSpeakerValidationErrors {
			fmt.Printf("  %s=%d\n", item.Error, item.Count)
		}
	}

	fmt.Println("top_empathy_validation_errors:")
	if len(r.TopEmpathyValidationErrors) == 0 {
		fmt.Println("  none")
	} else {
		for _, item := range r.TopEmpathyValidationErrors {
			fmt.Printf("  %s=%d\n", item.Error, item.Count)
		}
	}
}

func mapRecordToRow(rec annotationJSONLRecord, rawLine string) mappedAnnotationRow {
	speakerOutput := compactJSON(rec.Guided.UnitSpeaker.Output, "{}")
	empathyOutput := compactJSON(rec.Guided.UnitEmpathy.Output, `{"empathy_present":false,"empathy_type":"none","confidence":0,"evidence":[]}`)

	speakerPredicted := extractSpeakerPredicted(speakerOutput)
	speakerMatch := 0
	if speakerPredicted != "" && speakerPredicted == rec.SpeakerTrue {
		speakerMatch = 1
	}

	empathyPresent, empathyType, empathyConfidence := extractEmpathyFields(empathyOutput)
	return mappedAnnotationRow{
		conversationID:              rec.ConversationID,
		replicaID:                   rec.ReplicaID,
		speakerTrue:                 rec.SpeakerTrue,
		speakerPredicted:            speakerPredicted,
		speakerMatch:                speakerMatch,
		speakerOK:                   boolToInt(rec.Guided.UnitSpeaker.OK),
		speakerAttempts:             rec.Guided.UnitSpeaker.Attempts,
		speakerValidationErrorsJSON: marshalStringArray(rec.Guided.UnitSpeaker.ValidationErrors),
		speakerOutputJSON:           speakerOutput,
		empathyRan:                  boolToInt(rec.Guided.UnitEmpathy.Ran),
		empathyOK:                   boolToInt(rec.Guided.UnitEmpathy.OK),
		empathyPresent:              boolToInt(empathyPresent),
		empathyType:                 empathyType,
		empathyConfidence:           empathyConfidence,
		empathyAttempts:             rec.Guided.UnitEmpathy.Attempts,
		empathyValidationErrorsJSON: marshalStringArray(rec.Guided.UnitEmpathy.ValidationErrors),
		empathyOutputJSON:           empathyOutput,
		replicaText:                 rec.ReplicaText,
		turnIDsJSON:                 marshalIntArray(rec.TurnIDs),
		model:                       rec.Meta.Model,
		timestampUTC:                rec.Meta.TimestampUTC,
		requestIDsJSON:              marshalStringArray(rec.Meta.OpenAIRequestIDs),
		rawJSON:                     rawLine,
	}
}

func buildReportFromDB(db *sql.DB) (reportMetrics, error) {
	var report reportMetrics
	if err := db.QueryRow(`
		SELECT
			COUNT(*) AS total_rows,
			COUNT(DISTINCT conversation_id) AS total_conversations,
			COALESCE(SUM(speaker_match), 0) AS speaker_match_count,
			COALESCE(SUM(CASE WHEN speaker_ok = 0 THEN 1 ELSE 0 END), 0) AS speaker_ok_false_count,
			COALESCE(SUM(empathy_ran), 0) AS empathy_ran_count,
			COALESCE(SUM(empathy_present), 0) AS empathy_present_count
		FROM annotations
	`).Scan(
		&report.TotalRows,
		&report.TotalConversations,
		&report.SpeakerMatchCount,
		&report.SpeakerOKFalseCount,
		&report.EmpathyRanCount,
		&report.EmpathyPresentCount,
	); err != nil {
		return reportMetrics{}, fmt.Errorf("query basic metrics: %w", err)
	}

	if report.TotalRows > 0 {
		report.SpeakerAccuracyPercent = float64(report.SpeakerMatchCount) * 100 / float64(report.TotalRows)
	}
	if report.EmpathyRanCount > 0 {
		report.EmpathyPresentPercent = float64(report.EmpathyPresentCount) * 100 / float64(report.EmpathyRanCount)
	}

	typeRows, err := db.Query(`SELECT empathy_type, COUNT(*) FROM annotations GROUP BY empathy_type ORDER BY empathy_type`)
	if err != nil {
		return reportMetrics{}, fmt.Errorf("query empathy distribution: %w", err)
	}
	defer typeRows.Close()

	for typeRows.Next() {
		var item reportTypeCount
		if err := typeRows.Scan(&item.Type, &item.Count); err != nil {
			return reportMetrics{}, fmt.Errorf("scan empathy distribution: %w", err)
		}
		report.EmpathyTypeDistribution = append(report.EmpathyTypeDistribution, item)
	}
	if err := typeRows.Err(); err != nil {
		return reportMetrics{}, fmt.Errorf("iterate empathy distribution: %w", err)
	}

	speakerErrCounts := map[string]int{}
	empathyErrCounts := map[string]int{}

	validationRows, err := db.Query(`SELECT speaker_validation_errors_json, empathy_validation_errors_json FROM annotations`)
	if err != nil {
		return reportMetrics{}, fmt.Errorf("query validation errors: %w", err)
	}
	defer validationRows.Close()

	for validationRows.Next() {
		var speakerJSON string
		var empathyJSON string
		if err := validationRows.Scan(&speakerJSON, &empathyJSON); err != nil {
			return reportMetrics{}, fmt.Errorf("scan validation errors: %w", err)
		}

		for _, item := range parseStringArray(speakerJSON) {
			speakerErrCounts[item]++
		}
		for _, item := range parseStringArray(empathyJSON) {
			empathyErrCounts[item]++
		}
	}
	if err := validationRows.Err(); err != nil {
		return reportMetrics{}, fmt.Errorf("iterate validation errors: %w", err)
	}

	report.QualitySpeakerMismatchCount = speakerErrCounts["quality:speaker_mismatch"]
	report.TopSpeakerValidationErrors = toSortedErrorCounts(speakerErrCounts)
	report.TopEmpathyValidationErrors = toSortedErrorCounts(empathyErrCounts)
	return report, nil
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

func resetAnnotationsSchema(db *sql.DB) error {
	statements := []string{
		`DROP TABLE IF EXISTS annotations`,
		`CREATE TABLE annotations (
			conversation_id TEXT NOT NULL,
			replica_id INTEGER NOT NULL,
			speaker_true TEXT NOT NULL,
			speaker_predicted TEXT,
			speaker_match INTEGER NOT NULL,
			speaker_ok INTEGER NOT NULL,
			speaker_attempts INTEGER NOT NULL,
			speaker_validation_errors_json TEXT NOT NULL,
			speaker_output_json TEXT NOT NULL,
			empathy_ran INTEGER NOT NULL,
			empathy_ok INTEGER NOT NULL,
			empathy_present INTEGER NOT NULL,
			empathy_type TEXT NOT NULL,
			empathy_confidence REAL NOT NULL,
			empathy_attempts INTEGER NOT NULL,
			empathy_validation_errors_json TEXT NOT NULL,
			empathy_output_json TEXT NOT NULL,
			replica_text TEXT NOT NULL,
			turn_ids_json TEXT NOT NULL,
			model TEXT NOT NULL,
			timestamp_utc TEXT NOT NULL,
			request_ids_json TEXT NOT NULL,
			raw_json TEXT NOT NULL,
			PRIMARY KEY (conversation_id, replica_id)
		)`,
		`CREATE INDEX idx_annotations_speaker_match ON annotations(speaker_match)`,
		`CREATE INDEX idx_annotations_speaker_ok ON annotations(speaker_ok)`,
		`CREATE INDEX idx_annotations_empathy_type ON annotations(empathy_type)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			return fmt.Errorf("apply schema statement: %w", err)
		}
	}
	return nil
}

func extractSpeakerPredicted(raw string) string {
	var out struct {
		PredictedSpeaker string `json:"predicted_speaker"`
	}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return ""
	}
	return out.PredictedSpeaker
}

func extractEmpathyFields(raw string) (present bool, empathyType string, confidence float64) {
	var out struct {
		EmpathyPresent bool    `json:"empathy_present"`
		EmpathyType    string  `json:"empathy_type"`
		Confidence     float64 `json:"confidence"`
	}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return false, "none", 0
	}
	if out.EmpathyType == "" {
		out.EmpathyType = "none"
	}
	return out.EmpathyPresent, out.EmpathyType, out.Confidence
}

func compactJSON(raw json.RawMessage, fallback string) string {
	if len(raw) == 0 {
		return fallback
	}
	var data any
	if err := json.Unmarshal(raw, &data); err != nil {
		return fallback
	}
	out, err := json.Marshal(data)
	if err != nil {
		return fallback
	}
	return string(out)
}

func marshalStringArray(items []string) string {
	if items == nil {
		items = []string{}
	}
	out, err := json.Marshal(items)
	if err != nil {
		return "[]"
	}
	return string(out)
}

func marshalIntArray(items []int) string {
	if items == nil {
		items = []int{}
	}
	out, err := json.Marshal(items)
	if err != nil {
		return "[]"
	}
	return string(out)
}

func parseStringArray(raw string) []string {
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return out
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func toSortedErrorCounts(counts map[string]int) []reportErrorCount {
	out := make([]reportErrorCount, 0, len(counts))
	for key, count := range counts {
		out = append(out, reportErrorCount{Error: key, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Error < out[j].Error
		}
		return out[i].Count > out[j].Count
	})
	return out
}

const insertAnnotationSQL = `
INSERT INTO annotations (
	conversation_id,
	replica_id,
	speaker_true,
	speaker_predicted,
	speaker_match,
	speaker_ok,
	speaker_attempts,
	speaker_validation_errors_json,
	speaker_output_json,
	empathy_ran,
	empathy_ok,
	empathy_present,
	empathy_type,
	empathy_confidence,
	empathy_attempts,
	empathy_validation_errors_json,
	empathy_output_json,
	replica_text,
	turn_ids_json,
	model,
	timestamp_utc,
	request_ids_json,
	raw_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`
