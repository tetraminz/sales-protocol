package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "modernc.org/sqlite"
)

const (
	defaultSQLitePath    = "out/annotations.db"
	defaultInputDir      = "/Users/ablackman/data/sales-transcripts/data/chunked_transcripts"
	defaultAnnotateModel = "gpt-4.1-mini"
	defaultOpenAIBaseURL = "https://api.openai.com"
	speakerCustomer      = "Customer"

	reviewStatusPending = "pending"
	reviewStatusOK      = "ok"
	reviewStatusNotOK   = "not_ok"
)

const createAnnotationsTableSQL = `
CREATE TABLE IF NOT EXISTS annotations (
	conversation_id TEXT NOT NULL,
	replica_id INTEGER NOT NULL,
	speaker_true TEXT NOT NULL,
	speaker_predicted TEXT NOT NULL,
	speaker_confidence REAL NOT NULL,
	speaker_match INTEGER NOT NULL,
	empathy_confidence REAL NOT NULL,
	empathy_review_status TEXT NOT NULL DEFAULT 'pending',
	empathy_reviewer_note TEXT NOT NULL DEFAULT '',
	replica_text TEXT NOT NULL,
	model TEXT NOT NULL,
	annotated_at_utc TEXT NOT NULL,
	PRIMARY KEY (conversation_id, replica_id)
)`

const createAnnotateLogsTableSQL = `
CREATE TABLE IF NOT EXISTS annotate_logs (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	created_at_utc TEXT NOT NULL,
	conversation_id TEXT NOT NULL,
	replica_id INTEGER NOT NULL,
	stage TEXT NOT NULL,
	status TEXT NOT NULL,
	message TEXT NOT NULL,
	raw_json TEXT NOT NULL,
	model TEXT NOT NULL
)`

var createAnnotationsIndexesSQL = []string{
	`CREATE INDEX IF NOT EXISTS idx_annotations_speaker_match ON annotations(speaker_match)`,
	`CREATE INDEX IF NOT EXISTS idx_annotations_empathy_review_status ON annotations(empathy_review_status)`,
	`CREATE INDEX IF NOT EXISTS idx_annotations_empathy_confidence ON annotations(empathy_confidence)`,
}

var createAnnotateLogsIndexesSQL = []string{
	`CREATE INDEX IF NOT EXISTS idx_annotate_logs_conversation_replica ON annotate_logs(conversation_id, replica_id)`,
	`CREATE INDEX IF NOT EXISTS idx_annotate_logs_status ON annotate_logs(status)`,
}

const dropAnnotationsSQL = `DROP TABLE IF EXISTS annotations`
const dropAnnotateLogsSQL = `DROP TABLE IF EXISTS annotate_logs`
const dropRunsSQL = `DROP TABLE IF EXISTS annotation_runs`
const deleteAnnotationsSQL = `DELETE FROM annotations`
const deleteAnnotateLogsSQL = `DELETE FROM annotate_logs`

const insertAnnotationSQL = `
INSERT INTO annotations (
	conversation_id,
	replica_id,
	speaker_true,
	speaker_predicted,
	speaker_confidence,
	speaker_match,
	empathy_confidence,
	empathy_review_status,
	empathy_reviewer_note,
	replica_text,
	model,
	annotated_at_utc
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

const insertAnnotateLogSQL = `
INSERT INTO annotate_logs (
	created_at_utc,
	conversation_id,
	replica_id,
	stage,
	status,
	message,
	raw_json,
	model
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

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

func ensureAnnotationsSchema(db *sql.DB) error {
	if _, err := db.Exec(createAnnotationsTableSQL); err != nil {
		return fmt.Errorf("create annotations table: %w", err)
	}
	missing, err := missingAnnotationsColumns(db)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf(
			"incompatible annotations schema, missing columns: %s; run `go run . setup --db <path>`",
			strings.Join(missing, ", "),
		)
	}
	for _, stmt := range createAnnotationsIndexesSQL {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("create annotations index: %w", err)
		}
	}
	if err := ensureAnnotateLogsSchema(db); err != nil {
		return err
	}
	return nil
}

func ensureAnnotateLogsSchema(db *sql.DB) error {
	if _, err := db.Exec(createAnnotateLogsTableSQL); err != nil {
		return fmt.Errorf("create annotate_logs table: %w", err)
	}
	missing, err := missingAnnotateLogsColumns(db)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf(
			"incompatible annotate_logs schema, missing columns: %s; run `go run . setup --db <path>`",
			strings.Join(missing, ", "),
		)
	}
	for _, stmt := range createAnnotateLogsIndexesSQL {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("create annotate_logs index: %w", err)
		}
	}
	return nil
}

func missingAnnotationsColumns(db *sql.DB) ([]string, error) {
	required := []string{
		"conversation_id",
		"replica_id",
		"speaker_true",
		"speaker_predicted",
		"speaker_confidence",
		"speaker_match",
		"empathy_confidence",
		"empathy_review_status",
		"empathy_reviewer_note",
		"replica_text",
		"model",
		"annotated_at_utc",
	}

	rows, err := db.Query(`PRAGMA table_info(annotations)`)
	if err != nil {
		return nil, fmt.Errorf("inspect annotations schema: %w", err)
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
			return nil, fmt.Errorf("scan annotations schema: %w", err)
		}
		existing[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate annotations schema: %w", err)
	}

	var missing []string
	for _, col := range required {
		if _, ok := existing[col]; !ok {
			missing = append(missing, col)
		}
	}
	return missing, nil
}

func missingAnnotateLogsColumns(db *sql.DB) ([]string, error) {
	required := []string{
		"id",
		"created_at_utc",
		"conversation_id",
		"replica_id",
		"stage",
		"status",
		"message",
		"raw_json",
		"model",
	}

	rows, err := db.Query(`PRAGMA table_info(annotate_logs)`)
	if err != nil {
		return nil, fmt.Errorf("inspect annotate_logs schema: %w", err)
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
			return nil, fmt.Errorf("scan annotate_logs schema: %w", err)
		}
		existing[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate annotate_logs schema: %w", err)
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
	if _, err := db.Exec(dropAnnotateLogsSQL); err != nil {
		return fmt.Errorf("drop annotate_logs table: %w", err)
	}
	if _, err := db.Exec(dropRunsSQL); err != nil {
		return fmt.Errorf("drop annotation_runs table: %w", err)
	}
	if err := ensureAnnotationsSchema(db); err != nil {
		return err
	}
	return nil
}
