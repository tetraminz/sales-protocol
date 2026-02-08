package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestSetupSQLite_HardResetCreatesMinimalSchema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS annotation_runs (run_id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("create old annotation_runs table: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS annotations (legacy_col TEXT)`); err != nil {
		t.Fatalf("create old annotations table: %v", err)
	}

	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}

	db, err = openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite after setup: %v", err)
	}
	defer db.Close()

	if tableExists(t, db, "annotation_runs") {
		t.Fatalf("annotation_runs table should be removed")
	}
	if !tableExists(t, db, "annotations") {
		t.Fatalf("annotations table should exist")
	}

	cols := annotationColumns(t, db)
	want := []string{
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
	if strings.Join(cols, ",") != strings.Join(want, ",") {
		t.Fatalf("columns=%v want=%v", cols, want)
	}
}

func TestSetupSQLite_CreatesAnnotateLogsTable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}

	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	if !tableExists(t, db, "annotate_logs") {
		t.Fatalf("annotate_logs table should exist")
	}
	if !indexExists(t, db, "idx_annotate_logs_conversation_replica") {
		t.Fatalf("idx_annotate_logs_conversation_replica should exist")
	}
	if !indexExists(t, db, "idx_annotate_logs_status") {
		t.Fatalf("idx_annotate_logs_status should exist")
	}
}

func TestAnnotate_RespectsFromToInclusive(t *testing.T) {
	tmp := t.TempDir()
	csvDir := filepath.Join(tmp, "csv")
	dbPath := filepath.Join(tmp, "annotations.db")
	mock := newStructuredMockServer(t)
	defer mock.Close()

	mustWriteDialogCSV(t, csvDir, "a.csv", "conv_a", speakerSalesRep, speakerCustomer)
	mustWriteDialogCSV(t, csvDir, "b.csv", "conv_b", speakerSalesRep, speakerCustomer)
	mustWriteDialogCSV(t, csvDir, "c.csv", "conv_c", speakerSalesRep, speakerCustomer)

	err := AnnotateToSQLite(context.Background(), AnnotateConfig{
		DBPath:   dbPath,
		InputDir: csvDir,
		FromIdx:  2,
		ToIdx:    3,
		Model:    defaultAnnotateModel,
		APIKey:   "test_key",
		BaseURL:  mock.URL,
	})
	if err != nil {
		t.Fatalf("annotate: %v", err)
	}

	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	got := distinctConversationIDs(t, db)
	want := []string{"conv_b", "conv_c"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("conversation ids=%v want=%v", got, want)
	}
}

func TestAnnotate_ReplacesPreviousRows(t *testing.T) {
	tmp := t.TempDir()
	csvDir := filepath.Join(tmp, "csv")
	dbPath := filepath.Join(tmp, "annotations.db")
	mock := newStructuredMockServer(t)
	defer mock.Close()

	mustWriteDialogCSV(t, csvDir, "a.csv", "conv_a", speakerSalesRep, speakerCustomer)
	mustWriteDialogCSV(t, csvDir, "b.csv", "conv_b", speakerSalesRep, speakerCustomer)

	err := AnnotateToSQLite(context.Background(), AnnotateConfig{
		DBPath:   dbPath,
		InputDir: csvDir,
		FromIdx:  1,
		ToIdx:    1,
		Model:    defaultAnnotateModel,
		APIKey:   "test_key",
		BaseURL:  mock.URL,
	})
	if err != nil {
		t.Fatalf("first annotate: %v", err)
	}

	err = AnnotateToSQLite(context.Background(), AnnotateConfig{
		DBPath:   dbPath,
		InputDir: csvDir,
		FromIdx:  2,
		ToIdx:    2,
		Model:    defaultAnnotateModel,
		APIKey:   "test_key",
		BaseURL:  mock.URL,
	})
	if err != nil {
		t.Fatalf("second annotate: %v", err)
	}

	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	got := distinctConversationIDs(t, db)
	want := []string{"conv_b"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("conversation ids=%v want=%v", got, want)
	}
}

func TestSpeakerCanonicalization_StripsMarkdown(t *testing.T) {
	got := canonicalSpeakerLabel(" **Sales Rep ")
	if got != speakerSalesRep {
		t.Fatalf("canonical speaker label=%q want=%q", got, speakerSalesRep)
	}
	got = canonicalSpeakerLabel("**Customer")
	if got != speakerCustomer {
		t.Fatalf("canonical speaker label=%q want=%q", got, speakerCustomer)
	}
}

func TestEmpathyConfidence_DefaultPendingReview(t *testing.T) {
	tmp := t.TempDir()
	csvDir := filepath.Join(tmp, "csv")
	dbPath := filepath.Join(tmp, "annotations.db")
	mock := newStructuredMockServer(t)
	defer mock.Close()

	mustWriteDialogCSV(t, csvDir, "a.csv", "conv_a", "**Sales Rep", "**Customer")

	err := AnnotateToSQLite(context.Background(), AnnotateConfig{
		DBPath:   dbPath,
		InputDir: csvDir,
		FromIdx:  1,
		ToIdx:    1,
		Model:    defaultAnnotateModel,
		APIKey:   "test_key",
		BaseURL:  mock.URL,
	})
	if err != nil {
		t.Fatalf("annotate: %v", err)
	}

	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	var empathyConf float64
	var reviewStatus string
	var reviewNote string
	if err := db.QueryRow(`
		SELECT empathy_confidence, empathy_review_status, empathy_reviewer_note
		FROM annotations
		WHERE conversation_id = ? AND replica_id = 1
	`, "conv_a").Scan(&empathyConf, &reviewStatus, &reviewNote); err != nil {
		t.Fatalf("query sales rep row: %v", err)
	}
	if empathyConf <= 0 {
		t.Fatalf("sales-rep empathy confidence=%v want > 0", empathyConf)
	}
	if reviewStatus != reviewStatusPending {
		t.Fatalf("review status=%q want=%q", reviewStatus, reviewStatusPending)
	}
	if reviewNote != "" {
		t.Fatalf("review note=%q want empty", reviewNote)
	}

	var customerEmpathy float64
	if err := db.QueryRow(`
		SELECT empathy_confidence
		FROM annotations
		WHERE conversation_id = ? AND replica_id = 2
	`, "conv_a").Scan(&customerEmpathy); err != nil {
		t.Fatalf("query customer row: %v", err)
	}
	if customerEmpathy != 0 {
		t.Fatalf("customer empathy confidence=%v want 0", customerEmpathy)
	}
}

func TestAnnotate_WritesLLMResponseAndErrorLogs(t *testing.T) {
	tmp := t.TempDir()
	csvDir := filepath.Join(tmp, "csv")
	dbPath := filepath.Join(tmp, "annotations.db")
	mock := newInvalidQuoteMockServer(t)
	defer mock.Close()

	mustWriteDialogCSV(t, csvDir, "a.csv", "conv_fail", speakerSalesRep, speakerCustomer)

	err := AnnotateToSQLite(context.Background(), AnnotateConfig{
		DBPath:   dbPath,
		InputDir: csvDir,
		FromIdx:  1,
		ToIdx:    1,
		Model:    defaultAnnotateModel,
		APIKey:   "test_key",
		BaseURL:  mock.URL,
	})
	if err != nil {
		t.Fatalf("annotate should continue on llm validation errors: %v", err)
	}

	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	var speakerInfo int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM annotate_logs
		WHERE conversation_id = 'conv_fail' AND replica_id = 1 AND stage = 'speaker' AND status = 'info'
	`).Scan(&speakerInfo); err != nil {
		t.Fatalf("query speaker info logs: %v", err)
	}
	if speakerInfo == 0 {
		t.Fatalf("expected speaker info log row")
	}

	var speakerErr int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM annotate_logs
		WHERE conversation_id = 'conv_fail' AND replica_id = 1 AND stage = 'speaker' AND status = 'error' AND message LIKE '%quote_not_substring%'
	`).Scan(&speakerErr); err != nil {
		t.Fatalf("query speaker error logs: %v", err)
	}
	if speakerErr == 0 {
		t.Fatalf("expected speaker quote_not_substring error log row")
	}

	var pipelineErr int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM annotate_logs
		WHERE conversation_id = 'conv_fail' AND replica_id = 1 AND stage = 'pipeline' AND status = 'error' AND message LIKE '%replica_failed%'
	`).Scan(&pipelineErr); err != nil {
		t.Fatalf("query pipeline error logs: %v", err)
	}
	if pipelineErr == 0 {
		t.Fatalf("expected pipeline replica_failed error log row")
	}

	var annotationRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM annotations WHERE conversation_id = 'conv_fail'`).Scan(&annotationRows); err != nil {
		t.Fatalf("count annotations: %v", err)
	}
	if annotationRows == 0 {
		t.Fatalf("expected annotations to be inserted even on llm validation errors")
	}

	var rawContent string
	if err := db.QueryRow(`
		SELECT raw_json
		FROM annotate_logs
		WHERE conversation_id = 'conv_fail' AND replica_id = 1 AND stage = 'speaker' AND status = 'info'
		ORDER BY id DESC LIMIT 1
	`).Scan(&rawContent); err != nil {
		t.Fatalf("query speaker info raw_json: %v", err)
	}
	if !strings.Contains(rawContent, `"predicted_speaker"`) {
		t.Fatalf("speaker raw_json does not contain model response: %s", rawContent)
	}
}

func TestAnnotate_ClearsOldLogsOnStart(t *testing.T) {
	tmp := t.TempDir()
	csvDir := filepath.Join(tmp, "csv")
	dbPath := filepath.Join(tmp, "annotations.db")
	mock := newStructuredMockServer(t)
	defer mock.Close()

	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}
	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	if _, err := db.Exec(
		insertAnnotateLogSQL,
		time.Now().UTC().Format(time.RFC3339),
		"old_conv",
		1,
		"pipeline",
		"error",
		"old_log_marker",
		"{}",
		"old_model",
	); err != nil {
		t.Fatalf("seed old log: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close sqlite: %v", err)
	}

	mustWriteDialogCSV(t, csvDir, "a.csv", "conv_new", speakerSalesRep, speakerCustomer)
	if err := AnnotateToSQLite(context.Background(), AnnotateConfig{
		DBPath:   dbPath,
		InputDir: csvDir,
		FromIdx:  1,
		ToIdx:    1,
		Model:    defaultAnnotateModel,
		APIKey:   "test_key",
		BaseURL:  mock.URL,
	}); err != nil {
		t.Fatalf("annotate: %v", err)
	}
	db, err = openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite after annotate: %v", err)
	}
	defer db.Close()

	var oldCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM annotate_logs WHERE message = 'old_log_marker'`).Scan(&oldCount); err != nil {
		t.Fatalf("query old log marker: %v", err)
	}
	if oldCount != 0 {
		t.Fatalf("old logs were not cleared, old_count=%d", oldCount)
	}

	var newCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM annotate_logs`).Scan(&newCount); err != nil {
		t.Fatalf("count new logs: %v", err)
	}
	if newCount == 0 {
		t.Fatalf("expected new log rows after annotate")
	}
}

type seededAnnotation struct {
	ConversationID    string
	ReplicaID         int
	SpeakerTrue       string
	SpeakerPredicted  string
	SpeakerConfidence float64
	SpeakerMatch      int
	EmpathyConfidence float64
	ReviewStatus      string
	ReviewerNote      string
	ReplicaText       string
	Model             string
	AnnotatedAtUTC    string
}

func insertSeededAnnotation(t *testing.T, dbPath string, row seededAnnotation) {
	t.Helper()
	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	if strings.TrimSpace(row.Model) == "" {
		row.Model = "seed_model"
	}
	if strings.TrimSpace(row.ReviewStatus) == "" {
		row.ReviewStatus = reviewStatusPending
	}
	if row.AnnotatedAtUTC == "" {
		row.AnnotatedAtUTC = time.Now().UTC().Format(time.RFC3339)
	}

	if _, err := db.Exec(
		insertAnnotationSQL,
		row.ConversationID,
		row.ReplicaID,
		row.SpeakerTrue,
		row.SpeakerPredicted,
		row.SpeakerConfidence,
		row.SpeakerMatch,
		row.EmpathyConfidence,
		row.ReviewStatus,
		row.ReviewerNote,
		row.ReplicaText,
		row.Model,
		row.AnnotatedAtUTC,
	); err != nil {
		t.Fatalf("insert seeded annotation: %v", err)
	}
}

func newStructuredMockServer(t *testing.T) *httptest.Server {
	t.Helper()
	currentTextExpr := regexp.MustCompile(`current: "(.*?)"`)

	type requestMessage struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	type requestBody struct {
		Messages       []requestMessage `json:"messages"`
		ResponseFormat struct {
			JSONSchema struct {
				Name string `json:"name"`
			} `json:"json_schema"`
		} `json:"response_format"`
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req requestBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		currentText := ""
		for _, m := range req.Messages {
			if m.Role != "user" {
				continue
			}
			match := currentTextExpr.FindStringSubmatch(m.Content)
			if len(match) > 1 {
				currentText = match[1]
				break
			}
		}

		content := "{}"
		switch req.ResponseFormat.JSONSchema.Name {
		case "speaker_case_v1":
			predicted := speakerSalesRep
			if strings.Contains(strings.ToLower(currentText), "customer") {
				predicted = speakerCustomer
			}
			quote := firstQuoteToken(currentText)
			content = mustJSONString(t, map[string]any{
				"predicted_speaker": predicted,
				"confidence":        0.91,
				"evidence": map[string]any{
					"quote": quote,
				},
			})
		case "empathy_confidence_v1":
			content = mustJSONString(t, map[string]any{
				"confidence": 0.83,
			})
		default:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"unexpected schema"}`))
			return
		}

		resp := map[string]any{
			"choices": []map[string]any{
				{
					"message": map[string]any{
						"content": content,
						"refusal": "",
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	})

	return httptest.NewServer(handler)
}

func newInvalidQuoteMockServer(t *testing.T) *httptest.Server {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		content := mustJSONString(t, map[string]any{
			"predicted_speaker": speakerSalesRep,
			"confidence":        0.9,
			"evidence": map[string]any{
				"quote": "this_quote_is_missing",
			},
		})
		resp := map[string]any{
			"choices": []map[string]any{
				{
					"message": map[string]any{
						"content": content,
						"refusal": "",
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	})
	return httptest.NewServer(handler)
}

func firstQuoteToken(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return "."
	}
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return text
	}
	return fields[0]
}

func mustWriteDialogCSV(t *testing.T, dir, fileName, conversationID, salesRepLabel, customerLabel string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir csv dir: %v", err)
	}
	content := strings.Join([]string{
		"Conversation,Chunk_id,Speaker,Text,Embedding",
		fmt.Sprintf("%s,1,%s,\"Hello from sales\",[]", conversationID, salesRepLabel),
		fmt.Sprintf("%s,2,%s,\"Hello from customer\",[]", conversationID, customerLabel),
	}, "\n")
	path := filepath.Join(dir, fileName)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
}

func mustJSONString(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return string(data)
}

func tableExists(t *testing.T, db *sql.DB, tableName string) bool {
	t.Helper()
	var count int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?`,
		tableName,
	).Scan(&count); err != nil {
		t.Fatalf("check table exists: %v", err)
	}
	return count > 0
}

func indexExists(t *testing.T, db *sql.DB, indexName string) bool {
	t.Helper()
	var count int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name = ?`,
		indexName,
	).Scan(&count); err != nil {
		t.Fatalf("check index exists: %v", err)
	}
	return count > 0
}

func annotationColumns(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`PRAGMA table_info(annotations)`)
	if err != nil {
		t.Fatalf("query table info: %v", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var cid int
		var name string
		var colType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultValue, &pk); err != nil {
			t.Fatalf("scan table info: %v", err)
		}
		cols = append(cols, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table info: %v", err)
	}
	return cols
}

func distinctConversationIDs(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT DISTINCT conversation_id FROM annotations ORDER BY conversation_id`)
	if err != nil {
		t.Fatalf("query conversations: %v", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan conversation id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate conversations: %v", err)
	}
	sort.Strings(ids)
	return ids
}
