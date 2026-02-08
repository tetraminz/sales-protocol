package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestSetupSQLite_CreatesRunAndAnnotationsTables(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}

	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	assertTableExists(t, db, "annotation_runs")
	assertTableExists(t, db, "annotations")
	assertIndexExists(t, db, "idx_annotations_run_id")
}

func TestAnnotateRange_RespectsFromToInclusive(t *testing.T) {
	tmp := t.TempDir()
	csvDir := filepath.Join(tmp, "csv")
	dbPath := filepath.Join(tmp, "annotations.db")
	mock := newMockOpenAIServer(t)
	defer mock.Close()
	mustWriteCSV(t, csvDir, "a.csv", "conv_a")
	mustWriteCSV(t, csvDir, "b.csv", "conv_b")
	mustWriteCSV(t, csvDir, "c.csv", "conv_c")

	runID, err := AnnotateRangeToSQLite(context.Background(), AnnotateConfig{
		DBPath:     dbPath,
		InputDir:   csvDir,
		FromIdx:    2,
		ToIdx:      3,
		ReleaseTag: "range_test",
		Model:      "gpt-4.1-mini",
		APIKey:     "test_key",
		BaseURL:    mock.URL,
		MaxRetries: 0,
	})
	if err != nil {
		t.Fatalf("annotate range: %v", err)
	}

	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	var conversations int
	if err := db.QueryRow(`SELECT total_conversations FROM annotation_runs WHERE run_id = ?`, runID).Scan(&conversations); err != nil {
		t.Fatalf("query run record: %v", err)
	}
	if conversations != 2 {
		t.Fatalf("total_conversations=%d want 2", conversations)
	}

	rows, err := db.Query(`SELECT DISTINCT conversation_id FROM annotations WHERE run_id = ? ORDER BY conversation_id`, runID)
	if err != nil {
		t.Fatalf("query conversations: %v", err)
	}
	defer rows.Close()

	got := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan conversation id: %v", err)
		}
		got = append(got, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate conversation ids: %v", err)
	}

	want := []string{"conv_b", "conv_c"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("conversation ids=%v want %v", got, want)
	}
}

func TestAnnotateRange_AppendsNewRunDoesNotOverwrite(t *testing.T) {
	tmp := t.TempDir()
	csvDir := filepath.Join(tmp, "csv")
	dbPath := filepath.Join(tmp, "annotations.db")
	mock := newMockOpenAIServer(t)
	defer mock.Close()
	mustWriteCSV(t, csvDir, "a.csv", "conv_a")
	mustWriteCSV(t, csvDir, "b.csv", "conv_b")

	run1, err := AnnotateRangeToSQLite(context.Background(), AnnotateConfig{
		DBPath:     dbPath,
		InputDir:   csvDir,
		FromIdx:    1,
		ToIdx:      1,
		ReleaseTag: "r1",
		Model:      "gpt-4.1-mini",
		APIKey:     "test_key",
		BaseURL:    mock.URL,
		MaxRetries: 0,
	})
	if err != nil {
		t.Fatalf("annotate run1: %v", err)
	}
	run2, err := AnnotateRangeToSQLite(context.Background(), AnnotateConfig{
		DBPath:     dbPath,
		InputDir:   csvDir,
		FromIdx:    2,
		ToIdx:      2,
		ReleaseTag: "r2",
		Model:      "gpt-4.1-mini",
		APIKey:     "test_key",
		BaseURL:    mock.URL,
		MaxRetries: 0,
	})
	if err != nil {
		t.Fatalf("annotate run2: %v", err)
	}
	if run1 == run2 {
		t.Fatalf("run ids must differ")
	}

	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	var runCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM annotation_runs`).Scan(&runCount); err != nil {
		t.Fatalf("count runs: %v", err)
	}
	if runCount != 2 {
		t.Fatalf("run count=%d want 2", runCount)
	}

	var totalRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM annotations`).Scan(&totalRows); err != nil {
		t.Fatalf("count annotations: %v", err)
	}
	if totalRows == 0 {
		t.Fatalf("expected annotations rows > 0")
	}
}

func TestGreenRedReplicaClassification_StrictGate(t *testing.T) {
	green := analyzedReplicaRow{
		SpeakerMatch:  true,
		SpeakerOK:     true,
		SpeakerErrors: []string{},
		EmpathyRan:    false,
	}
	if !isGreenReplica(green) {
		t.Fatalf("expected green row")
	}

	redMismatch := analyzedReplicaRow{
		SpeakerMatch:  false,
		SpeakerOK:     true,
		SpeakerErrors: []string{},
		EmpathyRan:    false,
	}
	if isGreenReplica(redMismatch) {
		t.Fatalf("expected red row for mismatch")
	}

	redEmpathy := analyzedReplicaRow{
		SpeakerMatch:  true,
		SpeakerOK:     true,
		SpeakerErrors: []string{},
		EmpathyRan:    true,
		EmpathyOK:     false,
		EmpathyErrors: []string{"format:error"},
	}
	if isGreenReplica(redEmpathy) {
		t.Fatalf("expected red row for empathy errors")
	}
}

func TestConversationColor_ComputedFromReplicaColors(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}

	runID := "run_conversation_color"
	createdAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	seedRun(t, dbPath, runID, createdAt)
	seedAnnotation(t, dbPath, runID, "conv_a", 1, true, true, []string{}, false, true, []string{}, "none")
	seedAnnotation(t, dbPath, runID, "conv_a", 2, false, true, []string{"quality:speaker_mismatch"}, false, true, []string{}, "none")
	seedAnnotation(t, dbPath, runID, "conv_b", 1, true, true, []string{}, false, true, []string{}, "none")

	report, err := BuildReport(dbPath, runID)
	if err != nil {
		t.Fatalf("build report: %v", err)
	}
	if report.GreenConversationCount != 1 {
		t.Fatalf("green conversations=%d want 1", report.GreenConversationCount)
	}
	if report.RedConversationCount != 1 {
		t.Fatalf("red conversations=%d want 1", report.RedConversationCount)
	}
}

func TestAnalyticsMarkdown_ContainsCoreSections(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}
	runID := "run_analytics"
	seedRun(t, dbPath, runID, time.Now().UTC().Add(-2*time.Minute).Format(time.RFC3339))
	seedAnnotation(t, dbPath, runID, "conv_a", 1, true, true, []string{}, false, true, []string{}, "none")

	md, err := BuildAnalyticsMarkdown(dbPath, runID)
	if err != nil {
		t.Fatalf("build analytics: %v", err)
	}

	for _, section := range []string{
		"## Run Metadata",
		"## Totals",
		"## Speaker Accuracy",
		"## Empathy",
		"## Top Validation Errors",
	} {
		if !strings.Contains(md, section) {
			t.Fatalf("analytics markdown missing section %q", section)
		}
	}
}

func TestDebugMarkdown_ContainsBrokenDialogsAndReasons(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}

	runID := "run_debug_broken"
	seedRun(t, dbPath, runID, time.Now().UTC().Add(-2*time.Minute).Format(time.RFC3339))
	seedAnnotation(t, dbPath, runID, "conv_bad", 1, false, true, []string{"quality:speaker_mismatch"}, false, true, []string{}, "none")

	md, err := BuildReleaseDebugMarkdown(dbPath, runID)
	if err != nil {
		t.Fatalf("build debug markdown: %v", err)
	}
	if !strings.Contains(md, "## Red Conversations") {
		t.Fatalf("missing red conversations section")
	}
	if !strings.Contains(md, "conv_bad") {
		t.Fatalf("missing broken conversation id")
	}
	if !strings.Contains(md, "speaker_mismatch") {
		t.Fatalf("missing failure reason")
	}
}

func TestDebugMarkdown_ContainsDeltaToPreviousRun(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}

	seedRun(t, dbPath, "run_prev", time.Now().UTC().Add(-2*time.Hour).Format(time.RFC3339))
	seedAnnotation(t, dbPath, "run_prev", "conv_a", 1, true, true, []string{}, false, true, []string{}, "none")

	seedRun(t, dbPath, "run_curr", time.Now().UTC().Add(-time.Hour).Format(time.RFC3339))
	seedAnnotation(t, dbPath, "run_curr", "conv_a", 1, false, true, []string{"quality:speaker_mismatch"}, false, true, []string{}, "none")

	md, err := BuildReleaseDebugMarkdown(dbPath, "run_curr")
	if err != nil {
		t.Fatalf("build debug markdown: %v", err)
	}
	if !strings.Contains(md, "## Delta vs previous run") {
		t.Fatalf("missing delta section")
	}
	if !strings.Contains(md, "previous_run_id: `run_prev`") {
		t.Fatalf("missing previous run id")
	}
	if !strings.Contains(md, "speaker_accuracy_delta_pp") {
		t.Fatalf("missing delta metric")
	}
}

func TestReportLatestRun_ConsoleMetrics(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}

	seedRun(t, dbPath, "run_report", time.Now().UTC().Add(-time.Minute).Format(time.RFC3339))
	seedAnnotation(t, dbPath, "run_report", "conv_a", 1, true, true, []string{}, false, true, []string{}, "none")

	report, err := BuildReport(dbPath, "latest")
	if err != nil {
		t.Fatalf("build report: %v", err)
	}
	text := FormatReport(report)

	for _, item := range []string{
		"run_id=run_report",
		"speaker_accuracy_percent=",
		"green_replicas=",
		"red_replicas=",
	} {
		if !strings.Contains(text, item) {
			t.Fatalf("report output missing %q", item)
		}
	}
}

func mustWriteCSV(t *testing.T, dir, fileName, conversationID string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir csv dir: %v", err)
	}

	path := filepath.Join(dir, fileName)
	content := strings.Join([]string{
		"Conversation,Chunk_id,Speaker,Text,Embedding",
		fmt.Sprintf("%s,1,Sales Rep,\"I understand your concern and I can help.\",[]", conversationID),
		fmt.Sprintf("%s,2,Customer,\"I am not sure this is worth the price.\",[]", conversationID),
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
}

func assertTableExists(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?`, table).Scan(&count); err != nil {
		t.Fatalf("check table %s: %v", table, err)
	}
	if count == 0 {
		t.Fatalf("table not found: %s", table)
	}
}

func assertIndexExists(t *testing.T, db *sql.DB, index string) {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name = ?`, index).Scan(&count); err != nil {
		t.Fatalf("check index %s: %v", index, err)
	}
	if count == 0 {
		t.Fatalf("index not found: %s", index)
	}
}

func seedRun(t *testing.T, dbPath, runID, createdAt string) {
	t.Helper()
	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(insertRunSQL,
		runID,
		"seed",
		createdAt,
		"seed_input",
		1,
		1,
		"seed_model",
		1,
		1,
	); err != nil {
		t.Fatalf("seed run: %v", err)
	}
}

func seedAnnotation(
	t *testing.T,
	dbPath, runID, conversationID string,
	replicaID int,
	speakerMatch bool,
	speakerOK bool,
	speakerErrors []string,
	empathyRan bool,
	empathyOK bool,
	empathyErrors []string,
	empathyType string,
) {
	t.Helper()
	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	speakerPredicted := speakerCustomer
	if speakerMatch {
		speakerPredicted = speakerSalesRep
	}
	if strings.TrimSpace(empathyType) == "" {
		empathyType = "none"
	}

	if _, err := db.Exec(insertAnnotationSQL,
		runID,
		conversationID,
		replicaID,
		speakerSalesRep,
		speakerPredicted,
		boolToInt(speakerMatch),
		boolToInt(speakerOK),
		1,
		mustJSON(speakerErrors),
		mustJSON(map[string]any{
			"predicted_speaker": speakerPredicted,
			"confidence":        0.8,
			"evidence":          map[string]string{"quote": "sample"},
		}),
		boolToInt(empathyRan),
		boolToInt(empathyOK),
		0,
		empathyType,
		0.7,
		boolToInt(empathyRan),
		mustJSON(empathyErrors),
		mustJSON(map[string]any{
			"empathy_present": false,
			"empathy_type":    empathyType,
			"confidence":      0.7,
			"evidence":        []any{},
		}),
		"sample text",
		mustJSON([]int{1}),
		"seed_model",
		time.Now().UTC().Format(time.RFC3339),
		"[]",
	); err != nil {
		t.Fatalf("seed annotation: %v", err)
	}
}

func newMockOpenAIServer(t *testing.T) *httptest.Server {
	t.Helper()
	currentTextRegex := regexp.MustCompile(`current: "(.*?)"`)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}

		var req struct {
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("unmarshal request: %v", err)
		}

		currentText := ""
		for _, m := range req.Messages {
			if m.Role != "user" {
				continue
			}
			matches := currentTextRegex.FindStringSubmatch(m.Content)
			if len(matches) >= 2 {
				currentText = matches[1]
				break
			}
		}

		predicted := speakerSalesRep
		if strings.Contains(strings.ToLower(currentText), "not sure") ||
			strings.Contains(strings.ToLower(currentText), "worth the price") {
			predicted = speakerCustomer
		}

		quote := "I"
		if strings.Contains(currentText, "I understand") {
			quote = "I understand"
		} else if strings.Contains(currentText, "I am") {
			quote = "I am"
		}

		content := mustJSON(map[string]any{
			"predicted_speaker": predicted,
			"confidence":        0.9,
			"evidence": map[string]string{
				"quote": quote,
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
		w.Header().Set("x-request-id", "req_test_1")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	})

	return httptest.NewServer(handler)
}
