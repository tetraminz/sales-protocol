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
	"sync/atomic"
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

func TestNormalizeSpeakerLabel_StripsMarkdownAsterisks(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{in: "**Sales Rep", want: speakerSalesRep},
		{in: "**Customer", want: speakerCustomer},
		{in: "  *Sales Rep*  ", want: speakerSalesRep},
		{in: "**Prospect**", want: "Prospect"},
	}

	for _, tc := range cases {
		got := normalizeSpeaker(tc.in)
		if got != tc.want {
			t.Fatalf("normalizeSpeaker(%q)=%q want %q", tc.in, got, tc.want)
		}
	}
}

func TestBuildAnnotationInsert_UsesCanonicalSpeakerMatch(t *testing.T) {
	row := buildAnnotationInsert(
		"run_test",
		"gpt-4.1-mini",
		annotateReplica{
			ConversationID: "conv_a",
			ReplicaID:      1,
			SpeakerTrue:    "**Sales Rep",
			Text:           "Hello there",
			TurnIDs:        []int{1},
		},
		ProcessOutput{
			Speaker: ReplicaCaseResult{
				PredictedSpeaker: speakerSalesRep,
				Confidence:       0.9,
				EvidenceQuote:    "Hello",
			},
			Empathy: EmpathyCaseResult{
				Ran:            false,
				EmpathyPresent: false,
				EmpathyType:    "none",
			},
		},
		unitTrace{Ran: true, OK: true, Attempts: 1, ValidationErrors: []string{}, RequestIDs: []string{"req_1"}},
		unitTrace{Ran: false, OK: true, Attempts: 0, ValidationErrors: []string{}, RequestIDs: []string{}},
	)

	if row.SpeakerTrue != speakerSalesRep {
		t.Fatalf("speaker_true=%q want %q", row.SpeakerTrue, speakerSalesRep)
	}
	if row.SpeakerPredicted != speakerSalesRep {
		t.Fatalf("speaker_predicted=%q want %q", row.SpeakerPredicted, speakerSalesRep)
	}
	if row.SpeakerMatch != 1 {
		t.Fatalf("speaker_match=%d want 1", row.SpeakerMatch)
	}
	errors := parseStringArray(row.SpeakerValidationErrorsJSON)
	if len(errors) != 0 {
		t.Fatalf("speaker_validation_errors=%v want []", errors)
	}
}

func TestOpenAISpeakerCase_FinalSuccessHasNoTransientErrors(t *testing.T) {
	var calls int64
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := atomic.AddInt64(&calls, 1)
		quote := "missing"
		if call >= 3 {
			quote = "Hello"
		}

		content := mustJSON(map[string]any{
			"predicted_speaker": speakerSalesRep,
			"confidence":        0.9,
			"evidence": map[string]any{
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
		w.Header().Set("x-request-id", fmt.Sprintf("req_%d", call))
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode mock response: %v", err)
		}
	}))
	defer mock.Close()

	c := &openAISpeakerCase{
		client: &openAIClient{
			apiKey:  "test_key",
			baseURL: mock.URL,
			httpClient: &http.Client{
				Timeout: 5 * time.Second,
			},
		},
		model:      "gpt-4.1-mini",
		maxRetries: 2,
	}

	_, err := c.Evaluate(context.Background(), ReplicaCaseInput{
		PrevText:    "Previous",
		ReplicaText: "Hello there",
		NextText:    "Next",
	})
	if err != nil {
		t.Fatalf("evaluate speaker case: %v", err)
	}

	trace := c.LastTrace()
	if !trace.OK {
		t.Fatalf("trace.OK=false want true")
	}
	if trace.Attempts != 3 {
		t.Fatalf("trace.Attempts=%d want 3", trace.Attempts)
	}
	if len(trace.ValidationErrors) != 0 {
		t.Fatalf("trace.ValidationErrors=%v want []", trace.ValidationErrors)
	}
}

func TestReasonsForRedReplica_NoDuplicateMismatchReason(t *testing.T) {
	reasons := reasonsForRedReplica(analyzedReplicaRow{
		SpeakerMatch:  false,
		SpeakerOK:     true,
		SpeakerErrors: []string{"quality:speaker_mismatch"},
	})

	countMismatch := 0
	for _, reason := range reasons {
		if reason == "speaker_mismatch" {
			countMismatch++
		}
		if reason == "speaker:quality:speaker_mismatch" {
			t.Fatalf("unexpected duplicate mismatch reason: %v", reasons)
		}
	}
	if countMismatch != 1 {
		t.Fatalf("speaker_mismatch count=%d want 1, reasons=%v", countMismatch, reasons)
	}
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
		"## Root Cause Breakdown",
	} {
		if !strings.Contains(md, section) {
			t.Fatalf("analytics markdown missing section %q", section)
		}
	}
}

func TestAnalytics_RootCauseBreakdownSectionsPresent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}
	runID := "run_root_cause_analytics"
	seedRun(t, dbPath, runID, time.Now().UTC().Add(-2*time.Minute).Format(time.RFC3339))

	seedAnnotationRaw(t, dbPath, annotationInsert{
		RunID:                       runID,
		ConversationID:              "conv_fmt",
		ReplicaID:                   1,
		SpeakerTrue:                 "**Sales Rep",
		SpeakerPredicted:            speakerSalesRep,
		SpeakerMatch:                0,
		SpeakerOK:                   1,
		SpeakerAttempts:             1,
		SpeakerValidationErrorsJSON: mustJSON([]string{"quality:speaker_mismatch"}),
		SpeakerOutputJSON: mustJSON(map[string]any{
			"predicted_speaker": speakerSalesRep,
			"confidence":        0.95,
			"evidence":          map[string]string{"quote": "Hello"},
		}),
		EmpathyRan:                  0,
		EmpathyOK:                   1,
		EmpathyPresent:              0,
		EmpathyType:                 "none",
		EmpathyConfidence:           0,
		EmpathyAttempts:             0,
		EmpathyValidationErrorsJSON: "[]",
		EmpathyOutputJSON:           mustJSON(map[string]any{"empathy_present": false, "empathy_type": "none", "confidence": 0, "evidence": []any{}}),
		ReplicaText:                 "Hello there",
		TurnIDsJSON:                 mustJSON([]int{1}),
		Model:                       "seed_model",
		TimestampUTC:                time.Now().UTC().Format(time.RFC3339),
		RequestIDsJSON:              "[]",
	})
	seedAnnotationRaw(t, dbPath, annotationInsert{
		RunID:                       runID,
		ConversationID:              "conv_short",
		ReplicaID:                   2,
		SpeakerTrue:                 speakerSalesRep,
		SpeakerPredicted:            speakerCustomer,
		SpeakerMatch:                0,
		SpeakerOK:                   1,
		SpeakerAttempts:             1,
		SpeakerValidationErrorsJSON: mustJSON([]string{"quality:speaker_mismatch"}),
		SpeakerOutputJSON: mustJSON(map[string]any{
			"predicted_speaker": speakerCustomer,
			"confidence":        0.95,
			"evidence":          map[string]string{"quote": "Bye."},
		}),
		EmpathyRan:                  0,
		EmpathyOK:                   1,
		EmpathyPresent:              0,
		EmpathyType:                 "none",
		EmpathyConfidence:           0,
		EmpathyAttempts:             0,
		EmpathyValidationErrorsJSON: "[]",
		EmpathyOutputJSON:           mustJSON(map[string]any{"empathy_present": false, "empathy_type": "none", "confidence": 0, "evidence": []any{}}),
		ReplicaText:                 "Bye.",
		TurnIDsJSON:                 mustJSON([]int{2}),
		Model:                       "seed_model",
		TimestampUTC:                time.Now().UTC().Format(time.RFC3339),
		RequestIDsJSON:              "[]",
	})
	seedAnnotationRaw(t, dbPath, annotationInsert{
		RunID:                       runID,
		ConversationID:              "conv_retry",
		ReplicaID:                   3,
		SpeakerTrue:                 speakerCustomer,
		SpeakerPredicted:            speakerCustomer,
		SpeakerMatch:                1,
		SpeakerOK:                   1,
		SpeakerAttempts:             3,
		SpeakerValidationErrorsJSON: mustJSON([]string{"attempt 1: format:evidence_quote_not_substring"}),
		SpeakerOutputJSON: mustJSON(map[string]any{
			"predicted_speaker": speakerCustomer,
			"confidence":        0.95,
			"evidence":          map[string]string{"quote": "Sure"},
		}),
		EmpathyRan:                  0,
		EmpathyOK:                   1,
		EmpathyPresent:              0,
		EmpathyType:                 "none",
		EmpathyConfidence:           0,
		EmpathyAttempts:             0,
		EmpathyValidationErrorsJSON: "[]",
		EmpathyOutputJSON:           mustJSON(map[string]any{"empathy_present": false, "empathy_type": "none", "confidence": 0, "evidence": []any{}}),
		ReplicaText:                 "Sure, let's talk.",
		TurnIDsJSON:                 mustJSON([]int{3}),
		Model:                       "seed_model",
		TimestampUTC:                time.Now().UTC().Format(time.RFC3339),
		RequestIDsJSON:              "[]",
	})

	md, err := BuildAnalyticsMarkdown(dbPath, runID)
	if err != nil {
		t.Fatalf("build analytics: %v", err)
	}
	for _, item := range []string{
		"label_format_mismatch_false_red_count: `1`",
		"real_model_mismatch_count: `1`",
		"transient_retry_error_count: `1`",
		"`conv_short` / replica `2`",
	} {
		if !strings.Contains(md, item) {
			t.Fatalf("analytics markdown missing %q\n%s", item, md)
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
	if !strings.Contains(md, "## Root Cause Breakdown") {
		t.Fatalf("missing root cause section")
	}
}

func TestDebugRelease_RootCauseAndShortUtteranceSectionPresent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}
	runID := "run_root_cause_debug"
	seedRun(t, dbPath, runID, time.Now().UTC().Add(-2*time.Minute).Format(time.RFC3339))

	seedAnnotationRaw(t, dbPath, annotationInsert{
		RunID:                       runID,
		ConversationID:              "conv_short",
		ReplicaID:                   5,
		SpeakerTrue:                 speakerSalesRep,
		SpeakerPredicted:            speakerCustomer,
		SpeakerMatch:                0,
		SpeakerOK:                   1,
		SpeakerAttempts:             1,
		SpeakerValidationErrorsJSON: mustJSON([]string{"quality:speaker_mismatch"}),
		SpeakerOutputJSON: mustJSON(map[string]any{
			"predicted_speaker": speakerCustomer,
			"confidence":        0.95,
			"evidence":          map[string]string{"quote": "Bye."},
		}),
		EmpathyRan:                  0,
		EmpathyOK:                   1,
		EmpathyPresent:              0,
		EmpathyType:                 "none",
		EmpathyConfidence:           0,
		EmpathyAttempts:             0,
		EmpathyValidationErrorsJSON: "[]",
		EmpathyOutputJSON:           mustJSON(map[string]any{"empathy_present": false, "empathy_type": "none", "confidence": 0, "evidence": []any{}}),
		ReplicaText:                 "Bye.",
		TurnIDsJSON:                 mustJSON([]int{5}),
		Model:                       "seed_model",
		TimestampUTC:                time.Now().UTC().Format(time.RFC3339),
		RequestIDsJSON:              "[]",
	})

	md, err := BuildReleaseDebugMarkdown(dbPath, runID)
	if err != nil {
		t.Fatalf("build debug markdown: %v", err)
	}
	for _, item := range []string{
		"## Root Cause Breakdown",
		"## Top Short-Utterance Mismatches",
		"`conv_short`",
		"`5`",
		"`Bye.`",
	} {
		if !strings.Contains(md, item) {
			t.Fatalf("debug markdown missing %q\n%s", item, md)
		}
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

func seedAnnotationRaw(t *testing.T, dbPath string, row annotationInsert) {
	t.Helper()
	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	if row.TimestampUTC == "" {
		row.TimestampUTC = time.Now().UTC().Format(time.RFC3339)
	}
	if row.RequestIDsJSON == "" {
		row.RequestIDsJSON = "[]"
	}

	if _, err := db.Exec(insertAnnotationSQL,
		row.RunID,
		row.ConversationID,
		row.ReplicaID,
		row.SpeakerTrue,
		row.SpeakerPredicted,
		row.SpeakerMatch,
		row.SpeakerOK,
		row.SpeakerAttempts,
		row.SpeakerValidationErrorsJSON,
		row.SpeakerOutputJSON,
		row.EmpathyRan,
		row.EmpathyOK,
		row.EmpathyPresent,
		row.EmpathyType,
		row.EmpathyConfidence,
		row.EmpathyAttempts,
		row.EmpathyValidationErrorsJSON,
		row.EmpathyOutputJSON,
		row.ReplicaText,
		row.TurnIDsJSON,
		row.Model,
		row.TimestampUTC,
		row.RequestIDsJSON,
	); err != nil {
		t.Fatalf("seed raw annotation: %v", err)
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
