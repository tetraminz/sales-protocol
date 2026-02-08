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
	"sync"
	"testing"
)

type mockRequestMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func TestSetupSQLite_CreatesNewSchema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}

	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	if !tableExists(t, db, "annotations") {
		t.Fatalf("annotations table should exist")
	}
	if !tableExists(t, db, "llm_events") {
		t.Fatalf("llm_events table should exist")
	}

	annotationCols := tableColumns(t, db, "annotations")
	wantAnnotationCols := requiredAnnotationColumns()
	sort.Strings(wantAnnotationCols)
	if strings.Join(annotationCols, ",") != strings.Join(wantAnnotationCols, ",") {
		t.Fatalf("annotations columns=%v want=%v", annotationCols, wantAnnotationCols)
	}

	eventCols := tableColumns(t, db, "llm_events")
	wantEventCols := requiredLLMEventColumns()
	sort.Strings(wantEventCols)
	if strings.Join(eventCols, ",") != strings.Join(wantEventCols, ",") {
		t.Fatalf("llm_events columns=%v want=%v", eventCols, wantEventCols)
	}
}

func TestAnnotate_WritesAnnotationsAndLLMEvents(t *testing.T) {
	tmp := t.TempDir()
	csvDir := filepath.Join(tmp, "csv")
	dbPath := filepath.Join(tmp, "annotations.db")
	mock := newStructuredMockServer(t)
	defer mock.Close()

	mustWriteDialogCSV(t, csvDir, "a.csv", "conv_a", speakerSalesRep, speakerCustomer)

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

	var annotationRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM annotations`).Scan(&annotationRows); err != nil {
		t.Fatalf("count annotations: %v", err)
	}
	if annotationRows == 0 {
		t.Fatalf("annotations should not be empty")
	}

	var llmEventRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM llm_events`).Scan(&llmEventRows); err != nil {
		t.Fatalf("count llm_events: %v", err)
	}
	if llmEventRows == 0 {
		t.Fatalf("llm_events should not be empty")
	}
}

func TestAnnotate_LLMRequestContainsOnlyTextContext(t *testing.T) {
	tmp := t.TempDir()
	csvDir := filepath.Join(tmp, "csv")
	dbPath := filepath.Join(tmp, "annotations.db")
	mock := newStructuredMockServer(t)
	defer mock.Close()

	mustWriteDialogCSV(t, csvDir, "a.csv", "conv_text_only", speakerSalesRep, speakerCustomer)

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

	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	var requestJSON string
	if err := db.QueryRow(`
		SELECT request_json
		FROM llm_events
		WHERE conversation_id='conv_text_only' AND unit_name='speaker'
		ORDER BY id ASC LIMIT 1
	`).Scan(&requestJSON); err != nil {
		t.Fatalf("query speaker request_json: %v", err)
	}

	for _, forbidden := range []string{
		"conversation_id",
		"utterance_index",
		"ground_truth_speaker",
		"empathy_review_status",
	} {
		if strings.Contains(requestJSON, forbidden) {
			t.Fatalf("request_json unexpectedly contains %q: %s", forbidden, requestJSON)
		}
	}
}

func TestAnnotate_SpeakerRetryCycleOnInvalidEvidence(t *testing.T) {
	tmp := t.TempDir()
	csvDir := filepath.Join(tmp, "csv")
	dbPath := filepath.Join(tmp, "annotations.db")
	mock := newRetrySpeakerMockServer(t)
	defer mock.Close()

	if err := os.MkdirAll(csvDir, 0o755); err != nil {
		t.Fatalf("mkdir csv dir: %v", err)
	}
	csvBody := strings.Join([]string{
		"Conversation,Chunk_id,Speaker,Text,Embedding",
		fmt.Sprintf("conv_retry,1,%s,\"Hello from sales\",[]", speakerSalesRep),
	}, "\n")
	if err := os.WriteFile(filepath.Join(csvDir, "a.csv"), []byte(csvBody), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

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

	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	var speakerAttempts int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM llm_events
		WHERE conversation_id='conv_retry' AND utterance_index=1 AND unit_name='speaker'
	`).Scan(&speakerAttempts); err != nil {
		t.Fatalf("count speaker attempts: %v", err)
	}
	if speakerAttempts != 2 {
		t.Fatalf("speaker attempts=%d want=2", speakerAttempts)
	}

	var evidenceIsValid int
	if err := db.QueryRow(`
		SELECT speaker_evidence_is_valid
		FROM annotations
		WHERE conversation_id='conv_retry' AND utterance_index=1
	`).Scan(&evidenceIsValid); err != nil {
		t.Fatalf("query speaker_evidence_is_valid: %v", err)
	}
	if evidenceIsValid != 1 {
		t.Fatalf("speaker_evidence_is_valid=%d want=1 after successful retry", evidenceIsValid)
	}
}

func TestAnnotate_SGRFarewellOverrideWritesRawAndFinal(t *testing.T) {
	tmp := t.TempDir()
	csvDir := filepath.Join(tmp, "csv")
	dbPath := filepath.Join(tmp, "annotations.db")
	mock := newFarewellOverrideMockServer(t)
	defer mock.Close()

	if err := os.MkdirAll(csvDir, 0o755); err != nil {
		t.Fatalf("mkdir csv dir: %v", err)
	}
	csvBody := strings.Join([]string{
		"Conversation,Chunk_id,Speaker,Text,Embedding",
		fmt.Sprintf("conv_farewell,1,%s,\"Thanks, bye.\",[]", speakerCustomer),
		fmt.Sprintf("conv_farewell,2,%s,\"Goodbye!\",[]", speakerSalesRep),
	}, "\n")
	if err := os.WriteFile(filepath.Join(csvDir, "a.csv"), []byte(csvBody), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

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

	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	var rawCorrect int
	var finalCorrect int
	var qualityDecision string
	if err := db.QueryRow(`
		SELECT speaker_is_correct_raw, speaker_is_correct_final, speaker_quality_decision
		FROM annotations
		WHERE conversation_id='conv_farewell' AND utterance_index=2
	`).Scan(&rawCorrect, &finalCorrect, &qualityDecision); err != nil {
		t.Fatalf("query farewell annotation: %v", err)
	}
	if rawCorrect != 0 {
		t.Fatalf("speaker_is_correct_raw=%d want=0", rawCorrect)
	}
	if finalCorrect != 1 {
		t.Fatalf("speaker_is_correct_final=%d want=1", finalCorrect)
	}
	if qualityDecision != qualityDecisionFarewellOverride {
		t.Fatalf("speaker_quality_decision=%q want=%q", qualityDecision, qualityDecisionFarewellOverride)
	}
}

func newStructuredMockServer(t *testing.T) *httptest.Server {
	t.Helper()
	currentTextExpr := regexp.MustCompile(`current_text: "(.*?)"`)

	type requestBody struct {
		Messages       []mockRequestMessage `json:"messages"`
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
		currentText := extractCurrentText(req.Messages, currentTextExpr)

		content := "{}"
		switch req.ResponseFormat.JSONSchema.Name {
		case "speaker_case_v2":
			predicted := speakerSalesRep
			if strings.Contains(strings.ToLower(currentText), "customer") || strings.Contains(strings.ToLower(currentText), "thanks, bye") {
				predicted = speakerCustomer
			}
			content = mustJSONString(t, map[string]any{
				"farewell": map[string]any{
					"is_current_farewell": false,
					"is_closing_context":  false,
					"context_source":      farewellContextSourceNone,
				},
				"speaker": map[string]any{
					"predicted_speaker": predicted,
					"confidence":        0.91,
					"evidence_quote":    firstQuoteToken(currentText),
				},
			})
		case "empathy_case_v2":
			content = mustJSONString(t, map[string]any{
				"empathy_present": true,
				"confidence":      0.83,
				"evidence_quote":  firstQuoteToken(currentText),
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

func newRetrySpeakerMockServer(t *testing.T) *httptest.Server {
	t.Helper()
	currentTextExpr := regexp.MustCompile(`current_text: "(.*?)"`)
	attemptByText := map[string]int{}
	var mu sync.Mutex

	type requestBody struct {
		Messages       []mockRequestMessage `json:"messages"`
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
		currentText := extractCurrentText(req.Messages, currentTextExpr)

		content := "{}"
		switch req.ResponseFormat.JSONSchema.Name {
		case "speaker_case_v2":
			mu.Lock()
			attemptByText[currentText]++
			attempt := attemptByText[currentText]
			mu.Unlock()

			evidence := firstQuoteToken(currentText)
			if attempt == 1 {
				evidence = "this_quote_is_missing"
			}
			content = mustJSONString(t, map[string]any{
				"farewell": map[string]any{
					"is_current_farewell": false,
					"is_closing_context":  false,
					"context_source":      farewellContextSourceNone,
				},
				"speaker": map[string]any{
					"predicted_speaker": speakerSalesRep,
					"confidence":        0.9,
					"evidence_quote":    evidence,
				},
			})
		case "empathy_case_v2":
			content = mustJSONString(t, map[string]any{
				"empathy_present": true,
				"confidence":      0.8,
				"evidence_quote":  firstQuoteToken(currentText),
			})
		default:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"unexpected schema"}`))
			return
		}

		resp := map[string]any{
			"choices": []map[string]any{{"message": map[string]any{"content": content, "refusal": ""}}},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	})
	return httptest.NewServer(handler)
}

func newFarewellOverrideMockServer(t *testing.T) *httptest.Server {
	t.Helper()
	currentTextExpr := regexp.MustCompile(`current_text: "(.*?)"`)

	type requestBody struct {
		Messages       []mockRequestMessage `json:"messages"`
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
		currentText := extractCurrentText(req.Messages, currentTextExpr)

		content := "{}"
		switch req.ResponseFormat.JSONSchema.Name {
		case "speaker_case_v2":
			isFarewell := strings.Contains(strings.ToLower(currentText), "bye") || strings.Contains(strings.ToLower(currentText), "goodbye")
			predicted := speakerSalesRep
			if isFarewell {
				predicted = speakerCustomer // намеренный raw mismatch для проверки override
			}
			contextSource := farewellContextSourceNone
			if isFarewell {
				contextSource = farewellContextSourceCurrent
			}
			content = mustJSONString(t, map[string]any{
				"farewell": map[string]any{
					"is_current_farewell": isFarewell,
					"is_closing_context":  isFarewell,
					"context_source":      contextSource,
				},
				"speaker": map[string]any{
					"predicted_speaker": predicted,
					"confidence":        0.89,
					"evidence_quote":    firstQuoteToken(currentText),
				},
			})
		case "empathy_case_v2":
			content = mustJSONString(t, map[string]any{
				"empathy_present": true,
				"confidence":      0.77,
				"evidence_quote":  firstQuoteToken(currentText),
			})
		default:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"unexpected schema"}`))
			return
		}

		resp := map[string]any{
			"choices": []map[string]any{{"message": map[string]any{"content": content, "refusal": ""}}},
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	})

	return httptest.NewServer(handler)
}

func extractCurrentText(messages []mockRequestMessage, expr *regexp.Regexp) string {
	for _, msg := range messages {
		if msg.Role != "user" {
			continue
		}
		match := expr.FindStringSubmatch(msg.Content)
		if len(match) > 1 {
			return match[1]
		}
	}
	return ""
}

func firstQuoteToken(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return "."
	}
	parts := strings.Fields(text)
	if len(parts) == 0 {
		return text
	}
	return parts[0]
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
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return string(b)
}

func tableExists(t *testing.T, db *sql.DB, tableName string) bool {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, tableName).Scan(&count); err != nil {
		t.Fatalf("query table exists: %v", err)
	}
	return count > 0
}

func tableColumns(t *testing.T, db *sql.DB, tableName string) []string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info(%s)`, tableName))
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
	sort.Strings(cols)
	return cols
}
