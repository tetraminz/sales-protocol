package openai

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/tetraminz/sales_protocol/internal/dataset"
)

func TestAnnotateConversationUsesStrictSchema(t *testing.T) {
	t.Parallel()

	annotationJSON := `{
		"summary": {"product_or_service":"x","one_sentence":"y"},
		"participants": {"company":"ModaMart","sales_rep_name":"Jamie","customer_name":""},
		"scorecard": {"opening":2,"discovery":2,"objection_handling":1,"closing":1,"overall":2},
		"signals": {
			"greeting": {
				"present": true,
				"quality": "good",
				"introduced_self": true,
				"introduced_company": true,
				"asked_how_are_you": true,
				"evidence": []
			},
			"needs": {"stated_need":"warm jacket","need_categories":["quality"],"evidence":[]},
			"objections": [],
			"upsell": {
				"present": false,
				"kind":"none",
				"offer_summary":"",
				"customer_response":"not_applicable",
				"evidence":[]
			},
			"next_step": {"type":"follow_up","summary":"email later","evidence":[]},
			"tone": {"sales_rep_tone":"friendly","customer_tone":"neutral","evidence":[]}
		},
		"quality_checks": {"referenced_turn_ids":[],"ambiguities":[],"notes":""}
	}`

	doer := &fakeHTTPDoer{
		statusCode: http.StatusOK,
		body:       `{"choices":[{"message":{"content":` + strconv.Quote(annotationJSON) + `}}]}`,
	}
	client := NewClient("test-api-key", "gpt-4.1-mini", doer)

	turns := []dataset.Turn{
		{TurnID: 0, Speaker: "Sales Rep", Text: "Hello"},
		{TurnID: 1, Speaker: "Customer", Text: "Need help"},
	}

	result, err := client.AnnotateConversation(context.Background(), "modamart__0_transcript", "modamart", turns)
	if err != nil {
		t.Fatalf("AnnotateConversation error: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(doer.requestBody, &payload); err != nil {
		t.Fatalf("decode request payload: %v", err)
	}

	responseFormat, ok := payload["response_format"].(map[string]any)
	if !ok {
		t.Fatalf("response_format missing in request")
	}
	if got, want := responseFormat["type"], "json_schema"; got != want {
		t.Fatalf("response_format.type got %v want %v", got, want)
	}

	jsonSchema, ok := responseFormat["json_schema"].(map[string]any)
	if !ok {
		t.Fatalf("response_format.json_schema missing in request")
	}
	if got, want := jsonSchema["name"], llmAnnotationSchemaName; got != want {
		t.Fatalf("json_schema.name got %v want %v", got, want)
	}
	if got, want := jsonSchema["strict"], true; got != want {
		t.Fatalf("json_schema.strict got %v want %v", got, want)
	}

	var annotation map[string]any
	if err := json.Unmarshal(result, &annotation); err != nil {
		t.Fatalf("result is not valid json object: %v", err)
	}
	if _, ok := annotation["summary"]; !ok {
		t.Fatalf("annotation missing summary field")
	}
}

type fakeHTTPDoer struct {
	statusCode  int
	body        string
	requestBody []byte
}

func (f *fakeHTTPDoer) Do(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	f.requestBody = append([]byte(nil), body...)

	return &http.Response{
		StatusCode: f.statusCode,
		Body:       io.NopCloser(strings.NewReader(f.body)),
		Header:     make(http.Header),
	}, nil
}
