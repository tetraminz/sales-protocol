package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type fakeResponse struct {
	content string
	reqID   string
	err     error
}

type fakeLLM struct {
	responses []fakeResponse
	calls     int
}

func (f *fakeLLM) GenerateStructured(ctx context.Context, model string, messages []LLMMessage, schemaName string, schema map[string]any) (string, string, error) {
	if f.calls >= len(f.responses) {
		return "", "", context.DeadlineExceeded
	}
	resp := f.responses[f.calls]
	f.calls++
	return resp.content, resp.reqID, resp.err
}

func TestNormalizeSpeaker(t *testing.T) {
	cases := map[string]string{
		"**Sales Rep":           speakerSalesRep,
		" sales representative": speakerSalesRep,
		"Customer":              speakerCustomer,
		"  CLIENT ":             speakerCustomer,
		"unknown":               "unknown",
	}
	for in, want := range cases {
		if got := normalizeSpeaker(in); got != want {
			t.Fatalf("normalizeSpeaker(%q)=%q want %q", in, got, want)
		}
	}
}

func TestBuildReplicas(t *testing.T) {
	turns := []Turn{
		{ConversationID: "c1", TurnID: 1, Speaker: speakerSalesRep, Text: "hello"},
		{ConversationID: "c1", TurnID: 2, Speaker: speakerSalesRep, Text: "follow up"},
		{ConversationID: "c1", TurnID: 3, Speaker: speakerCustomer, Text: "answer"},
	}
	replicas := buildReplicas(turns)
	if len(replicas) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(replicas))
	}
	if replicas[0].Text != "hello\nfollow up" {
		t.Fatalf("unexpected first replica text: %q", replicas[0].Text)
	}
	if !reflect.DeepEqual(replicas[0].TurnIDs, []int{1, 2}) {
		t.Fatalf("unexpected turn ids: %v", replicas[0].TurnIDs)
	}
}

func TestRunSpeakerUnitMismatchQualityError(t *testing.T) {
	fake := &fakeLLM{
		responses: []fakeResponse{
			{content: `{"predicted_speaker":"Customer","confidence":0.8,"evidence":{"quote":"Let me check"}}`, reqID: "r1"},
			{content: `{"predicted_speaker":"Customer","confidence":0.7,"evidence":{"quote":"Let me check"}}`, reqID: "r2"},
		},
	}

	replica := Replica{SpeakerTrue: speakerSalesRep, Text: "Let me check that for you"}
	cfg := Config{Model: "gpt-4.1-mini", MaxRetries: 0, DryRun: false}
	res, reqIDs := runSpeakerUnit(context.Background(), fake, cfg, replica, "", "")

	if !res.OK {
		t.Fatalf("expected ok=true for format-valid mismatch case")
	}
	if res.Attempts != 2 {
		t.Fatalf("expected 2 attempts (initial+quality retry), got %d", res.Attempts)
	}
	if len(reqIDs) != 2 {
		t.Fatalf("expected 2 request ids, got %d", len(reqIDs))
	}
	if !containsString(res.ValidationErrors, "quality:speaker_mismatch") {
		t.Fatalf("expected quality mismatch marker, got %v", res.ValidationErrors)
	}
}

func TestRunSpeakerUnitFormatRetry(t *testing.T) {
	fake := &fakeLLM{
		responses: []fakeResponse{
			{content: `{"predicted_speaker":"Sales Rep","confidence":1.4,"evidence":{"quote":"missing"}}`, reqID: "r1"},
			{content: `{"predicted_speaker":"Sales Rep","confidence":0.9,"evidence":{"quote":"hello"}}`, reqID: "r2"},
		},
	}

	replica := Replica{SpeakerTrue: speakerSalesRep, Text: "hello there"}
	cfg := Config{Model: "gpt-4.1-mini", MaxRetries: 1, DryRun: false}
	res, _ := runSpeakerUnit(context.Background(), fake, cfg, replica, "", "")

	if !res.OK {
		t.Fatalf("expected OK after retry, errors=%v", res.ValidationErrors)
	}
	var out SpeakerAttributionOutput
	if err := json.Unmarshal(res.Output, &out); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if out.Confidence != 0.9 {
		t.Fatalf("expected clamped/kept confidence 0.9, got %v", out.Confidence)
	}
}

func TestRunEmpathyUnitBranchingNonSalesRep(t *testing.T) {
	replica := Replica{SpeakerTrue: speakerCustomer, Text: "I am upset"}
	cfg := Config{Model: "gpt-4.1-mini", MaxRetries: 1, DryRun: false}
	res, reqIDs := runEmpathyUnit(context.Background(), &fakeLLM{}, cfg, replica)

	if res.Ran {
		t.Fatalf("expected ran=false for non-sales-rep")
	}
	if !res.OK {
		t.Fatalf("expected ok=true for skipped branch")
	}
	if res.Attempts != 0 {
		t.Fatalf("expected attempts=0, got %d", res.Attempts)
	}
	if len(reqIDs) != 0 {
		t.Fatalf("expected no request ids")
	}
	var out EmpathyDetectionOutput
	if err := json.Unmarshal(res.Output, &out); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if out.EmpathyPresent || out.EmpathyType != "none" || out.Confidence != 0 || len(out.Evidence) != 0 {
		t.Fatalf("unexpected default empathy output: %+v", out)
	}
}

func TestRunEmpathyUnitRetryValidation(t *testing.T) {
	fake := &fakeLLM{
		responses: []fakeResponse{
			{content: `{"empathy_present":true,"empathy_type":"support","confidence":0.3,"evidence":[{"quote":"not found"}]}`, reqID: "e1"},
			{content: `{"empathy_present":true,"empathy_type":"support","confidence":1.3,"evidence":[{"quote":"I understand this is frustrating"}]}`, reqID: "e2"},
		},
	}
	replica := Replica{SpeakerTrue: speakerSalesRep, Text: "I understand this is frustrating, and I can help."}
	cfg := Config{Model: "gpt-4.1-mini", MaxRetries: 1, DryRun: false}
	res, _ := runEmpathyUnit(context.Background(), fake, cfg, replica)

	if !res.Ran {
		t.Fatalf("expected ran=true")
	}
	if !res.OK {
		t.Fatalf("expected ok=true after retry, errors=%v", res.ValidationErrors)
	}
	var out EmpathyDetectionOutput
	if err := json.Unmarshal(res.Output, &out); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if out.Confidence != 1 {
		t.Fatalf("expected confidence to be clamped to 1, got %v", out.Confidence)
	}
}

func TestReadTurnsAndRunDry(t *testing.T) {
	tmp := t.TempDir()
	csvPath := filepath.Join(tmp, "conv.csv")
	csvData := strings.Join([]string{
		"Conversation,Chunk_id,Speaker,Text,Embedding",
		"conv_1,1,**Sales Rep,Hello there,[1,2,3]",
		"conv_1,2,**Sales Rep,Can I help?,[1,2,3]",
		"conv_1,3,Customer,Need discount,[1,2,3]",
	}, "\n")
	if err := os.WriteFile(csvPath, []byte(csvData), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	turns, err := readTurns(csvPath)
	if err != nil {
		t.Fatalf("readTurns: %v", err)
	}
	if len(turns) != 3 {
		t.Fatalf("expected 3 turns, got %d", len(turns))
	}
	if turns[0].Speaker != speakerSalesRep {
		t.Fatalf("expected normalized speaker, got %q", turns[0].Speaker)
	}

	out := filepath.Join(tmp, "out.jsonl")
	cfg := Config{
		InputDir:           tmp,
		OutJSONL:           out,
		LimitConversations: 1,
		Model:              "gpt-4.1-mini",
		MaxRetries:         1,
		DryRun:             true,
	}
	if err := run(context.Background(), cfg); err != nil {
		t.Fatalf("run dry: %v", err)
	}

	content, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read out jsonl: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 replicas in output, got %d", len(lines))
	}
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}
