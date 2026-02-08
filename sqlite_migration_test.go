package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMigrate_RowCount(t *testing.T) {
	tmp := t.TempDir()
	in := filepath.Join(tmp, "annotations.jsonl")
	dbPath := filepath.Join(tmp, "annotations.db")

	lines := []string{
		testJSONLRecord("conv_1", 1, "Sales Rep", "Sales Rep", true, nil, true, true, "support", 0.9),
		testJSONLRecord("conv_1", 2, "Customer", "Customer", true, nil, false, false, "none", 0),
	}
	if err := os.WriteFile(in, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}

	inserted, err := MigrateJSONLToSQLite(in, dbPath)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if inserted != 2 {
		t.Fatalf("inserted=%d want 2", inserted)
	}

	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	var got int
	if err := db.QueryRow(`SELECT COUNT(*) FROM annotations`).Scan(&got); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if got != 2 {
		t.Fatalf("row count=%d want 2", got)
	}
}

func TestMigrate_FieldMapping(t *testing.T) {
	tmp := t.TempDir()
	in := filepath.Join(tmp, "annotations.jsonl")
	dbPath := filepath.Join(tmp, "annotations.db")

	line := testJSONLRecord("conv_map", 7, "Sales Rep", "Customer", true, []string{"quality:speaker_mismatch"}, true, true, "validation", 0.7)
	if err := os.WriteFile(in, []byte(line+"\n"), 0o644); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}

	if _, err := MigrateJSONLToSQLite(in, dbPath); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	db, err := openSQLite(dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	var predicted string
	var speakerMatch int
	var empathyRan int
	var empathyType string
	if err := db.QueryRow(`
		SELECT speaker_predicted, speaker_match, empathy_ran, empathy_type
		FROM annotations
		WHERE conversation_id = 'conv_map' AND replica_id = 7
	`).Scan(&predicted, &speakerMatch, &empathyRan, &empathyType); err != nil {
		t.Fatalf("query row: %v", err)
	}

	if predicted != "Customer" {
		t.Fatalf("predicted=%q want Customer", predicted)
	}
	if speakerMatch != 0 {
		t.Fatalf("speaker_match=%d want 0", speakerMatch)
	}
	if empathyRan != 1 {
		t.Fatalf("empathy_ran=%d want 1", empathyRan)
	}
	if empathyType != "validation" {
		t.Fatalf("empathy_type=%q want validation", empathyType)
	}
}

func TestMigrate_RejectsInvalidJSONLine(t *testing.T) {
	tmp := t.TempDir()
	in := filepath.Join(tmp, "annotations.jsonl")
	dbPath := filepath.Join(tmp, "annotations.db")

	content := strings.Join([]string{
		testJSONLRecord("conv_1", 1, "Sales Rep", "Sales Rep", true, nil, true, true, "support", 0.9),
		`{"broken_json":`,
	}, "\n")
	if err := os.WriteFile(in, []byte(content), 0o644); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}

	_, err := MigrateJSONLToSQLite(in, dbPath)
	if err == nil {
		t.Fatalf("expected error for invalid line")
	}
	if !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("error=%q want line number", err)
	}
}

func TestReport_ComputesSpeakerAccuracy(t *testing.T) {
	tmp := t.TempDir()
	in := filepath.Join(tmp, "annotations.jsonl")
	dbPath := filepath.Join(tmp, "annotations.db")

	lines := []string{
		testJSONLRecord("conv_a", 1, "Sales Rep", "Sales Rep", true, nil, true, true, "support", 0.9),
		testJSONLRecord("conv_a", 2, "Customer", "Customer", true, nil, false, false, "none", 0),
		testJSONLRecord("conv_b", 1, "Sales Rep", "Customer", true, []string{"quality:speaker_mismatch"}, true, true, "validation", 0.8),
	}
	if err := os.WriteFile(in, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}

	if _, err := MigrateJSONLToSQLite(in, dbPath); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	report, err := BuildReport(dbPath)
	if err != nil {
		t.Fatalf("build report: %v", err)
	}

	if report.TotalRows != 3 {
		t.Fatalf("total rows=%d want 3", report.TotalRows)
	}
	if report.SpeakerMatchCount != 2 {
		t.Fatalf("speaker match=%d want 2", report.SpeakerMatchCount)
	}
	if report.QualitySpeakerMismatchCount != 1 {
		t.Fatalf("quality mismatch count=%d want 1", report.QualitySpeakerMismatchCount)
	}
	if report.TotalConversations != 2 {
		t.Fatalf("total conversations=%d want 2", report.TotalConversations)
	}
}

func TestReport_EmptahyDistribution(t *testing.T) {
	tmp := t.TempDir()
	in := filepath.Join(tmp, "annotations.jsonl")
	dbPath := filepath.Join(tmp, "annotations.db")

	lines := []string{
		testJSONLRecord("conv_a", 1, "Sales Rep", "Sales Rep", true, nil, true, true, "support", 0.9),
		testJSONLRecord("conv_a", 2, "Customer", "Customer", true, nil, false, false, "none", 0),
		testJSONLRecord("conv_b", 1, "Sales Rep", "Sales Rep", true, nil, true, true, "validation", 0.6),
	}
	if err := os.WriteFile(in, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}

	if _, err := MigrateJSONLToSQLite(in, dbPath); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	report, err := BuildReport(dbPath)
	if err != nil {
		t.Fatalf("build report: %v", err)
	}

	distribution := map[string]int{}
	for _, item := range report.EmpathyTypeDistribution {
		distribution[item.Type] = item.Count
	}
	if distribution["support"] != 1 || distribution["validation"] != 1 || distribution["none"] != 1 {
		t.Fatalf("unexpected empathy distribution: %+v", distribution)
	}
	if report.EmpathyRanCount != 2 {
		t.Fatalf("empathy_ran=%d want 2", report.EmpathyRanCount)
	}
	if report.EmpathyPresentCount != 2 {
		t.Fatalf("empathy_present=%d want 2", report.EmpathyPresentCount)
	}
}

func TestBusinessProcess_RoutingForEmpathy(t *testing.T) {
	speaker := &fakeSpeakerCase{
		result: ReplicaCaseResult{
			PredictedSpeaker: "Customer",
			Confidence:       0.8,
			EvidenceQuote:    "quote",
		},
	}
	empathy := &fakeEmpathyCase{
		result: EmpathyCaseResult{
			Ran:            true,
			EmpathyPresent: true,
			EmpathyType:    "support",
			Confidence:     0.9,
			EvidenceQuote:  "quote",
		},
	}

	process := AnnotationBusinessProcess{
		SpeakerCase: speaker,
		EmpathyCase: empathy,
	}

	outCustomer, err := process.Run(context.Background(), ProcessInput{
		ReplicaText: "hello",
		SpeakerTrue: "Customer",
	})
	if err != nil {
		t.Fatalf("run customer: %v", err)
	}
	if outCustomer.Empathy.Ran {
		t.Fatalf("expected empathy skip for customer")
	}
	if empathy.calls != 0 {
		t.Fatalf("empathy calls=%d want 0", empathy.calls)
	}

	outSales, err := process.Run(context.Background(), ProcessInput{
		ReplicaText: "hello",
		SpeakerTrue: "Sales Rep",
	})
	if err != nil {
		t.Fatalf("run sales rep: %v", err)
	}
	if !outSales.Empathy.Ran {
		t.Fatalf("expected empathy run for sales rep")
	}
	if empathy.calls != 1 {
		t.Fatalf("empathy calls=%d want 1", empathy.calls)
	}
}

type fakeSpeakerCase struct {
	result ReplicaCaseResult
	err    error
}

func (f *fakeSpeakerCase) Evaluate(ctx context.Context, in ReplicaCaseInput) (ReplicaCaseResult, error) {
	if f.err != nil {
		return ReplicaCaseResult{}, f.err
	}
	return f.result, nil
}

type fakeEmpathyCase struct {
	result EmpathyCaseResult
	err    error
	calls  int
}

func (f *fakeEmpathyCase) Evaluate(ctx context.Context, in EmpathyCaseInput) (EmpathyCaseResult, error) {
	f.calls++
	if f.err != nil {
		return EmpathyCaseResult{}, f.err
	}
	return f.result, nil
}

func testJSONLRecord(
	conversationID string,
	replicaID int,
	speakerTrue string,
	predictedSpeaker string,
	speakerOK bool,
	speakerValidationErrors []string,
	empathyRan bool,
	empathyPresent bool,
	empathyType string,
	empathyConfidence float64,
) string {
	if speakerValidationErrors == nil {
		speakerValidationErrors = []string{}
	}

	empathyEvidence := "[]"
	if empathyPresent {
		empathyEvidence = `[{"quote":"I understand"}]`
	}
	return fmt.Sprintf(
		`{"conversation_id":"%s","replica_id":%d,"speaker_true":"%s","replica_text":"sample text","turn_ids":[1,2],"guided":{"unit_speaker":{"ok":%t,"attempts":1,"validation_errors":%s,"output":{"predicted_speaker":"%s","confidence":0.9,"evidence":{"quote":"sample"}}},"unit_empathy":{"ran":%t,"ok":true,"attempts":1,"validation_errors":[],"output":{"empathy_present":%t,"empathy_type":"%s","confidence":%v,"evidence":%s}}},"meta":{"model":"gpt-4.1-mini","timestamp_utc":"2026-02-08T10:00:00Z","openai_request_ids":["req1"]}}`,
		conversationID,
		replicaID,
		speakerTrue,
		speakerOK,
		marshalStringArray(speakerValidationErrors),
		predictedSpeaker,
		empathyRan,
		empathyPresent,
		empathyType,
		empathyConfidence,
		empathyEvidence,
	)
}

func TestOpenSQLite_EmptyPath(t *testing.T) {
	_, err := openSQLite("")
	if err == nil {
		t.Fatalf("expected error for empty path")
	}
}
