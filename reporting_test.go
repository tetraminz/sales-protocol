package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestAnalyticsMarkdown_ContainsCoreSections(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}

	insertSeededAnnotation(t, dbPath, seededAnnotation{
		ConversationID:    "conv_a",
		ReplicaID:         1,
		SpeakerTrue:       speakerSalesRep,
		SpeakerPredicted:  speakerSalesRep,
		SpeakerConfidence: 0.92,
		SpeakerMatch:      1,
		EmpathyConfidence: 0.86,
		ReviewStatus:      reviewStatusPending,
		ReplicaText:       "I understand your concern.",
	})
	insertSeededAnnotation(t, dbPath, seededAnnotation{
		ConversationID:    "conv_a",
		ReplicaID:         2,
		SpeakerTrue:       speakerCustomer,
		SpeakerPredicted:  speakerSalesRep,
		SpeakerConfidence: 0.65,
		SpeakerMatch:      0,
		EmpathyConfidence: 0,
		ReviewStatus:      reviewStatusPending,
		ReplicaText:       "Bye.",
	})

	md, err := BuildAnalyticsMarkdown(dbPath)
	if err != nil {
		t.Fatalf("build analytics markdown: %v", err)
	}

	for _, section := range []string{
		"# Analytics",
		"## Totals",
		"## Speaker Accuracy",
		"## Empathy Confidence",
		"## Manual Review",
		"## Short-Utterance Speaker Mismatches",
	} {
		if !strings.Contains(md, section) {
			t.Fatalf("analytics markdown missing %q", section)
		}
	}
}

func TestDebugMarkdown_ContainsRedConversations(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}

	insertSeededAnnotation(t, dbPath, seededAnnotation{
		ConversationID:    "conv_bad",
		ReplicaID:         1,
		SpeakerTrue:       speakerSalesRep,
		SpeakerPredicted:  speakerCustomer,
		SpeakerConfidence: 0.90,
		SpeakerMatch:      0,
		EmpathyConfidence: 0.73,
		ReviewStatus:      reviewStatusPending,
		ReplicaText:       "Goodbye.",
	})

	md, err := BuildReleaseDebugMarkdown(dbPath)
	if err != nil {
		t.Fatalf("build debug markdown: %v", err)
	}

	for _, item := range []string{
		"## Red Conversations",
		"conv_bad",
		"speaker_mismatch",
	} {
		if !strings.Contains(md, item) {
			t.Fatalf("debug markdown missing %q", item)
		}
	}
}

func TestReport_ConsoleSummaryContainsManualReviewStats(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}

	insertSeededAnnotation(t, dbPath, seededAnnotation{
		ConversationID:    "conv_a",
		ReplicaID:         1,
		SpeakerTrue:       speakerSalesRep,
		SpeakerPredicted:  speakerSalesRep,
		SpeakerConfidence: 0.88,
		SpeakerMatch:      1,
		EmpathyConfidence: 0.80,
		ReviewStatus:      reviewStatusPending,
		ReplicaText:       "I hear you.",
	})
	insertSeededAnnotation(t, dbPath, seededAnnotation{
		ConversationID:    "conv_a",
		ReplicaID:         2,
		SpeakerTrue:       speakerSalesRep,
		SpeakerPredicted:  speakerSalesRep,
		SpeakerConfidence: 0.84,
		SpeakerMatch:      1,
		EmpathyConfidence: 0.76,
		ReviewStatus:      reviewStatusOK,
		ReplicaText:       "Let's solve this.",
	})
	insertSeededAnnotation(t, dbPath, seededAnnotation{
		ConversationID:    "conv_b",
		ReplicaID:         1,
		SpeakerTrue:       speakerSalesRep,
		SpeakerPredicted:  speakerCustomer,
		SpeakerConfidence: 0.70,
		SpeakerMatch:      0,
		EmpathyConfidence: 0.61,
		ReviewStatus:      reviewStatusNotOK,
		ReviewerNote:      "Not empathetic enough.",
		ReplicaText:       "Thanks.",
	})

	report, err := BuildReport(dbPath)
	if err != nil {
		t.Fatalf("build report: %v", err)
	}
	text := FormatReport(report)

	for _, token := range []string{
		"speaker_accuracy_percent=",
		"empathy_review_pending=",
		"empathy_review_ok=",
		"empathy_review_not_ok=",
	} {
		if !strings.Contains(text, token) {
			t.Fatalf("report output missing %q", token)
		}
	}
}
