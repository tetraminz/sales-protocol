package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAnalyticsMarkdown_ContainsRawFinalAndQualityCounters(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}

	insertSeededAnnotation(t, dbPath, AnnotationRow{
		ConversationID:                "conv_a",
		UtteranceIndex:                1,
		UtteranceText:                 "Hello",
		GroundTruthSpeaker:            speakerSalesRep,
		PredictedSpeaker:              speakerSalesRep,
		PredictedSpeakerConfidence:    0.92,
		SpeakerIsCorrectRaw:           true,
		SpeakerIsCorrectFinal:         true,
		SpeakerQualityDecision:        qualityDecisionStrictMatch,
		FarewellIsCurrentUtterance:    false,
		FarewellIsConversationClosing: false,
		FarewellContextSource:         farewellContextSourceNone,
		SpeakerEvidenceQuote:          "Hello",
		SpeakerEvidenceIsValid:        true,
		EmpathyApplicable:             true,
		EmpathyPresent:                true,
		EmpathyConfidence:             0.8,
		EmpathyEvidenceQuote:          "Hello",
		EmpathyReviewStatus:           reviewStatusPending,
		Model:                         "seed_model",
	})
	insertSeededAnnotation(t, dbPath, AnnotationRow{
		ConversationID:                "conv_a",
		UtteranceIndex:                2,
		UtteranceText:                 "Goodbye!",
		GroundTruthSpeaker:            speakerSalesRep,
		PredictedSpeaker:              speakerCustomer,
		PredictedSpeakerConfidence:    0.81,
		SpeakerIsCorrectRaw:           false,
		SpeakerIsCorrectFinal:         true,
		SpeakerQualityDecision:        qualityDecisionFarewellOverride,
		FarewellIsCurrentUtterance:    true,
		FarewellIsConversationClosing: true,
		FarewellContextSource:         farewellContextSourceCurrent,
		SpeakerEvidenceQuote:          "",
		SpeakerEvidenceIsValid:        false,
		EmpathyApplicable:             true,
		EmpathyPresent:                false,
		EmpathyConfidence:             0.4,
		EmpathyEvidenceQuote:          "",
		EmpathyReviewStatus:           reviewStatusPending,
		Model:                         "seed_model",
	})

	md, err := BuildAnalyticsMarkdown(dbPath)
	if err != nil {
		t.Fatalf("build analytics markdown: %v", err)
	}

	for _, token := range []string{
		"speaker_accuracy_raw_percent",
		"speaker_accuracy_final_percent",
		"farewell_override_count",
		"speaker_evidence_invalid_count",
		"empathy_review_pending_applicable",
	} {
		if !strings.Contains(md, token) {
			t.Fatalf("analytics markdown missing %q", token)
		}
	}
}

func TestBuildReport_PendingReviewCountsOnlyApplicableRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}

	insertSeededAnnotation(t, dbPath, AnnotationRow{
		ConversationID:             "conv_a",
		UtteranceIndex:             1,
		UtteranceText:              "Sales row",
		GroundTruthSpeaker:         speakerSalesRep,
		PredictedSpeaker:           speakerSalesRep,
		PredictedSpeakerConfidence: 0.9,
		SpeakerIsCorrectRaw:        true,
		SpeakerIsCorrectFinal:      true,
		SpeakerQualityDecision:     qualityDecisionStrictMatch,
		FarewellContextSource:      farewellContextSourceNone,
		SpeakerEvidenceQuote:       "Sales",
		SpeakerEvidenceIsValid:     true,
		EmpathyApplicable:          true,
		EmpathyPresent:             true,
		EmpathyConfidence:          0.7,
		EmpathyEvidenceQuote:       "Sales",
		EmpathyReviewStatus:        reviewStatusPending,
		Model:                      "seed_model",
	})
	insertSeededAnnotation(t, dbPath, AnnotationRow{
		ConversationID:             "conv_b",
		UtteranceIndex:             1,
		UtteranceText:              "Customer row",
		GroundTruthSpeaker:         speakerCustomer,
		PredictedSpeaker:           speakerCustomer,
		PredictedSpeakerConfidence: 0.88,
		SpeakerIsCorrectRaw:        true,
		SpeakerIsCorrectFinal:      true,
		SpeakerQualityDecision:     qualityDecisionStrictMatch,
		FarewellContextSource:      farewellContextSourceNone,
		SpeakerEvidenceQuote:       "Customer",
		SpeakerEvidenceIsValid:     true,
		EmpathyApplicable:          false,
		EmpathyPresent:             false,
		EmpathyConfidence:          0,
		EmpathyEvidenceQuote:       "",
		EmpathyReviewStatus:        reviewStatusNotApplicable,
		Model:                      "seed_model",
	})

	report, err := BuildReport(dbPath)
	if err != nil {
		t.Fatalf("build report: %v", err)
	}
	if report.EmpathyReviewPendingCount != 1 {
		t.Fatalf("pending applicable=%d want=1", report.EmpathyReviewPendingCount)
	}
}

func TestDebugMarkdown_ContainsRawAndFinalSections(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "annotations.db")
	if err := SetupSQLite(dbPath); err != nil {
		t.Fatalf("setup sqlite: %v", err)
	}

	insertSeededAnnotation(t, dbPath, AnnotationRow{
		ConversationID:             "conv_bad",
		UtteranceIndex:             1,
		UtteranceText:              "Hello",
		GroundTruthSpeaker:         speakerSalesRep,
		PredictedSpeaker:           speakerCustomer,
		PredictedSpeakerConfidence: 0.8,
		SpeakerIsCorrectRaw:        false,
		SpeakerIsCorrectFinal:      false,
		SpeakerQualityDecision:     qualityDecisionStrictMismatch,
		FarewellContextSource:      farewellContextSourceNone,
		SpeakerEvidenceQuote:       "Hello",
		SpeakerEvidenceIsValid:     true,
		EmpathyApplicable:          true,
		EmpathyPresent:             false,
		EmpathyConfidence:          0.3,
		EmpathyEvidenceQuote:       "Hello",
		EmpathyReviewStatus:        reviewStatusPending,
		Model:                      "seed_model",
	})

	md, err := BuildReleaseDebugMarkdown(dbPath)
	if err != nil {
		t.Fatalf("build debug markdown: %v", err)
	}

	for _, token := range []string{
		"Red Conversations (Raw)",
		"Red Conversations (Final)",
		"Top Raw Mismatches",
		"Top Evidence Invalid",
	} {
		if !strings.Contains(md, token) {
			t.Fatalf("debug markdown missing %q", token)
		}
	}
}

func insertSeededAnnotation(t *testing.T, dbPath string, row AnnotationRow) {
	t.Helper()
	store, err := OpenSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer store.Close()

	if strings.TrimSpace(row.AnnotatedAtUTC) == "" {
		row.AnnotatedAtUTC = time.Now().UTC().Format(time.RFC3339)
	}
	if strings.TrimSpace(row.Model) == "" {
		row.Model = "seed_model"
	}
	if err := store.InsertAnnotation(row); err != nil {
		t.Fatalf("insert seeded annotation: %v", err)
	}
}
