package main

import (
	"context"
	"testing"
)

func TestBusinessProcess_FarewellContextOverridesFinalMismatch(t *testing.T) {
	process := AnnotationBusinessProcess{
		SpeakerUnit: staticSpeakerUnit{out: SpeakerCaseResult{
			PredictedSpeaker:              speakerCustomer,
			PredictedSpeakerConfidence:    0.92,
			FarewellIsCurrentUtterance:    true,
			FarewellIsConversationClosing: true,
			FarewellContextSource:         farewellContextSourceCurrent,
			SpeakerEvidenceQuote:          "Goodbye!",
			SpeakerEvidenceIsValid:        true,
		}},
		EmpathyUnit: staticEmpathyUnit{out: EmpathyCaseResult{
			EmpathyPresent:         false,
			EmpathyConfidence:      0.0,
			EmpathyEvidenceQuote:   "",
			EmpathyEvidenceIsValid: false,
		}},
	}

	out, err := process.Run(context.Background(), ProcessInput{
		UtteranceText:      "Goodbye!",
		PreviousText:       "Thanks, bye.",
		NextText:           "",
		GroundTruthSpeaker: speakerSalesRep,
	})
	if err != nil {
		t.Fatalf("process run: %v", err)
	}
	if out.Speaker.SpeakerIsCorrectRaw {
		t.Fatalf("speaker_is_correct_raw=true, want false")
	}
	if !out.Speaker.SpeakerIsCorrectFinal {
		t.Fatalf("speaker_is_correct_final=false, want true")
	}
	if out.Speaker.SpeakerQualityDecision != qualityDecisionFarewellOverride {
		t.Fatalf("speaker_quality_decision=%q want=%q", out.Speaker.SpeakerQualityDecision, qualityDecisionFarewellOverride)
	}
}

func TestBusinessProcess_StrictMismatchWithoutFarewell(t *testing.T) {
	process := AnnotationBusinessProcess{
		SpeakerUnit: staticSpeakerUnit{out: SpeakerCaseResult{
			PredictedSpeaker:              speakerCustomer,
			PredictedSpeakerConfidence:    0.88,
			FarewellIsCurrentUtterance:    false,
			FarewellIsConversationClosing: false,
			FarewellContextSource:         farewellContextSourceNone,
			SpeakerEvidenceQuote:          "Hello",
			SpeakerEvidenceIsValid:        true,
		}},
		EmpathyUnit: staticEmpathyUnit{out: EmpathyCaseResult{}},
	}

	out, err := process.Run(context.Background(), ProcessInput{
		UtteranceText:      "Hello Sarah, thanks for taking my call.",
		PreviousText:       "",
		NextText:           "Hi Mark",
		GroundTruthSpeaker: speakerSalesRep,
	})
	if err != nil {
		t.Fatalf("process run: %v", err)
	}
	if out.Speaker.SpeakerIsCorrectRaw {
		t.Fatalf("speaker_is_correct_raw=true, want false")
	}
	if out.Speaker.SpeakerIsCorrectFinal {
		t.Fatalf("speaker_is_correct_final=true, want false")
	}
	if out.Speaker.SpeakerQualityDecision != qualityDecisionStrictMismatch {
		t.Fatalf("speaker_quality_decision=%q want=%q", out.Speaker.SpeakerQualityDecision, qualityDecisionStrictMismatch)
	}
}

func TestBusinessProcess_EmathyRoutingUsesGroundTruthSpeaker(t *testing.T) {
	empathy := &countingEmpathyUnit{out: EmpathyCaseResult{
		EmpathyPresent:         true,
		EmpathyConfidence:      0.7,
		EmpathyEvidenceQuote:   "I understand",
		EmpathyEvidenceIsValid: true,
	}}
	process := AnnotationBusinessProcess{
		SpeakerUnit: staticSpeakerUnit{out: SpeakerCaseResult{
			PredictedSpeaker:              speakerCustomer,
			PredictedSpeakerConfidence:    0.6,
			FarewellIsCurrentUtterance:    false,
			FarewellIsConversationClosing: false,
			FarewellContextSource:         farewellContextSourceNone,
			SpeakerEvidenceQuote:          "Thanks",
			SpeakerEvidenceIsValid:        true,
		}},
		EmpathyUnit: empathy,
	}

	customerOut, err := process.Run(context.Background(), ProcessInput{
		UtteranceText:      "Thanks",
		PreviousText:       "",
		NextText:           "",
		GroundTruthSpeaker: speakerCustomer,
	})
	if err != nil {
		t.Fatalf("customer run: %v", err)
	}
	if customerOut.Empathy.EmpathyApplicable {
		t.Fatalf("customer empathy_applicable=true, want false")
	}
	if empathy.calls != 0 {
		t.Fatalf("empathy calls=%d want 0", empathy.calls)
	}

	salesOut, err := process.Run(context.Background(), ProcessInput{
		UtteranceText:      "I understand your concern",
		PreviousText:       "",
		NextText:           "",
		GroundTruthSpeaker: speakerSalesRep,
	})
	if err != nil {
		t.Fatalf("sales run: %v", err)
	}
	if !salesOut.Empathy.EmpathyApplicable {
		t.Fatalf("sales empathy_applicable=false, want true")
	}
	if empathy.calls != 1 {
		t.Fatalf("empathy calls=%d want 1", empathy.calls)
	}
}

type staticSpeakerUnit struct {
	out SpeakerCaseResult
}

func (s staticSpeakerUnit) Evaluate(context.Context, SpeakerCaseInput) (SpeakerCaseResult, error) {
	return s.out, nil
}

type staticEmpathyUnit struct {
	out EmpathyCaseResult
}

func (s staticEmpathyUnit) Evaluate(context.Context, EmpathyCaseInput) (EmpathyCaseResult, error) {
	return s.out, nil
}

type countingEmpathyUnit struct {
	calls int
	out   EmpathyCaseResult
}

func (c *countingEmpathyUnit) Evaluate(context.Context, EmpathyCaseInput) (EmpathyCaseResult, error) {
	c.calls++
	return c.out, nil
}
