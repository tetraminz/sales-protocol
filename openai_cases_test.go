package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSpeakerCase_LLMOnly(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		content := mustJSONString(t, map[string]any{
			"predicted_speaker":     speakerCustomer,
			"confidence":            0.31,
			"is_farewell_utterance": false,
			"is_farewell_context":   false,
			"context_source":        "none",
			"evidence": map[string]any{
				"quote": "Thanks",
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
		model: defaultAnnotateModel,
	}

	out, err := c.Evaluate(context.Background(), ReplicaCaseInput{
		ReplicaText: "Thanks for your help.",
		PrevText:    "Can you help me?",
		NextText:    "Sure",
	})
	if err != nil {
		t.Fatalf("evaluate speaker case: %v", err)
	}
	if out.PredictedSpeaker != speakerCustomer {
		t.Fatalf("predicted speaker=%q want=%q", out.PredictedSpeaker, speakerCustomer)
	}
}

func TestEmpathyCase_ReturnsConfidenceForSalesRep(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		content := mustJSONString(t, map[string]any{
			"confidence": 0.74,
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
			t.Fatalf("encode mock response: %v", err)
		}
	}))
	defer mock.Close()

	c := &openAIEmpathyCase{
		client: &openAIClient{
			apiKey:  "test_key",
			baseURL: mock.URL,
			httpClient: &http.Client{
				Timeout: 5 * time.Second,
			},
		},
		model: defaultAnnotateModel,
	}

	out, err := c.Evaluate(context.Background(), EmpathyCaseInput{
		ReplicaText: "I understand your concern and can help.",
		SpeakerTrue: speakerSalesRep,
	})
	if err != nil {
		t.Fatalf("evaluate empathy case: %v", err)
	}
	if !out.Ran {
		t.Fatalf("empathy case must run")
	}
	if out.Confidence != 0.74 {
		t.Fatalf("confidence=%v want=0.74", out.Confidence)
	}
}

func TestEmpathyCase_SkipsCustomerReplica(t *testing.T) {
	empathyCase := &countingEmpathyCase{}
	process := AnnotationBusinessProcess{
		SpeakerCase: staticSpeakerCase{out: ReplicaCaseResult{
			PredictedSpeaker: speakerCustomer,
			Confidence:       0.9,
			EvidenceQuote:    "Thanks",
		}},
		EmpathyCase: empathyCase,
	}

	out, err := process.Run(context.Background(), ProcessInput{
		ReplicaText: "Thanks for the call.",
		PrevText:    "",
		NextText:    "",
		SpeakerTrue: speakerCustomer,
	})
	if err != nil {
		t.Fatalf("run process: %v", err)
	}
	if out.Empathy.Ran {
		t.Fatalf("empathy should be skipped for customer rows")
	}
	if empathyCase.calls != 0 {
		t.Fatalf("empathy case calls=%d want 0", empathyCase.calls)
	}
}

func TestBusinessProcess_FarewellContextOverridesMismatch(t *testing.T) {
	empathyCase := &countingEmpathyCase{}
	process := AnnotationBusinessProcess{
		SpeakerCase: staticSpeakerCase{out: ReplicaCaseResult{
			PredictedSpeaker:      speakerCustomer,
			Confidence:            0.93,
			EvidenceQuote:         "Goodbye!",
			FarewellUtterance:     true,
			FarewellContext:       true,
			FarewellContextSource: farewellContextSourceCurrent,
		}},
		EmpathyCase: empathyCase,
	}

	out, err := process.Run(context.Background(), ProcessInput{
		ReplicaText: "Goodbye!",
		PrevText:    "Thanks, bye.",
		NextText:    "",
		SpeakerTrue: speakerSalesRep,
	})
	if err != nil {
		t.Fatalf("run process: %v", err)
	}
	if out.Speaker.PredictedSpeaker != speakerCustomer {
		t.Fatalf("predicted speaker=%q want=%q", out.Speaker.PredictedSpeaker, speakerCustomer)
	}
	if out.Speaker.QualityMismatch {
		t.Fatalf("quality mismatch must be overridden for farewell context")
	}
	if out.Speaker.QualityDecision != qualityDecisionFarewellOverride {
		t.Fatalf("quality decision=%q want=%q", out.Speaker.QualityDecision, qualityDecisionFarewellOverride)
	}
}

func TestBusinessProcess_StrictMismatchWithoutFarewellContext(t *testing.T) {
	empathyCase := &countingEmpathyCase{}
	process := AnnotationBusinessProcess{
		SpeakerCase: staticSpeakerCase{out: ReplicaCaseResult{
			PredictedSpeaker:      speakerCustomer,
			Confidence:            0.91,
			EvidenceQuote:         "Hello",
			FarewellUtterance:     false,
			FarewellContext:       false,
			FarewellContextSource: farewellContextSourceNone,
		}},
		EmpathyCase: empathyCase,
	}

	out, err := process.Run(context.Background(), ProcessInput{
		ReplicaText: "Hello Sarah, thanks for taking my call.",
		PrevText:    "",
		NextText:    "Hi Mark.",
		SpeakerTrue: speakerSalesRep,
	})
	if err != nil {
		t.Fatalf("run process: %v", err)
	}
	if !out.Speaker.QualityMismatch {
		t.Fatalf("quality mismatch should stay strict when farewell context is false")
	}
	if out.Speaker.QualityDecision != qualityDecisionStrictMismatch {
		t.Fatalf("quality decision=%q want=%q", out.Speaker.QualityDecision, qualityDecisionStrictMismatch)
	}
}

type staticSpeakerCase struct {
	out ReplicaCaseResult
}

func (s staticSpeakerCase) Evaluate(context.Context, ReplicaCaseInput) (ReplicaCaseResult, error) {
	return s.out, nil
}

type countingEmpathyCase struct {
	calls int
}

func (c *countingEmpathyCase) Evaluate(context.Context, EmpathyCaseInput) (EmpathyCaseResult, error) {
	c.calls++
	return EmpathyCaseResult{
		Ran:            true,
		EmpathyPresent: true,
		EmpathyType:    "acknowledgement",
		Confidence:     0.5,
		EvidenceQuote:  "x",
	}, nil
}
