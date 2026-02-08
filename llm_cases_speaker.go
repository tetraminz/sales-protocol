package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type speakerLLMOutput struct {
	Farewell struct {
		IsCurrentFarewell bool   `json:"is_current_farewell"`
		IsClosingContext  bool   `json:"is_closing_context"`
		ContextSource     string `json:"context_source"`
	} `json:"farewell"`
	Speaker struct {
		PredictedSpeaker string  `json:"predicted_speaker"`
		Confidence       float64 `json:"confidence"`
		EvidenceQuote    string  `json:"evidence_quote"`
	} `json:"speaker"`
}

type openAISpeakerCase struct {
	client         *openAIClient
	model          string
	store          *SQLiteStore
	conversationID string
	utteranceIndex int
}

func newOpenAISpeakerCase(client *openAIClient, model string, store *SQLiteStore) *openAISpeakerCase {
	return &openAISpeakerCase{
		client: client,
		model:  strings.TrimSpace(model),
		store:  store,
	}
}

func (c *openAISpeakerCase) setLogContext(conversationID string, utteranceIndex int) {
	c.conversationID = strings.TrimSpace(conversationID)
	c.utteranceIndex = utteranceIndex
}

func (c *openAISpeakerCase) Evaluate(ctx context.Context, in SpeakerCaseInput) (SpeakerCaseResult, error) {
	if c.client == nil {
		return fallbackSpeakerCaseResult(), nil
	}
	if c.store == nil {
		return SpeakerCaseResult{}, fmt.Errorf("speaker case store is required")
	}

	messages := speakerMessages(in.PreviousText, in.CurrentText, in.NextText)
	schema := speakerSchema()
	for attempt := 1; attempt <= maxLLMAttempts; attempt++ {
		callResult, callErr := c.client.CallStrictJSONSchema(ctx, c.model, messages, "speaker_case_v2", schema)

		candidate := fallbackSpeakerCaseResult()
		parseOK := false
		validationOK := false
		errorMessage := ""
		retryableFailure := false

		if callErr != nil {
			errorMessage = fmt.Sprintf("call_error: %v", callErr)
		} else {
			var payload speakerLLMOutput
			if err := json.Unmarshal([]byte(callResult.ExtractedContentJSON), &payload); err != nil {
				errorMessage = fmt.Sprintf("parse_error: %v", err)
				retryableFailure = true
			} else {
				parseOK = true
				candidate = SpeakerCaseResult{
					PredictedSpeaker:              canonicalSpeakerLabel(payload.Speaker.PredictedSpeaker),
					PredictedSpeakerConfidence:    clamp01(payload.Speaker.Confidence),
					FarewellIsCurrentUtterance:    payload.Farewell.IsCurrentFarewell,
					FarewellIsConversationClosing: payload.Farewell.IsClosingContext,
					FarewellContextSource:         normalizeFarewellContextSource(payload.Farewell.ContextSource),
					SpeakerEvidenceQuote:          strings.TrimSpace(payload.Speaker.EvidenceQuote),
					SpeakerEvidenceIsValid:        false,
				}

				validationErrors := validateSpeakerOutput(in.CurrentText, candidate)
				if len(validationErrors) == 0 {
					candidate.SpeakerEvidenceIsValid = true
					validationOK = true
				} else {
					errorMessage = "validation_error: " + strings.Join(validationErrors, "; ")
					retryableFailure = true
				}
			}
		}

		event := LLMEvent{
			ConversationID:       c.conversationID,
			UtteranceIndex:       c.utteranceIndex,
			UnitName:             llmUnitSpeaker,
			Attempt:              attempt,
			Model:                c.model,
			RequestJSON:          callResult.RequestJSON,
			ResponseHTTPStatus:   callResult.HTTPStatus,
			ResponseJSON:         callResult.ResponseJSON,
			ExtractedContentJSON: callResult.ExtractedContentJSON,
			ParseOK:              parseOK,
			ValidationOK:         validationOK,
			ErrorMessage:         errorMessage,
		}
		if err := c.store.InsertLLMEvent(event); err != nil {
			return SpeakerCaseResult{}, fmt.Errorf("write speaker llm event: %w", err)
		}

		if parseOK && validationOK {
			return candidate, nil
		}
		if !retryableFailure {
			break
		}
	}

	return fallbackSpeakerCaseResult(), nil
}

func validateSpeakerOutput(currentText string, out SpeakerCaseResult) []string {
	errs := make([]string, 0, 4)
	if out.PredictedSpeaker != speakerSalesRep && out.PredictedSpeaker != speakerCustomer {
		errs = append(errs, "predicted_speaker must be Sales Rep or Customer")
	}
	if out.FarewellIsConversationClosing && out.FarewellContextSource == farewellContextSourceNone {
		errs = append(errs, "context_source cannot be none when is_closing_context is true")
	}
	if !out.FarewellIsConversationClosing {
		out.FarewellContextSource = farewellContextSourceNone
	}
	if strings.TrimSpace(out.SpeakerEvidenceQuote) == "" {
		errs = append(errs, "evidence_quote is empty")
	} else if !strings.Contains(currentText, out.SpeakerEvidenceQuote) {
		errs = append(errs, "evidence_quote is not substring of current_text")
	}
	return errs
}

func fallbackSpeakerCaseResult() SpeakerCaseResult {
	return SpeakerCaseResult{
		PredictedSpeaker:              speakerCustomer,
		PredictedSpeakerConfidence:    0,
		FarewellIsCurrentUtterance:    false,
		FarewellIsConversationClosing: false,
		FarewellContextSource:         farewellContextSourceNone,
		SpeakerEvidenceQuote:          "",
		SpeakerEvidenceIsValid:        false,
	}
}

func speakerMessages(previousText, currentText, nextText string) []openAIMessage {
	system := "Return JSON only. Follow schema strictly. Step 1: analyze farewell context from previous/current/next text. Step 2: predict speaker for current text. Do not use any metadata except the three text fields."
	user := fmt.Sprintf(
		"previous_text: %q\ncurrent_text: %q\nnext_text: %q\nTask:\n1) fill farewell fields\n2) predict speaker for current_text\n3) evidence_quote must be exact substring of current_text",
		previousText,
		currentText,
		nextText,
	)
	return []openAIMessage{
		{Role: "system", Content: system},
		{Role: "user", Content: user},
	}
}

func speakerSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"farewell", "speaker"},
		"properties": map[string]any{
			"farewell": map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"required":             []string{"is_current_farewell", "is_closing_context", "context_source"},
				"properties": map[string]any{
					"is_current_farewell": map[string]any{"type": "boolean"},
					"is_closing_context":  map[string]any{"type": "boolean"},
					"context_source": map[string]any{
						"enum": []string{
							farewellContextSourceCurrent,
							farewellContextSourcePrevious,
							farewellContextSourceNext,
							farewellContextSourceMixed,
							farewellContextSourceNone,
						},
					},
				},
			},
			"speaker": map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"required":             []string{"predicted_speaker", "confidence", "evidence_quote"},
				"properties": map[string]any{
					"predicted_speaker": map[string]any{"enum": []string{speakerSalesRep, speakerCustomer}},
					"confidence":        map[string]any{"type": "number"},
					"evidence_quote":    map[string]any{"type": "string"},
				},
			},
		},
	}
}
