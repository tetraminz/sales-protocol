package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type empathyLLMOutput struct {
	EmpathyPresent bool    `json:"empathy_present"`
	Confidence     float64 `json:"confidence"`
	EvidenceQuote  string  `json:"evidence_quote"`
}

type openAIEmpathyCase struct {
	client         *openAIClient
	model          string
	store          *SQLiteStore
	conversationID string
	utteranceIndex int
}

func newOpenAIEmpathyCase(client *openAIClient, model string, store *SQLiteStore) *openAIEmpathyCase {
	return &openAIEmpathyCase{
		client: client,
		model:  strings.TrimSpace(model),
		store:  store,
	}
}

func (c *openAIEmpathyCase) setLogContext(conversationID string, utteranceIndex int) {
	c.conversationID = strings.TrimSpace(conversationID)
	c.utteranceIndex = utteranceIndex
}

func (c *openAIEmpathyCase) Evaluate(ctx context.Context, in EmpathyCaseInput) (EmpathyCaseResult, error) {
	if c.client == nil {
		return fallbackEmpathyCaseResult(), nil
	}
	if c.store == nil {
		return EmpathyCaseResult{}, fmt.Errorf("empathy case store is required")
	}

	messages := empathyMessages(in.CurrentText)
	schema := empathySchema()
	for attempt := 1; attempt <= maxLLMAttempts; attempt++ {
		callResult, callErr := c.client.CallStrictJSONSchema(ctx, c.model, messages, "empathy_case_v2", schema)

		candidate := fallbackEmpathyCaseResult()
		parseOK := false
		validationOK := false
		errorMessage := ""
		retryableFailure := false

		if callErr != nil {
			errorMessage = fmt.Sprintf("call_error: %v", callErr)
		} else {
			var payload empathyLLMOutput
			if err := json.Unmarshal([]byte(callResult.ExtractedContentJSON), &payload); err != nil {
				errorMessage = fmt.Sprintf("parse_error: %v", err)
				retryableFailure = true
			} else {
				parseOK = true
				candidate = EmpathyCaseResult{
					EmpathyPresent:         payload.EmpathyPresent,
					EmpathyConfidence:      clamp01(payload.Confidence),
					EmpathyEvidenceQuote:   strings.TrimSpace(payload.EvidenceQuote),
					EmpathyEvidenceIsValid: false,
				}
				validationErrors := validateEmpathyOutput(in.CurrentText, candidate)
				if len(validationErrors) == 0 {
					candidate.EmpathyEvidenceIsValid = true
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
			UnitName:             llmUnitEmpathy,
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
			return EmpathyCaseResult{}, fmt.Errorf("write empathy llm event: %w", err)
		}

		if parseOK && validationOK {
			return candidate, nil
		}
		if !retryableFailure {
			break
		}
	}

	return fallbackEmpathyCaseResult(), nil
}

func validateEmpathyOutput(currentText string, out EmpathyCaseResult) []string {
	errs := make([]string, 0, 1)
	if strings.TrimSpace(out.EmpathyEvidenceQuote) == "" {
		errs = append(errs, "evidence_quote is empty")
	} else if !strings.Contains(currentText, out.EmpathyEvidenceQuote) {
		errs = append(errs, "evidence_quote is not substring of current_text")
	}
	return errs
}

func fallbackEmpathyCaseResult() EmpathyCaseResult {
	return EmpathyCaseResult{
		EmpathyPresent:         false,
		EmpathyConfidence:      0,
		EmpathyEvidenceQuote:   "",
		EmpathyEvidenceIsValid: false,
	}
}

func empathyMessages(currentText string) []openAIMessage {
	system := "Return JSON only. Estimate empathy on the current text. Do not use any metadata beyond current_text."
	user := fmt.Sprintf(
		"current_text: %q\nTask: return empathy_present, confidence, evidence_quote where evidence_quote is exact substring of current_text.",
		currentText,
	)
	return []openAIMessage{
		{Role: "system", Content: system},
		{Role: "user", Content: user},
	}
}

func empathySchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"empathy_present", "confidence", "evidence_quote"},
		"properties": map[string]any{
			"empathy_present": map[string]any{"type": "boolean"},
			"confidence":      map[string]any{"type": "number"},
			"evidence_quote":  map[string]any{"type": "string"},
		},
	}
}
