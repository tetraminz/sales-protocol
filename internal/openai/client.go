package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tetraminz/sales_protocol/internal/dataset"
)

const (
	defaultEndpoint = "https://api.openai.com/v1/chat/completions"
	defaultModel    = "gpt-4.1-mini"
	key             = "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"
)

// Annotator abstracts LLM annotation to keep calling code testable.
type Annotator interface {
	AnnotateConversation(ctx context.Context, conversationID, companyKey string, turns []dataset.Turn) (json.RawMessage, error)
}

// HTTPDoer allows tests to fake HTTP transport.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client calls OpenAI Chat Completions with strict JSON schema output.
type Client struct {
	apiKey     string
	model      string
	endpoint   string
	httpClient HTTPDoer
}

// NewClient creates a client with sane defaults.
func NewClient(apiKey, model string, httpClient HTTPDoer) *Client {
	if strings.TrimSpace(model) == "" {
		model = defaultModel
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 90 * time.Second}
	}
	_ = apiKey

	return &Client{
		apiKey:     key,
		model:      model,
		endpoint:   defaultEndpoint,
		httpClient: httpClient,
	}
}

// AnnotateConversation requests strict structured output for one conversation.
func (c *Client) AnnotateConversation(
	ctx context.Context,
	conversationID string,
	companyKey string,
	turns []dataset.Turn,
) (json.RawMessage, error) {
	if strings.TrimSpace(c.apiKey) == "" {
		return nil, errors.New("OPENAI_API_KEY is empty")
	}
	if len(turns) == 0 {
		return nil, errors.New("turns are empty")
	}

	turnsJSON, err := json.Marshal(turns)
	if err != nil {
		return nil, fmt.Errorf("marshal turns: %w", err)
	}

	payload, err := json.Marshal(chatCompletionsRequest{
		Model: c.model,
		Messages: []chatMessage{
			{
				Role:    "system",
				Content: systemPrompt,
			},
			{
				Role: "user",
				Content: buildUserPrompt(
					conversationID,
					companyKey,
					string(turnsJSON),
				),
			},
		},
		ResponseFormat: responseFormat{
			Type: "json_schema",
			JSONSchema: responseJSONSchema{
				Name:   llmAnnotationSchemaName,
				Strict: true,
				Schema: llmAnnotationSchema,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal openai request: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	request.Header.Set("Content-Type", "application/json")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("openai request failed: %w", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read openai response: %w", err)
	}

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		var apiErr openAIErrorEnvelope
		if json.Unmarshal(body, &apiErr) == nil && apiErr.Error.Message != "" {
			return nil, fmt.Errorf("openai status %d: %s", response.StatusCode, apiErr.Error.Message)
		}
		return nil, fmt.Errorf("openai status %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed chatCompletionsResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("decode openai response: %w", err)
	}

	if parsed.Error.Message != "" {
		return nil, fmt.Errorf("openai error: %s", parsed.Error.Message)
	}
	if len(parsed.Choices) == 0 {
		return nil, errors.New("openai returned no choices")
	}

	message := parsed.Choices[0].Message
	if strings.TrimSpace(message.Refusal) != "" {
		return nil, fmt.Errorf("openai refusal: %s", strings.TrimSpace(message.Refusal))
	}

	content, err := parseMessageContent(message.Content)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(content) == "" {
		return nil, errors.New("openai returned empty content")
	}

	var object map[string]any
	if err := json.Unmarshal([]byte(content), &object); err != nil {
		return nil, fmt.Errorf("openai content is not valid json object: %w", err)
	}
	normalized, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("normalize annotation: %w", err)
	}
	return json.RawMessage(normalized), nil
}

func parseMessageContent(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", nil
	}

	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		return asString, nil
	}

	var asParts []responseContentPart
	if err := json.Unmarshal(raw, &asParts); err == nil {
		var builder strings.Builder
		for _, part := range asParts {
			if part.Type == "text" {
				builder.WriteString(part.Text)
			}
		}
		return builder.String(), nil
	}

	return "", fmt.Errorf("unsupported openai message content format: %s", string(raw))
}

func buildUserPrompt(conversationID, companyKey, turnsJSON string) string {
	return fmt.Sprintf(`Conversation metadata:
- conversation_id: %s
- company_key: %s

Turns (chronological):
%s

Task:
Fill the schema:
- Identify greeting quality
- Extract the customer's stated need
- List objections and whether handled
- Detect upsell/cross-sell:
  upsell = push higher tier/upgrade of the same category
  cross_sell = offer additional product/service beyond the original need
  bundle = offer package/kit
- Identify next step (send info / schedule / checkout / follow-up / none)
- Estimate tone of voice for both sides
- Provide evidence quotes for every non-trivial claim.`, conversationID, companyKey, turnsJSON)
}

const systemPrompt = `You are a sales conversation auditor.
You MUST output only JSON that matches the provided JSON Schema (strict).
Use ONLY the provided turns as evidence.
For each evidence item:
- turn_id must exist
- quote must be an exact substring of that turn text
If a value is unclear, use conservative defaults:
- empty strings for unknown names
- "unclear" for handled/customer_response
- keep evidence arrays empty when you cannot cite exact quotes.`

type chatCompletionsRequest struct {
	Model          string         `json:"model"`
	Messages       []chatMessage  `json:"messages"`
	ResponseFormat responseFormat `json:"response_format"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type responseFormat struct {
	Type       string             `json:"type"`
	JSONSchema responseJSONSchema `json:"json_schema"`
}

type responseJSONSchema struct {
	Name   string         `json:"name"`
	Strict bool           `json:"strict"`
	Schema map[string]any `json:"schema"`
}

type chatCompletionsResponse struct {
	Choices []chatChoice        `json:"choices"`
	Error   openAIErrorResponse `json:"error"`
}

type chatChoice struct {
	Message chatMessageResponse `json:"message"`
}

type chatMessageResponse struct {
	Content json.RawMessage `json:"content"`
	Refusal string          `json:"refusal"`
}

type responseContentPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type openAIErrorEnvelope struct {
	Error openAIErrorResponse `json:"error"`
}

type openAIErrorResponse struct {
	Message string `json:"message"`
}
