package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type openAIClient struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func newOpenAIClient(apiKey, baseURL string) *openAIClient {
	cleanBaseURL := strings.TrimSpace(baseURL)
	if cleanBaseURL == "" {
		cleanBaseURL = defaultOpenAIBaseURL
	}
	return &openAIClient{
		apiKey:  strings.TrimSpace(apiKey),
		baseURL: strings.TrimRight(cleanBaseURL, "/"),
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

func (c *openAIClient) CallStrictJSONSchema(
	ctx context.Context,
	model string,
	messages []openAIMessage,
	schemaName string,
	schema map[string]any,
) (LLMCallResult, error) {
	result := LLMCallResult{
		RequestJSON:          "{}",
		ResponseJSON:         "{}",
		HTTPStatus:           0,
		ExtractedContentJSON: "",
	}

	requestBody := map[string]any{
		"model":       model,
		"messages":    messages,
		"temperature": 0,
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   schemaName,
				"strict": true,
				"schema": schema,
			},
		},
	}
	payload, err := json.Marshal(requestBody)
	if err != nil {
		return result, fmt.Errorf("marshal request: %w", err)
	}
	result.RequestJSON = string(payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return result, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return result, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return result, fmt.Errorf("read response: %w", err)
	}
	result.ResponseJSON = string(body)
	result.HTTPStatus = resp.StatusCode

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return result, fmt.Errorf("openai status=%d schema=%s", resp.StatusCode, schemaName)
	}

	var parsed struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
				Refusal string `json:"refusal"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return result, fmt.Errorf("parse response: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return result, fmt.Errorf("empty choices")
	}

	message := parsed.Choices[0].Message
	if strings.TrimSpace(message.Refusal) != "" {
		return result, fmt.Errorf("model refusal schema=%s reason=%s", schemaName, strings.TrimSpace(message.Refusal))
	}
	content := strings.TrimSpace(message.Content)
	if content == "" {
		return result, fmt.Errorf("empty content schema=%s", schemaName)
	}
	result.ExtractedContentJSON = content
	return result, nil
}
