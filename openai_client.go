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

type LLMMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type LLMClient interface {
	GenerateStructured(ctx context.Context, model string, messages []LLMMessage, schemaName string, schema map[string]any) (content string, requestID string, err error)
}

type OpenAIClient struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

func NewOpenAIClient(apiKey, baseURL string) *OpenAIClient {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultBaseURL
	}
	return &OpenAIClient{
		apiKey:  apiKey,
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

func (c *OpenAIClient) GenerateStructured(ctx context.Context, model string, messages []LLMMessage, schemaName string, schema map[string]any) (string, string, error) {
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
		return "", "", fmt.Errorf("marshal request: %w", err)
	}

	url := c.baseURL + "/v1/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return "", "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", resp.Header.Get("x-request-id"), fmt.Errorf("read response: %w", err)
	}

	requestID := resp.Header.Get("x-request-id")
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", requestID, fmt.Errorf("openai status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
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
		return "", requestID, fmt.Errorf("parse response: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return "", requestID, fmt.Errorf("empty choices")
	}

	choice := parsed.Choices[0].Message
	if strings.TrimSpace(choice.Refusal) != "" {
		return "", requestID, fmt.Errorf("model refusal: %s", choice.Refusal)
	}
	if strings.TrimSpace(choice.Content) == "" {
		return "", requestID, fmt.Errorf("empty content")
	}

	return choice.Content, requestID, nil
}
