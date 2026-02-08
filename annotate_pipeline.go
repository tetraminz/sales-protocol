package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const shortUtteranceMaxLen = 40
const maxLogRawJSONLen = 500
const maxLogMessageLen = 300

const (
	farewellContextSourceNone     = "none"
	farewellContextSourceCurrent  = "current"
	farewellContextSourcePrevious = "previous"
	farewellContextSourceNext     = "next"
	farewellContextSourceMixed    = "mixed"
)

type AnnotateConfig struct {
	DBPath   string
	InputDir string
	FromIdx  int
	ToIdx    int
	Model    string
	APIKey   string
	BaseURL  string
}

type annotateTurn struct {
	ConversationID string
	TurnID         int
	Speaker        string
	Text           string
}

type annotateReplica struct {
	ConversationID string
	ReplicaID      int
	SpeakerTrue    string
	Text           string
}

type openAIClient struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type speakerLLMOutput struct {
	PredictedSpeaker    string  `json:"predicted_speaker"`
	Confidence          float64 `json:"confidence"`
	IsFarewellUtterance bool    `json:"is_farewell_utterance"`
	IsFarewellContext   bool    `json:"is_farewell_context"`
	ContextSource       string  `json:"context_source"`
	Evidence            struct {
		Quote string `json:"quote"`
	} `json:"evidence"`
}

type empathyLLMOutput struct {
	Confidence float64 `json:"confidence"`
}

type openAISpeakerCase struct {
	client         *openAIClient
	model          string
	conversationID string
	replicaID      int
	logger         *annotateLogger
}

type openAIEmpathyCase struct {
	client         *openAIClient
	model          string
	conversationID string
	replicaID      int
	logger         *annotateLogger
}

var fallbackSpeakerResult = ReplicaCaseResult{
	PredictedSpeaker:      speakerCustomer,
	Confidence:            0,
	EvidenceQuote:         "",
	FarewellUtterance:     false,
	FarewellContext:       false,
	FarewellContextSource: farewellContextSourceNone,
	QualityMismatch:       true,
	QualityDecision:       qualityDecisionStrictMismatch,
}

var fallbackEmpathyResult = EmpathyCaseResult{
	Ran:            false,
	EmpathyPresent: false,
	EmpathyType:    "none",
	Confidence:     0,
	EvidenceQuote:  "",
}

type annotateLogEntry struct {
	CreatedAtUTC   string
	ConversationID string
	ReplicaID      int
	Stage          string
	Status         string
	Message        string
	RawJSON        string
	Model          string
}

type annotateLogger struct {
	db      *sql.DB
	entries []annotateLogEntry
}

func AnnotateToSQLite(ctx context.Context, cfg AnnotateConfig) error {
	if strings.TrimSpace(cfg.DBPath) == "" {
		cfg.DBPath = defaultSQLitePath
	}
	if strings.TrimSpace(cfg.InputDir) == "" {
		cfg.InputDir = defaultInputDir
	}
	if strings.TrimSpace(cfg.Model) == "" {
		cfg.Model = defaultAnnotateModel
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		cfg.BaseURL = defaultOpenAIBaseURL
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		return fmt.Errorf("OPENAI_API_KEY is required for annotate")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil {
		return fmt.Errorf("create db directory: %w", err)
	}

	files, err := findCSVFiles(cfg.InputDir)
	if err != nil {
		return err
	}
	selected, err := selectCSVRange(files, cfg.FromIdx, cfg.ToIdx)
	if err != nil {
		return err
	}

	db, err := openSQLite(cfg.DBPath)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := ensureAnnotationsSchema(db); err != nil {
		return err
	}
	if _, err := db.Exec(deleteAnnotateLogsSQL); err != nil {
		return fmt.Errorf("clear annotate logs: %w", err)
	}
	logger := newAnnotateLogger(db)
	defer logger.flush()

	client := &openAIClient{
		apiKey:  cfg.APIKey,
		baseURL: strings.TrimRight(cfg.BaseURL, "/"),
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
	speakerCase := &openAISpeakerCase{client: client, model: cfg.Model, logger: logger}
	empathyCase := &openAIEmpathyCase{client: client, model: cfg.Model, logger: logger}
	process := AnnotationBusinessProcess{
		SpeakerCase: speakerCase,
		EmpathyCase: empathyCase,
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(deleteAnnotationsSQL); err != nil {
		return fmt.Errorf("clear annotations: %w", err)
	}
	stmt, err := tx.Prepare(insertAnnotationSQL)
	if err != nil {
		return fmt.Errorf("prepare insert annotation: %w", err)
	}
	defer stmt.Close()

	totalReplicas := 0
	totalMatches := 0
	logger.write(
		"",
		0,
		"pipeline",
		"info",
		fmt.Sprintf("annotate_start files=%d range=%d..%d", len(selected), cfg.FromIdx, cfg.ToIdx),
		"{}",
		cfg.Model,
	)
	fmt.Printf("annotate_start files=%d db=%s model=%s range=%d..%d\n", len(selected), cfg.DBPath, cfg.Model, cfg.FromIdx, cfg.ToIdx)

	for fileIdx, path := range selected {
		turns, err := readTurns(path)
		if err != nil {
			return fmt.Errorf("read turns %s: %w", path, err)
		}
		replicas := buildReplicas(turns)
		conversationID := firstConversationID(turns, path)
		fmt.Printf("annotate_file %d/%d conversation=%s replicas=%d\n", fileIdx+1, len(selected), conversationID, len(replicas))
		fileMatches := 0

		for i, replica := range replicas {
			prevText := ""
			nextText := ""
			if i > 0 {
				prevText = replicas[i-1].Text
			}
			if i+1 < len(replicas) {
				nextText = replicas[i+1].Text
			}
			speakerCase.setLogContext(replica.ConversationID, replica.ReplicaID)
			empathyCase.setLogContext(replica.ConversationID, replica.ReplicaID)

			out, err := process.Run(ctx, ProcessInput{
				ReplicaText: replica.Text,
				PrevText:    prevText,
				NextText:    nextText,
				SpeakerTrue: replica.SpeakerTrue,
			})
			if err != nil {
				logger.write(
					replica.ConversationID,
					replica.ReplicaID,
					"pipeline",
					"error",
					fmt.Sprintf("replica_failed reason=%v", err),
					"{}",
					cfg.Model,
				)
				out = fallbackProcessOutput()
			}
			if strings.TrimSpace(out.Speaker.EvidenceQuote) == "" {
				logger.write(
					replica.ConversationID,
					replica.ReplicaID,
					"pipeline",
					"error",
					"replica_failed reason=speaker_case_degraded",
					"{}",
					cfg.Model,
				)
			}

			speakerTrue := canonicalSpeakerLabel(replica.SpeakerTrue)
			speakerPredicted := canonicalSpeakerLabel(out.Speaker.PredictedSpeaker)
			rawSpeakerMatch := speakerTrue == speakerPredicted
			if strings.TrimSpace(out.Speaker.QualityDecision) == "" {
				// Защита на деградацию: если бизнес-решение не было заполнено,
				// восстанавливаем строгую ветку по истинной/предсказанной метке.
				out.Speaker.QualityMismatch, out.Speaker.QualityDecision = decideSpeakerQuality(
					speakerTrue,
					speakerPredicted,
					false,
				)
			}
			out.Speaker.FarewellContextSource = normalizeFarewellContextSource(out.Speaker.FarewellContextSource)
			speakerMatch := boolToInt(!out.Speaker.QualityMismatch)
			speakerConfidence := clamp01(out.Speaker.Confidence)
			empathyConfidence := 0.0
			if speakerTrue == speakerSalesRep {
				empathyConfidence = clamp01(out.Empathy.Confidence)
			}
			sgrPayload := map[string]any{
				"conversation_id":      replica.ConversationID,
				"replica_id":           replica.ReplicaID,
				"quality_decision":     out.Speaker.QualityDecision,
				"farewell_context":     out.Speaker.FarewellContext,
				"farewell_utterance":   out.Speaker.FarewellUtterance,
				"context_source":       out.Speaker.FarewellContextSource,
				"raw_match":            boolToInt(rawSpeakerMatch),
				"final_match":          speakerMatch,
				"speaker_true":         speakerTrue,
				"speaker_predicted":    speakerPredicted,
				"speaker_quality_miss": boolToInt(out.Speaker.QualityMismatch),
			}
			sgrRawJSON := "{}"
			if payload, err := json.Marshal(sgrPayload); err == nil {
				sgrRawJSON = string(payload)
			}
			logger.write(
				replica.ConversationID,
				replica.ReplicaID,
				"sgr",
				"info",
				fmt.Sprintf(
					"decision=%s raw_match=%d final_match=%d",
					out.Speaker.QualityDecision,
					boolToInt(rawSpeakerMatch),
					speakerMatch,
				),
				sgrRawJSON,
				cfg.Model,
			)

			if _, err := stmt.Exec(
				replica.ConversationID,
				replica.ReplicaID,
				speakerTrue,
				speakerPredicted,
				speakerConfidence,
				speakerMatch,
				empathyConfidence,
				reviewStatusPending,
				"",
				replica.Text,
				cfg.Model,
				time.Now().UTC().Format(time.RFC3339),
			); err != nil {
				logger.write(
					replica.ConversationID,
					replica.ReplicaID,
					"pipeline",
					"error",
					fmt.Sprintf("insert_failed reason=%v", err),
					"{}",
					cfg.Model,
				)
				return fmt.Errorf("insert annotation: %w", err)
			}

			totalReplicas++
			if speakerMatch == 1 {
				totalMatches++
				fileMatches++
			}

			fmt.Printf("annotate_row conversation=%s replica=%d speaker_true=%s speaker_predicted=%s speaker_match=%d empathy_confidence=%.2f\n",
				replica.ConversationID,
				replica.ReplicaID,
				speakerTrue,
				speakerPredicted,
				speakerMatch,
				empathyConfidence,
			)
			logger.write(
				replica.ConversationID,
				replica.ReplicaID,
				"pipeline",
				"info",
				fmt.Sprintf("replica_ok speaker_match=%d empathy_confidence=%.2f", speakerMatch, empathyConfidence),
				"{}",
				cfg.Model,
			)
			if totalReplicas%25 == 0 {
				fmt.Printf("annotate_progress replicas=%d speaker_match=%d speaker_mismatch=%d\n", totalReplicas, totalMatches, totalReplicas-totalMatches)
			}
		}

		fmt.Printf(
			"annotate_file_done conversation=%s replicas=%d speaker_match=%d speaker_mismatch=%d\n",
			conversationID,
			len(replicas),
			fileMatches,
			len(replicas)-fileMatches,
		)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit annotations: %w", err)
	}
	logger.write(
		"",
		0,
		"pipeline",
		"info",
		fmt.Sprintf("annotate_done replicas=%d speaker_match=%d speaker_mismatch=%d", totalReplicas, totalMatches, totalReplicas-totalMatches),
		"{}",
		cfg.Model,
	)
	fmt.Printf("annotate_done replicas=%d speaker_match=%d speaker_mismatch=%d\n", totalReplicas, totalMatches, totalReplicas-totalMatches)
	return nil
}

func findCSVFiles(inputDir string) ([]string, error) {
	files, err := filepath.Glob(filepath.Join(inputDir, "*.csv"))
	if err != nil {
		return nil, fmt.Errorf("glob csv files: %w", err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no csv files found in %s", inputDir)
	}
	return files, nil
}

func selectCSVRange(files []string, fromIdx, toIdx int) ([]string, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("no files available")
	}
	if fromIdx < 1 {
		return nil, fmt.Errorf("from_idx must be >= 1")
	}
	if toIdx < fromIdx {
		return nil, fmt.Errorf("to_idx must be >= from_idx")
	}
	if toIdx > len(files) {
		return nil, fmt.Errorf("to_idx (%d) is out of range, max=%d", toIdx, len(files))
	}
	return files[fromIdx-1 : toIdx], nil
}

func readTurns(csvPath string) ([]annotateTurn, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1

	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read csv header: %w", err)
	}
	index := indexColumns(headers)
	required := []string{"Conversation", "Chunk_id", "Speaker", "Text"}
	for _, col := range required {
		if _, ok := index[col]; !ok {
			return nil, fmt.Errorf("missing required column %q", col)
		}
	}

	fallbackConvID := strings.TrimSuffix(filepath.Base(csvPath), filepath.Ext(csvPath))
	turns := []annotateTurn{}
	for {
		rec, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read csv row: %w", err)
		}
		if len(rec) == 0 {
			continue
		}

		turnID, err := strconv.Atoi(strings.TrimSpace(getField(rec, index["Chunk_id"])))
		if err != nil {
			continue
		}
		convID := strings.TrimSpace(getField(rec, index["Conversation"]))
		if convID == "" {
			convID = fallbackConvID
		}
		turns = append(turns, annotateTurn{
			ConversationID: convID,
			TurnID:         turnID,
			Speaker:        normalizeSpeaker(getField(rec, index["Speaker"])),
			Text:           strings.TrimSpace(getField(rec, index["Text"])),
		})
	}

	sort.SliceStable(turns, func(i, j int) bool {
		return turns[i].TurnID < turns[j].TurnID
	})
	return turns, nil
}

func buildReplicas(turns []annotateTurn) []annotateReplica {
	if len(turns) == 0 {
		return nil
	}
	replicas := make([]annotateReplica, 0, len(turns))
	for _, turn := range turns {
		if len(replicas) == 0 || replicas[len(replicas)-1].SpeakerTrue != turn.Speaker {
			replicas = append(replicas, annotateReplica{
				ConversationID: turn.ConversationID,
				ReplicaID:      len(replicas) + 1,
				SpeakerTrue:    turn.Speaker,
				Text:           turn.Text,
			})
			continue
		}
		if strings.TrimSpace(turn.Text) == "" {
			continue
		}
		last := &replicas[len(replicas)-1]
		if last.Text == "" {
			last.Text = turn.Text
		} else {
			last.Text += "\n" + turn.Text
		}
	}
	return replicas
}

func firstConversationID(turns []annotateTurn, fallbackPath string) string {
	if len(turns) > 0 && strings.TrimSpace(turns[0].ConversationID) != "" {
		return turns[0].ConversationID
	}
	return strings.TrimSuffix(filepath.Base(fallbackPath), filepath.Ext(fallbackPath))
}

func indexColumns(headers []string) map[string]int {
	out := make(map[string]int, len(headers))
	for i, header := range headers {
		name := strings.TrimSpace(header)
		if i == 0 {
			name = strings.TrimPrefix(name, "\uFEFF")
		}
		out[name] = i
	}
	return out
}

func getField(rec []string, idx int) string {
	if idx < 0 || idx >= len(rec) {
		return ""
	}
	return rec[idx]
}

func normalizeSpeaker(raw string) string {
	return canonicalSpeakerLabel(raw)
}

func canonicalSpeakerLabel(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.Trim(s, "*")
	s = strings.TrimSpace(s)
	if strings.EqualFold(s, speakerSalesRep) {
		return speakerSalesRep
	}
	if strings.EqualFold(s, speakerCustomer) {
		return speakerCustomer
	}
	return s
}

func normalizeFarewellContextSource(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case farewellContextSourceCurrent:
		return farewellContextSourceCurrent
	case farewellContextSourcePrevious:
		return farewellContextSourcePrevious
	case farewellContextSourceNext:
		return farewellContextSourceNext
	case farewellContextSourceMixed:
		return farewellContextSourceMixed
	default:
		return farewellContextSourceNone
	}
}

func (c *openAISpeakerCase) setLogContext(conversationID string, replicaID int) {
	c.conversationID = conversationID
	c.replicaID = replicaID
}

func (c *openAIEmpathyCase) setLogContext(conversationID string, replicaID int) {
	c.conversationID = conversationID
	c.replicaID = replicaID
}

func (c *openAISpeakerCase) log(status, message, rawJSON string) {
	if c.logger == nil {
		return
	}
	c.logger.write(c.conversationID, c.replicaID, "speaker", status, message, rawJSON, c.model)
}

func (c *openAIEmpathyCase) log(status, message, rawJSON string) {
	if c.logger == nil {
		return
	}
	c.logger.write(c.conversationID, c.replicaID, "empathy", status, message, rawJSON, c.model)
}

func (c *openAISpeakerCase) Evaluate(ctx context.Context, in ReplicaCaseInput) (ReplicaCaseResult, error) {
	content, err := c.client.generateStructured(
		ctx,
		c.model,
		speakerMessages(in.PrevText, in.ReplicaText, in.NextText),
		"speaker_case_v1",
		speakerSchema(),
	)
	if err != nil {
		c.log("error", fmt.Sprintf("request_failed reason=%v", err), "{}")
		return fallbackSpeakerResult, nil
	}
	c.log("info", "response_ok", content)

	var out speakerLLMOutput
	if err := json.Unmarshal([]byte(content), &out); err != nil {
		c.log("error", fmt.Sprintf("parse_error reason=%v", err), content)
		return fallbackSpeakerResult, nil
	}
	pred := canonicalSpeakerLabel(out.PredictedSpeaker)
	if pred != speakerSalesRep && pred != speakerCustomer {
		c.log("error", fmt.Sprintf("invalid_speaker value=%q", pred), content)
		return fallbackSpeakerResult, nil
	}
	contextSource := normalizeFarewellContextSource(out.ContextSource)
	farewellContext := out.IsFarewellContext
	if !farewellContext {
		contextSource = farewellContextSourceNone
	}
	if farewellContext && contextSource == farewellContextSourceNone {
		c.log("error", fmt.Sprintf("invalid_context_source value=%q", out.ContextSource), content)
		farewellContext = false
	}
	quote := strings.TrimSpace(out.Evidence.Quote)
	if quote == "" {
		c.log("error", "quote_empty", content)
		return ReplicaCaseResult{
			PredictedSpeaker:      pred,
			Confidence:            clamp01(out.Confidence),
			EvidenceQuote:         "",
			FarewellUtterance:     out.IsFarewellUtterance,
			FarewellContext:       farewellContext,
			FarewellContextSource: contextSource,
		}, nil
	}
	if !strings.Contains(in.ReplicaText, quote) {
		c.log(
			"error",
			fmt.Sprintf(
				"quote_not_substring quote=%q replica_excerpt=%q",
				shortLogText(quote, 80),
				shortLogText(strings.TrimSpace(in.ReplicaText), 80),
			),
			content,
		)
		return ReplicaCaseResult{
			PredictedSpeaker:      pred,
			Confidence:            clamp01(out.Confidence),
			EvidenceQuote:         "",
			FarewellUtterance:     out.IsFarewellUtterance,
			FarewellContext:       farewellContext,
			FarewellContextSource: contextSource,
		}, nil
	}

	return ReplicaCaseResult{
		PredictedSpeaker:      pred,
		Confidence:            clamp01(out.Confidence),
		EvidenceQuote:         quote,
		FarewellUtterance:     out.IsFarewellUtterance,
		FarewellContext:       farewellContext,
		FarewellContextSource: contextSource,
	}, nil
}

func (c *openAIEmpathyCase) Evaluate(ctx context.Context, in EmpathyCaseInput) (EmpathyCaseResult, error) {
	content, err := c.client.generateStructured(
		ctx,
		c.model,
		empathyMessages(in.ReplicaText),
		"empathy_confidence_v1",
		empathySchema(),
	)
	if err != nil {
		c.log("error", fmt.Sprintf("request_failed reason=%v", err), "{}")
		return fallbackEmpathyResult, nil
	}
	c.log("info", "response_ok", content)

	var out empathyLLMOutput
	if err := json.Unmarshal([]byte(content), &out); err != nil {
		c.log("error", fmt.Sprintf("parse_error reason=%v", err), content)
		return fallbackEmpathyResult, nil
	}

	return EmpathyCaseResult{
		Ran:            true,
		EmpathyPresent: false,
		EmpathyType:    "none",
		Confidence:     clamp01(out.Confidence),
		EvidenceQuote:  "",
	}, nil
}

func fallbackProcessOutput() ProcessOutput {
	return ProcessOutput{
		Speaker: fallbackSpeakerResult,
		Empathy: fallbackEmpathyResult,
	}
}

func speakerMessages(prevText, replicaText, nextText string) []openAIMessage {
	system := "Return JSON only. Follow schema strictly. Step 1: detect farewell signals from previous/current/next. Step 2: classify who wrote current. context_source must be one of current, previous, next, mixed, none. evidence.quote must be exact substring of current."
	user := fmt.Sprintf(
		"previous: %q\ncurrent: %q\nnext: %q\nTask:\n1) fill is_farewell_utterance, is_farewell_context, context_source.\n2) predict speaker for current as Sales Rep or Customer.\nRules for farewell_context: true if current/neighbor turns are part of a closing exchange (bye, goodbye, thanks+good day, final sign-off).",
		prevText,
		replicaText,
		nextText,
	)
	return []openAIMessage{
		{Role: "system", Content: system},
		{Role: "user", Content: user},
	}
}

func empathyMessages(replicaText string) []openAIMessage {
	system := "Return JSON only. Estimate empathy confidence for a sales-rep utterance from 0 to 1."
	user := fmt.Sprintf("current: %q\nTask: return empathy confidence only.", replicaText)
	return []openAIMessage{
		{Role: "system", Content: system},
		{Role: "user", Content: user},
	}
}

func speakerSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required": []string{
			"predicted_speaker",
			"confidence",
			"is_farewell_utterance",
			"is_farewell_context",
			"context_source",
			"evidence",
		},
		"properties": map[string]any{
			"predicted_speaker":     map[string]any{"enum": []string{speakerSalesRep, speakerCustomer}},
			"confidence":            map[string]any{"type": "number"},
			"is_farewell_utterance": map[string]any{"type": "boolean"},
			"is_farewell_context":   map[string]any{"type": "boolean"},
			"context_source": map[string]any{
				"enum": []string{
					farewellContextSourceCurrent,
					farewellContextSourcePrevious,
					farewellContextSourceNext,
					farewellContextSourceMixed,
					farewellContextSourceNone,
				},
			},
			"evidence": map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"required":             []string{"quote"},
				"properties": map[string]any{
					"quote": map[string]any{"type": "string"},
				},
			},
		},
	}
}

func empathySchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"confidence"},
		"properties": map[string]any{
			"confidence": map[string]any{"type": "number"},
		},
	}
}

func (c *openAIClient) generateStructured(
	ctx context.Context,
	model string,
	messages []openAIMessage,
	schemaName string,
	schema map[string]any,
) (string, error) {
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
		return "", fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf(
			"openai status=%d schema=%s response=%s",
			resp.StatusCode,
			schemaName,
			shortLogText(strings.TrimSpace(string(body)), maxLogRawJSONLen),
		)
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
		return "", fmt.Errorf("parse response: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return "", fmt.Errorf("empty choices")
	}

	msg := parsed.Choices[0].Message
	if strings.TrimSpace(msg.Refusal) != "" {
		return "", fmt.Errorf("model refusal schema=%s reason=%s", schemaName, msg.Refusal)
	}
	if strings.TrimSpace(msg.Content) == "" {
		return "", fmt.Errorf("empty content schema=%s", schemaName)
	}
	return msg.Content, nil
}

func newAnnotateLogger(db *sql.DB) *annotateLogger {
	return &annotateLogger{
		db:      db,
		entries: make([]annotateLogEntry, 0, 256),
	}
}

func (l *annotateLogger) write(
	conversationID string,
	replicaID int,
	stage string,
	status string,
	message string,
	rawJSON string,
	model string,
) {
	entry := annotateLogEntry{
		CreatedAtUTC:   time.Now().UTC().Format(time.RFC3339),
		ConversationID: strings.TrimSpace(conversationID),
		ReplicaID:      replicaID,
		Stage:          strings.TrimSpace(stage),
		Status:         strings.TrimSpace(status),
		Message:        shortLogText(message, maxLogMessageLen),
		RawJSON:        shortLogText(normalizeLogRawJSON(rawJSON), maxLogRawJSONLen),
		Model:          strings.TrimSpace(model),
	}
	if entry.RawJSON == "" {
		entry.RawJSON = "{}"
	}
	if entry.Status == "" {
		entry.Status = "info"
	}

	l.entries = append(l.entries, entry)
	fmt.Printf(
		"annotate_log stage=%s status=%s conversation=%s replica=%d message=%s\n",
		entry.Stage,
		entry.Status,
		entry.ConversationID,
		entry.ReplicaID,
		entry.Message,
	)
}

func (l *annotateLogger) flush() {
	if l == nil || l.db == nil || len(l.entries) == 0 {
		return
	}
	for _, entry := range l.entries {
		if _, err := l.db.Exec(
			insertAnnotateLogSQL,
			entry.CreatedAtUTC,
			entry.ConversationID,
			entry.ReplicaID,
			entry.Stage,
			entry.Status,
			entry.Message,
			entry.RawJSON,
			entry.Model,
		); err != nil {
			fmt.Printf("log_write_error stage=%s conversation=%s replica=%d err=%v\n", entry.Stage, entry.ConversationID, entry.ReplicaID, err)
		}
	}
	l.entries = nil
}

func normalizeLogRawJSON(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "{}"
	}
	return strings.Join(strings.Fields(raw), " ")
}

func shortLogText(text string, maxLen int) string {
	clean := strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
	if maxLen <= 0 || len(clean) <= maxLen {
		return clean
	}
	if maxLen <= 3 {
		return clean[:maxLen]
	}
	return clean[:maxLen-3] + "..."
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
