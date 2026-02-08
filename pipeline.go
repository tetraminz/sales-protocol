package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var speakerCleanupRegex = regexp.MustCompile(`[^a-z]+`)

func run(ctx context.Context, cfg Config) error {
	files, err := findCSVFiles(cfg.InputDir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no CSV files found in %s", cfg.InputDir)
	}

	limit := len(files)
	if cfg.LimitConversations > 0 && cfg.LimitConversations < limit {
		limit = cfg.LimitConversations
	}
	files = files[:limit]

	if err := ensureParentDir(cfg.OutJSONL); err != nil {
		return err
	}
	outFile, err := os.Create(cfg.OutJSONL)
	if err != nil {
		return fmt.Errorf("create out_jsonl: %w", err)
	}
	defer outFile.Close()

	encoder := json.NewEncoder(outFile)
	totalReplicas := 0

	var llmClient LLMClient
	if !cfg.DryRun {
		llmClient = NewOpenAIClient(cfg.APIKey, cfg.BaseURL)
	}

	for _, path := range files {
		turns, err := readTurns(path)
		if err != nil {
			return fmt.Errorf("read turns %s: %w", path, err)
		}
		replicas := buildReplicas(turns)
		log.Printf("processing %s: turns=%d replicas=%d", filepath.Base(path), len(turns), len(replicas))

		for i, replica := range replicas {
			prevText := ""
			nextText := ""
			if i > 0 {
				prevText = replicas[i-1].Text
			}
			if i+1 < len(replicas) {
				nextText = replicas[i+1].Text
			}

			speakerRes, speakerReqIDs := runSpeakerUnit(ctx, llmClient, cfg, replica, prevText, nextText)
			empathyRes, empathyReqIDs := runEmpathyUnit(ctx, llmClient, cfg, replica)

			record := AnnotationRecord{
				SchemaVersion:  schemaVersion,
				Dataset:        datasetName,
				ConversationID: replica.ConversationID,
				ReplicaID:      replica.ReplicaID,
				TurnIDs:        append([]int(nil), replica.TurnIDs...),
				SpeakerTrue:    replica.SpeakerTrue,
				ReplicaText:    replica.Text,
				ReplicaTurns:   convertReplicaTurns(replica.Turns),
				Guided: GuidedBlock{
					UnitSpeaker: speakerRes,
					UnitEmpathy: empathyRes,
				},
				Meta: MetaBlock{
					Model:            cfg.Model,
					TimestampUTC:     time.Now().UTC().Format(time.RFC3339),
					OpenAIRequestIDs: append(speakerReqIDs, empathyReqIDs...),
				},
			}

			if err := encoder.Encode(record); err != nil {
				return fmt.Errorf("encode jsonl record: %w", err)
			}
			totalReplicas++
		}
	}

	log.Printf("completed: conversations=%d replicas=%d", len(files), totalReplicas)
	return nil
}

func findCSVFiles(inputDir string) ([]string, error) {
	glob := filepath.Join(inputDir, "*.csv")
	files, err := filepath.Glob(glob)
	if err != nil {
		return nil, fmt.Errorf("glob input_dir: %w", err)
	}
	sort.Strings(files)
	return files, nil
}

func ensureParentDir(path string) error {
	parent := filepath.Dir(path)
	if parent == "." || parent == "" {
		return nil
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("mkdir for out_jsonl: %w", err)
	}
	return nil
}

func readTurns(csvPath string) ([]Turn, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1

	headers, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	idx := indexColumns(headers)

	required := []string{"Conversation", "Chunk_id", "Speaker", "Text"}
	for _, col := range required {
		if _, ok := idx[col]; !ok {
			return nil, fmt.Errorf("missing required column %q", col)
		}
	}

	fallbackConvID := strings.TrimSuffix(filepath.Base(csvPath), filepath.Ext(csvPath))
	var turns []Turn

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read row: %w", err)
		}
		if len(rec) == 0 {
			continue
		}

		chunkRaw := getField(rec, idx["Chunk_id"])
		chunkID, err := strconv.Atoi(strings.TrimSpace(chunkRaw))
		if err != nil {
			continue
		}

		convID := strings.TrimSpace(getField(rec, idx["Conversation"]))
		if convID == "" {
			convID = fallbackConvID
		}

		speaker := normalizeSpeaker(getField(rec, idx["Speaker"]))
		text := strings.TrimSpace(getField(rec, idx["Text"]))

		turns = append(turns, Turn{
			ConversationID: convID,
			TurnID:         chunkID,
			Speaker:        speaker,
			Text:           text,
		})
	}

	sort.SliceStable(turns, func(i, j int) bool {
		return turns[i].TurnID < turns[j].TurnID
	})
	return turns, nil
}

func indexColumns(headers []string) map[string]int {
	out := make(map[string]int, len(headers))
	for i, h := range headers {
		name := strings.TrimSpace(h)
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
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.TrimLeft(trimmed, "*_`#-: ")
	lower := strings.ToLower(trimmed)
	compact := strings.TrimSpace(speakerCleanupRegex.ReplaceAllString(lower, " "))

	switch {
	case strings.Contains(compact, "sales rep"):
		return speakerSalesRep
	case strings.Contains(compact, "sales representative"):
		return speakerSalesRep
	case strings.Contains(compact, "sales") && strings.Contains(compact, "rep"):
		return speakerSalesRep
	case strings.Contains(compact, "customer"):
		return speakerCustomer
	case strings.Contains(compact, "client"):
		return speakerCustomer
	case strings.Contains(compact, "buyer"):
		return speakerCustomer
	default:
		if strings.EqualFold(trimmed, speakerSalesRep) {
			return speakerSalesRep
		}
		if strings.EqualFold(trimmed, speakerCustomer) {
			return speakerCustomer
		}
		return trimmed
	}
}

func buildReplicas(turns []Turn) []Replica {
	if len(turns) == 0 {
		return nil
	}

	replicas := make([]Replica, 0, len(turns))
	var current Replica

	flush := func() {
		if len(current.Turns) == 0 {
			return
		}
		replicas = append(replicas, current)
	}

	for _, turn := range turns {
		if len(current.Turns) == 0 {
			current = Replica{
				ConversationID: turn.ConversationID,
				ReplicaID:      len(replicas) + 1,
				SpeakerTrue:    turn.Speaker,
				TurnIDs:        []int{turn.TurnID},
				Text:           turn.Text,
				Turns:          []Turn{turn},
			}
			continue
		}

		if turn.Speaker == current.SpeakerTrue {
			current.TurnIDs = append(current.TurnIDs, turn.TurnID)
			current.Turns = append(current.Turns, turn)
			if strings.TrimSpace(turn.Text) != "" {
				if strings.TrimSpace(current.Text) == "" {
					current.Text = turn.Text
				} else {
					current.Text += "\n" + turn.Text
				}
			}
			continue
		}

		flush()
		current = Replica{
			ConversationID: turn.ConversationID,
			ReplicaID:      len(replicas) + 1,
			SpeakerTrue:    turn.Speaker,
			TurnIDs:        []int{turn.TurnID},
			Text:           turn.Text,
			Turns:          []Turn{turn},
		}
	}
	flush()
	return replicas
}

func convertReplicaTurns(turns []Turn) []ReplicaTurnOut {
	out := make([]ReplicaTurnOut, 0, len(turns))
	for _, t := range turns {
		out = append(out, ReplicaTurnOut{
			TurnID:  t.TurnID,
			Speaker: t.Speaker,
			Text:    t.Text,
		})
	}
	return out
}

func runSpeakerUnit(ctx context.Context, client LLMClient, cfg Config, replica Replica, prevText, nextText string) (UnitSpeakerResult, []string) {
	res := UnitSpeakerResult{
		OK:               false,
		Attempts:         0,
		ValidationErrors: []string{},
		Output:           json.RawMessage("{}"),
	}
	requestIDs := []string{}

	if cfg.DryRun {
		res.OK = true
		return res, requestIDs
	}

	schema := speakerSchema()
	retryBudget := cfg.MaxRetries
	qualityRetryUsed := false
	qualityHint := ""

	bestValid := false
	bestMismatch := false
	var bestOutput SpeakerAttributionOutput

	for {
		res.Attempts++
		messages := speakerMessages(prevText, replica.Text, nextText, qualityHint)
		content, reqID, err := client.GenerateStructured(ctx, cfg.Model, messages, "unit_speaker_attribution_v1", schema)
		if reqID != "" {
			requestIDs = append(requestIDs, reqID)
		}
		if err != nil {
			res.ValidationErrors = append(res.ValidationErrors, fmt.Sprintf("attempt %d: api_error: %v", res.Attempts, err))
			if retryBudget > 0 {
				retryBudget--
				continue
			}
			break
		}

		var parsed SpeakerAttributionOutput
		if err := json.Unmarshal([]byte(content), &parsed); err != nil {
			res.ValidationErrors = append(res.ValidationErrors, fmt.Sprintf("attempt %d: parse_error: %v", res.Attempts, err))
			if retryBudget > 0 {
				retryBudget--
				continue
			}
			break
		}

		parsed.Confidence = clamp01(parsed.Confidence)
		validationErrs := validateSpeakerOutput(parsed, replica.Text)
		if len(validationErrs) > 0 {
			for _, verr := range validationErrs {
				res.ValidationErrors = append(res.ValidationErrors, fmt.Sprintf("attempt %d: %s", res.Attempts, verr))
			}
			if retryBudget > 0 {
				retryBudget--
				continue
			}
			break
		}

		bestValid = true
		bestOutput = parsed
		bestMismatch = parsed.PredictedSpeaker != replica.SpeakerTrue

		if !bestMismatch {
			res.OK = true
			res.Output = mustMarshalRaw(parsed)
			return res, requestIDs
		}

		if !qualityRetryUsed {
			qualityRetryUsed = true
			qualityHint = "Your previous prediction mismatched the ground truth in our evaluation. Re-check the text and decide again. Return JSON only."
			continue
		}

		break
	}

	if bestValid {
		res.OK = true
		res.Output = mustMarshalRaw(bestOutput)
		if bestMismatch {
			res.ValidationErrors = append(res.ValidationErrors, "quality:speaker_mismatch")
		}
	}

	return res, requestIDs
}

func runEmpathyUnit(ctx context.Context, client LLMClient, cfg Config, replica Replica) (UnitEmpathyResult, []string) {
	res := UnitEmpathyResult{
		Ran:              false,
		OK:               true,
		Attempts:         0,
		ValidationErrors: []string{},
		Output:           mustMarshalRaw(defaultNoEmpathyOutput()),
	}
	requestIDs := []string{}

	if replica.SpeakerTrue != speakerSalesRep {
		return res, requestIDs
	}
	if cfg.DryRun {
		return res, requestIDs
	}

	res.Ran = true
	schema := empathySchema()
	retryBudget := cfg.MaxRetries
	retryHint := ""

	for {
		res.Attempts++
		messages := empathyMessages(replica.Text, retryHint)
		content, reqID, err := client.GenerateStructured(ctx, cfg.Model, messages, "unit_empathy_detection_v1", schema)
		if reqID != "" {
			requestIDs = append(requestIDs, reqID)
		}
		if err != nil {
			res.OK = false
			res.ValidationErrors = append(res.ValidationErrors, fmt.Sprintf("attempt %d: api_error: %v", res.Attempts, err))
			if retryBudget > 0 {
				retryBudget--
				continue
			}
			return res, requestIDs
		}

		var parsed EmpathyDetectionOutput
		if err := json.Unmarshal([]byte(content), &parsed); err != nil {
			res.OK = false
			res.ValidationErrors = append(res.ValidationErrors, fmt.Sprintf("attempt %d: parse_error: %v", res.Attempts, err))
			if retryBudget > 0 {
				retryBudget--
				retryHint = "The quote MUST be copied exactly from the text. Pick an exact substring."
				continue
			}
			return res, requestIDs
		}

		parsed.Confidence = clamp01(parsed.Confidence)
		validationErrs := validateEmpathyOutput(parsed, replica.Text)
		if len(validationErrs) > 0 {
			res.OK = false
			for _, verr := range validationErrs {
				res.ValidationErrors = append(res.ValidationErrors, fmt.Sprintf("attempt %d: %s", res.Attempts, verr))
			}
			if retryBudget > 0 {
				retryBudget--
				retryHint = "The quote MUST be copied exactly from the text. Pick an exact substring."
				continue
			}
			return res, requestIDs
		}

		res.OK = true
		res.Output = mustMarshalRaw(parsed)
		return res, requestIDs
	}
}

func defaultNoEmpathyOutput() EmpathyDetectionOutput {
	return EmpathyDetectionOutput{
		EmpathyPresent: false,
		EmpathyType:    "none",
		Confidence:     0,
		Evidence:       []EvidenceQuote{},
	}
}

func validateSpeakerOutput(out SpeakerAttributionOutput, replicaText string) []string {
	errs := []string{}
	if out.PredictedSpeaker != speakerSalesRep && out.PredictedSpeaker != speakerCustomer {
		errs = append(errs, "format:predicted_speaker must be Sales Rep or Customer")
	}
	if strings.TrimSpace(out.Evidence.Quote) == "" {
		errs = append(errs, "format:evidence.quote is required")
	} else if !strings.Contains(replicaText, out.Evidence.Quote) {
		errs = append(errs, "format:evidence.quote must be exact substring of replica_text")
	}
	return errs
}

func validateEmpathyOutput(out EmpathyDetectionOutput, replicaText string) []string {
	errs := []string{}
	allowed := map[string]struct{}{
		"none": {}, "validation": {}, "reassurance": {}, "apology": {}, "support": {}, "other": {},
	}
	if _, ok := allowed[out.EmpathyType]; !ok {
		errs = append(errs, "format:empathy_type is invalid")
	}

	if !out.EmpathyPresent {
		if out.EmpathyType != "none" {
			errs = append(errs, "format:empathy_type must be none when empathy_present=false")
		}
		if len(out.Evidence) != 0 {
			errs = append(errs, "format:evidence must be empty when empathy_present=false")
		}
		return errs
	}

	if len(out.Evidence) == 0 {
		errs = append(errs, "format:evidence must include at least one quote when empathy_present=true")
		return errs
	}
	if strings.TrimSpace(out.Evidence[0].Quote) == "" {
		errs = append(errs, "format:evidence[0].quote is required")
		return errs
	}
	if !strings.Contains(replicaText, out.Evidence[0].Quote) {
		errs = append(errs, "format:evidence[0].quote must be exact substring of replica_text")
	}
	return errs
}

func speakerMessages(prevText, replicaText, nextText, qualityHint string) []LLMMessage {
	user := fmt.Sprintf("previous: %q\ncurrent: %q\nnext: %q\nTask: predict who wrote \"current\": Sales Rep or Customer.\nReturn JSON only.", prevText, replicaText, nextText)
	if qualityHint != "" {
		user += "\n\n" + qualityHint
	}
	return []LLMMessage{
		{Role: "system", Content: "Return only JSON matching schema (strict). Evidence quote must be an exact substring of the provided text."},
		{Role: "user", Content: user},
	}
}

func empathyMessages(replicaText, retryHint string) []LLMMessage {
	user := fmt.Sprintf("text: %q\nTask: detect whether empathy is present in this sales-replica text.\nReturn JSON only.", replicaText)
	if retryHint != "" {
		user += "\n\n" + retryHint
	}
	return []LLMMessage{
		{Role: "system", Content: "Return only JSON matching schema (strict)."},
		{Role: "user", Content: user},
	}
}

func speakerSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"predicted_speaker", "confidence", "evidence"},
		"properties": map[string]any{
			"predicted_speaker": map[string]any{"enum": []string{speakerSalesRep, speakerCustomer}},
			"confidence":        map[string]any{"type": "number"},
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
		"required":             []string{"empathy_present", "empathy_type", "confidence", "evidence"},
		"properties": map[string]any{
			"empathy_present": map[string]any{"type": "boolean"},
			"empathy_type": map[string]any{
				"enum": []string{"none", "validation", "reassurance", "apology", "support", "other"},
			},
			"confidence": map[string]any{"type": "number"},
			"evidence": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"required":             []string{"quote"},
					"properties": map[string]any{
						"quote": map[string]any{"type": "string"},
					},
				},
			},
		},
	}
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

func mustMarshalRaw(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return json.RawMessage(b)
}
