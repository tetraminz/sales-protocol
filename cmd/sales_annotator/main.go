package main

/*
sales_annotator annotates conversations from gwenshap/sales-transcripts CSV files.

Usage:
  OPENAI_API_KEY=... go run ./cmd/sales_annotator \
    --input_dir data/chunked_transcripts \
    --out_jsonl out/annotations.jsonl \
    --model gpt-4.1-mini

Flags:
  --input_dir      Directory with conversation CSV files.
  --out_jsonl      Output JSONL path (one record per line).
  --model          OpenAI model for annotation (default: gpt-4.1-mini).
  --limit          Optional max number of conversations (0 means all).
  --filter_prefix  Optional filename prefix filter, e.g. "modamart__".
*/

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tetraminz/sales_protocol/internal/compute"
	"github.com/tetraminz/sales_protocol/internal/dataset"
	"github.com/tetraminz/sales_protocol/internal/openai"
)

const (
	recordSchemaVersion = "record_v1"
	datasetName         = "gwenshap/sales-transcripts"
	datasetSplit        = "train"
	datasetSourceType   = "chunked_csv"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	inputDir := flag.String("input_dir", "", "directory containing conversation CSV files")
	outJSONL := flag.String("out_jsonl", "", "output jsonl path")
	model := flag.String("model", "gpt-4.1-mini", "OpenAI model name")
	limit := flag.Int("limit", 0, "optional max conversations to process (0 = all)")
	filterPrefix := flag.String("filter_prefix", "", "optional filename prefix filter")
	flag.Parse()

	if strings.TrimSpace(*inputDir) == "" {
		return errors.New("--input_dir is required")
	}
	if strings.TrimSpace(*outJSONL) == "" {
		return errors.New("--out_jsonl is required")
	}
	if *limit < 0 {
		return errors.New("--limit must be >= 0")
	}

	apiKey := strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))
	if apiKey == "" {
		return errors.New("OPENAI_API_KEY is required")
	}

	conversations, err := dataset.LoadConversations(*inputDir, *filterPrefix, *limit)
	if err != nil {
		return err
	}
	if len(conversations) == 0 {
		return errors.New("no CSV conversations matched the current filters")
	}

	if err := ensureParentDir(*outJSONL); err != nil {
		return err
	}

	outFile, err := os.Create(*outJSONL)
	if err != nil {
		return fmt.Errorf("create %q: %w", *outJSONL, err)
	}
	defer outFile.Close()

	annotator := openai.NewClient(apiKey, *model, nil)
	encoder := json.NewEncoder(outFile)
	encoder.SetEscapeHTML(false)

	for _, conversation := range conversations {
		annotation, err := annotator.AnnotateConversation(
			ctx,
			conversation.ConversationID,
			conversation.CompanyKey,
			conversation.Turns,
		)
		if err != nil {
			return fmt.Errorf("annotate %s: %w", conversation.ConversationID, err)
		}

		record := outputRecord{
			SchemaVersion: recordSchemaVersion,
			Dataset: datasetInfo{
				Name:       datasetName,
				Split:      datasetSplit,
				SourceType: datasetSourceType,
			},
			Conversation: conversationInfo{
				ConversationID: conversation.ConversationID,
				CompanyKey:     conversation.CompanyKey,
				SourceFile:     filepath.ToSlash(conversation.SourceFile),
			},
			Input: inputPayload{
				Turns:         conversation.Turns,
				RawTranscript: conversation.RawTranscript,
			},
			Computed: compute.ComputeMetrics(conversation.Turns),
			LLM:      annotation,
		}

		if err := encoder.Encode(record); err != nil {
			return fmt.Errorf("write record %s: %w", conversation.ConversationID, err)
		}
	}

	fmt.Printf("Wrote %d conversations to %s\n", len(conversations), *outJSONL)
	return nil
}

func ensureParentDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "." {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create output directory %q: %w", dir, err)
	}
	return nil
}

type outputRecord struct {
	SchemaVersion string           `json:"schema_version"`
	Dataset       datasetInfo      `json:"dataset"`
	Conversation  conversationInfo `json:"conversation"`
	Input         inputPayload     `json:"input"`
	Computed      compute.Metrics  `json:"computed"`
	LLM           json.RawMessage  `json:"llm"`
}

type datasetInfo struct {
	Name       string `json:"name"`
	Split      string `json:"split"`
	SourceType string `json:"source_type"`
}

type conversationInfo struct {
	ConversationID string `json:"conversation_id"`
	CompanyKey     string `json:"company_key"`
	SourceFile     string `json:"source_file"`
}

type inputPayload struct {
	Turns         []dataset.Turn `json:"turns"`
	RawTranscript string         `json:"raw_transcript"`
}
