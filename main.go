package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	cfg := parseFlags()
	if err := validateConfig(cfg); err != nil {
		log.Fatalf("config error: %v", err)
	}

	ctx := context.Background()
	if err := run(ctx, cfg); err != nil {
		log.Fatalf("run failed: %v", err)
	}

	fmt.Printf("Done. Wrote annotations to %s\n", cfg.OutJSONL)
}

func parseFlags() Config {
	cfg := Config{}
	flag.StringVar(&cfg.InputDir, "input_dir", "/Users/ablackman/data/sales-transcripts/data/chunked_transcripts", "Folder with CSV files (required)")
	flag.StringVar(&cfg.OutJSONL, "out_jsonl", "", "Output JSONL path (required)")
	flag.IntVar(&cfg.LimitConversations, "limit_conversations", 20, "How many CSV conversations to process")
	flag.StringVar(&cfg.Model, "model", "gpt-4.1-mini", "OpenAI model")
	flag.IntVar(&cfg.MaxRetries, "max_retries", 2, "Retries per unit for API/validation errors")
	flag.BoolVar(&cfg.DryRun, "dry_run", false, "Do not call OpenAI, only emit empty/default unit outputs")
	flag.Parse()

	cfg.APIKey = os.Getenv("OPENAI_API_KEY")
	cfg.BaseURL = os.Getenv("OPENAI_BASE_URL")
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultBaseURL
	}
	return cfg
}

func validateConfig(cfg Config) error {
	if cfg.InputDir == "" {
		return fmt.Errorf("--input_dir is required")
	}
	if cfg.OutJSONL == "" {
		return fmt.Errorf("--out_jsonl is required")
	}
	if cfg.MaxRetries < 0 {
		return fmt.Errorf("--max_retries must be >= 0")
	}
	if cfg.LimitConversations < 0 {
		return fmt.Errorf("--limit_conversations must be >= 0")
	}
	if !cfg.DryRun && cfg.APIKey == "" {
		return fmt.Errorf("OPENAI_API_KEY is required unless --dry_run=true")
	}
	return nil
}
