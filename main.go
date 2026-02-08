package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	log.SetFlags(0)
	if err := runCLI(); err != nil {
		log.Fatalf("error: %v", err)
	}
}

func runCLI() error {
	if len(os.Args) < 2 {
		printUsage()
		return nil
	}

	command := os.Args[1]
	args := os.Args[2:]

	switch command {
	case "setup":
		return runSetupCmd(args)
	case "annotate":
		return runAnnotateCmd(args)
	case "analytics":
		return runAnalyticsCmd(args)
	case "debug-release":
		return runDebugReleaseCmd(args)
	case "report":
		return runReportCmd(args)
	case "-h", "--help", "help":
		printUsage()
		return nil
	default:
		printUsage()
		return fmt.Errorf("unknown command: %s", command)
	}
}

func runSetupCmd(args []string) error {
	fs := flag.NewFlagSet("setup", flag.ContinueOnError)
	dbPath := fs.String("db", defaultSQLitePath, "Path to SQLite DB file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if err := SetupSQLite(*dbPath); err != nil {
		return err
	}
	fmt.Printf("sqlite_setup_ok db=%s\n", *dbPath)
	return nil
}

func runAnnotateCmd(args []string) error {
	fs := flag.NewFlagSet("annotate", flag.ContinueOnError)
	cfg := AnnotateConfig{}
	fs.StringVar(&cfg.DBPath, "db", defaultSQLitePath, "Path to SQLite DB file")
	fs.StringVar(&cfg.InputDir, "input_dir", defaultInputDir, "Directory with source CSV dialogs")
	fs.IntVar(&cfg.FromIdx, "from_idx", 1, "1-based inclusive start index in sorted CSV list")
	fs.IntVar(&cfg.ToIdx, "to_idx", 20, "1-based inclusive end index in sorted CSV list")
	fs.StringVar(&cfg.ReleaseTag, "release_tag", defaultReleaseTag, "Release tag for this run")
	fs.StringVar(&cfg.Model, "model", defaultAnnotateModel, "OpenAI model")
	fs.IntVar(&cfg.MaxRetries, "max_retries", 2, "Retry count for speaker LLM parsing/validation")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg.APIKey = os.Getenv("OPENAI_API_KEY")
	cfg.BaseURL = os.Getenv("OPENAI_BASE_URL")
	if strings.TrimSpace(cfg.BaseURL) == "" {
		cfg.BaseURL = defaultOpenAIBaseURL
	}

	runID, err := AnnotateRangeToSQLite(context.Background(), cfg)
	if err != nil {
		return err
	}
	fmt.Printf("annotate_ok run_id=%s db=%s\n", runID, cfg.DBPath)
	return nil
}

func runAnalyticsCmd(args []string) error {
	fs := flag.NewFlagSet("analytics", flag.ContinueOnError)
	dbPath := fs.String("db", defaultSQLitePath, "Path to SQLite DB file")
	runID := fs.String("run_id", defaultRunIDArg, "Run id or 'latest'")
	outPath := fs.String("out", filepath.Join("out", "analytics_latest.md"), "Output markdown path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	body, err := BuildAnalyticsMarkdown(*dbPath, *runID)
	if err != nil {
		return err
	}
	if err := writeTextFile(*outPath, body); err != nil {
		return err
	}
	fmt.Printf("analytics_ok run_id=%s out=%s\n", *runID, *outPath)
	return nil
}

func runDebugReleaseCmd(args []string) error {
	fs := flag.NewFlagSet("debug-release", flag.ContinueOnError)
	dbPath := fs.String("db", defaultSQLitePath, "Path to SQLite DB file")
	runID := fs.String("run_id", defaultRunIDArg, "Run id or 'latest'")
	outPath := fs.String("out", filepath.Join("out", "release_debug_latest.md"), "Output markdown path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	body, err := BuildReleaseDebugMarkdown(*dbPath, *runID)
	if err != nil {
		return err
	}
	if err := writeTextFile(*outPath, body); err != nil {
		return err
	}
	fmt.Printf("debug_release_ok run_id=%s out=%s\n", *runID, *outPath)
	return nil
}

func runReportCmd(args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	dbPath := fs.String("db", defaultSQLitePath, "Path to SQLite DB file")
	runID := fs.String("run_id", defaultRunIDArg, "Run id or 'latest'")
	if err := fs.Parse(args); err != nil {
		return err
	}

	report, err := BuildReport(*dbPath, *runID)
	if err != nil {
		return err
	}
	PrintReport(report)
	return nil
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  go run . setup --db out/annotations.db")
	fmt.Println("  OPENAI_API_KEY=... go run . annotate --db out/annotations.db --input_dir /Users/ablackman/data/sales-transcripts/data/chunked_transcripts --from_idx 1 --to_idx 20 --release_tag manual --model gpt-4.1-mini --max_retries 2")
	fmt.Println("  go run . analytics --db out/annotations.db --run_id latest --out out/analytics_latest.md")
	fmt.Println("  go run . debug-release --db out/annotations.db --run_id latest --out out/release_debug_latest.md")
	fmt.Println("  go run . report --db out/annotations.db --run_id latest")
}

func writeTextFile(path string, content string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("out path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write output file: %w", err)
	}
	return nil
}
