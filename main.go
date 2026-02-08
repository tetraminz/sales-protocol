package main

import (
	"flag"
	"fmt"
	"log"
	"os"
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
	case "migrate":
		return runMigrateCmd(args)
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

func runMigrateCmd(args []string) error {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	inJSONL := fs.String("in_jsonl", defaultInputJSONLPath, "Path to input annotations JSONL")
	outDB := fs.String("out_db", defaultSQLitePath, "Path to output SQLite DB file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	inserted, err := MigrateJSONLToSQLite(*inJSONL, *outDB)
	if err != nil {
		return err
	}
	fmt.Printf("migrated_rows=%d db=%s\n", inserted, *outDB)
	return nil
}

func runReportCmd(args []string) error {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	dbPath := fs.String("db", defaultSQLitePath, "Path to SQLite DB file")
	if err := fs.Parse(args); err != nil {
		return err
	}

	report, err := BuildReport(*dbPath)
	if err != nil {
		return err
	}
	PrintReport(report)
	return nil
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  go run . migrate --in_jsonl out/annotations.jsonl --out_db out/annotations.db")
	fmt.Println("  go run . report --db out/annotations.db")
}
