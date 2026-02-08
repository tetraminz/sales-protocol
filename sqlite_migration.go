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

	_ "modernc.org/sqlite"
)

const (
	defaultSQLitePath    = "out/annotations.db"
	defaultInputDir      = "/Users/ablackman/data/sales-transcripts/data/chunked_transcripts"
	defaultRunIDArg      = "latest"
	defaultReleaseTag    = "manual"
	defaultAnnotateModel = "gpt-4.1-mini"
	defaultOpenAIBaseURL = "https://api.openai.com"
	speakerCustomer      = "Customer"
	shortUtteranceMaxLen = 40
)

type AnnotateConfig struct {
	DBPath     string
	InputDir   string
	FromIdx    int
	ToIdx      int
	ReleaseTag string
	Model      string
	APIKey     string
	BaseURL    string
	MaxRetries int
}

type runRecord struct {
	RunID              string
	ReleaseTag         string
	CreatedAtUTC       string
	InputDir           string
	FromIdx            int
	ToIdx              int
	Model              string
	TotalConversations int
	TotalReplicas      int
}

type reportTypeCount struct {
	Type  string
	Count int
}

type reportErrorCount struct {
	Error string
	Count int
}

type conversationDebugItem struct {
	ConversationID string
	RedReplicas    int
	TotalReplicas  int
	TopReasons     []string
}

type shortUtteranceMismatch struct {
	ConversationID string
	ReplicaID      int
	ReplicaText    string
	TextLength     int
}

type reportMetrics struct {
	Run runRecord

	TotalRows                   int
	TotalConversations          int
	SpeakerMatchCount           int
	SpeakerAccuracyPercent      float64
	SpeakerOKFalseCount         int
	QualitySpeakerMismatchCount int
	EmpathyRanCount             int
	EmpathyPresentCount         int
	EmpathyPresentPercent       float64
	LabelFormatMismatchCount    int
	ModelMismatchCount          int
	TransientRetryErrorCount    int

	GreenReplicaCount      int
	RedReplicaCount        int
	GreenReplicaPercent    float64
	RedReplicaPercent      float64
	GreenConversationCount int
	RedConversationCount   int
	GreenConversationPct   float64
	RedConversationPct     float64

	EmpathyTypeDistribution    []reportTypeCount
	TopSpeakerValidationErrors []reportErrorCount
	TopEmpathyValidationErrors []reportErrorCount
	RedConversations           []conversationDebugItem
	ShortUtteranceMismatches   []shortUtteranceMismatch
}

type analyzedReplicaRow struct {
	ConversationID   string
	ReplicaID        int
	SpeakerTrue      string
	SpeakerPredicted string
	ReplicaText      string
	SpeakerMatch     bool
	SpeakerOK        bool
	SpeakerErrors    []string
	EmpathyRan       bool
	EmpathyOK        bool
	EmpathyPresent   bool
	EmpathyType      string
	EmpathyErrors    []string
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
	TurnIDs        []int
	SpeakerTrue    string
	Text           string
	Turns          []annotateTurn
}

type annotationInsert struct {
	RunID                       string
	ConversationID              string
	ReplicaID                   int
	SpeakerTrue                 string
	SpeakerPredicted            string
	SpeakerMatch                int
	SpeakerOK                   int
	SpeakerAttempts             int
	SpeakerValidationErrorsJSON string
	SpeakerOutputJSON           string
	EmpathyRan                  int
	EmpathyOK                   int
	EmpathyPresent              int
	EmpathyType                 string
	EmpathyConfidence           float64
	EmpathyAttempts             int
	EmpathyValidationErrorsJSON string
	EmpathyOutputJSON           string
	ReplicaText                 string
	TurnIDsJSON                 string
	Model                       string
	TimestampUTC                string
	RequestIDsJSON              string
}

type conversationAccumulator struct {
	Total      int
	Green      int
	Red        int
	ReasonHits map[string]int
}

type runDelta struct {
	HasPrevious              bool
	PreviousRunID            string
	SpeakerAccuracyDeltaPP   float64
	GreenReplicaDeltaPP      float64
	RedReplicaDeltaPP        float64
	GreenConversationDeltaPP float64
	RedConversationDeltaPP   float64
	QualityMismatchDelta     int
}

type unitTrace struct {
	Ran              bool
	OK               bool
	Attempts         int
	ValidationErrors []string
	RequestIDs       []string
}

type tracedSpeakerCase interface {
	ReplicaSpeakerCase
	LastTrace() unitTrace
}

type tracedEmpathyCase interface {
	EmpathyCase
	LastTrace() unitTrace
}

type caseBundle struct {
	Process AnnotationBusinessProcess
	Speaker tracedSpeakerCase
	Empathy tracedEmpathyCase
	Model   string
}

type openAIClient struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

func SetupSQLite(dbPath string) error {
	if strings.TrimSpace(dbPath) == "" {
		return fmt.Errorf("db path is required")
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return fmt.Errorf("create db directory: %w", err)
	}

	db, err := openSQLite(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	return ensureSQLiteSchema(db)
}

func AnnotateRangeToSQLite(ctx context.Context, cfg AnnotateConfig) (string, error) {
	if strings.TrimSpace(cfg.DBPath) == "" {
		return "", fmt.Errorf("db path is required")
	}
	if strings.TrimSpace(cfg.InputDir) == "" {
		return "", fmt.Errorf("input_dir is required")
	}
	if strings.TrimSpace(cfg.ReleaseTag) == "" {
		cfg.ReleaseTag = defaultReleaseTag
	}
	if strings.TrimSpace(cfg.Model) == "" {
		cfg.Model = defaultAnnotateModel
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		cfg.BaseURL = defaultOpenAIBaseURL
	}

	if err := SetupSQLite(cfg.DBPath); err != nil {
		return "", err
	}

	files, err := findCSVFiles(cfg.InputDir)
	if err != nil {
		return "", err
	}
	selected, err := selectCSVRange(files, cfg.FromIdx, cfg.ToIdx)
	if err != nil {
		return "", err
	}

	runID := generateRunID(cfg.ReleaseTag)
	createdAt := time.Now().UTC().Format(time.RFC3339)
	fmt.Printf(
		"annotate_start run_id=%s model=%s range=%d..%d files=%d db=%s\n",
		runID,
		cfg.Model,
		cfg.FromIdx,
		cfg.ToIdx,
		len(selected),
		cfg.DBPath,
	)

	db, err := openSQLite(cfg.DBPath)
	if err != nil {
		return "", err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return "", fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(insertAnnotationSQL)
	if err != nil {
		return "", fmt.Errorf("prepare insert annotation: %w", err)
	}
	defer stmt.Close()

	cases, err := buildCaseBundle(cfg)
	if err != nil {
		return "", err
	}

	totalReplicas := 0
	conversationSet := map[string]struct{}{}
	speakerMatchCount := 0
	speakerMismatchCount := 0
	speakerOKFalseCount := 0
	speakerFormatErrorsCount := 0
	manualEmpathySkipped := 0

	for fileIdx, path := range selected {
		turns, err := readTurns(path)
		if err != nil {
			return "", fmt.Errorf("read turns %s: %w", path, err)
		}
		replicas := buildReplicas(turns)
		conversationID := firstConversationID(turns, path)
		fileReplicaCount := 0
		fileSpeakerMatchCount := 0
		fileSpeakerMismatchCount := 0
		fileSpeakerOKFalseCount := 0
		fmt.Printf(
			"annotate_file %d/%d conversation=%s turns=%d replicas=%d\n",
			fileIdx+1,
			len(selected),
			conversationID,
			len(turns),
			len(replicas),
		)
		for i, replica := range replicas {
			prevText := ""
			nextText := ""
			if i > 0 {
				prevText = replicas[i-1].Text
			}
			if i+1 < len(replicas) {
				nextText = replicas[i+1].Text
			}

			out, err := cases.Process.Run(ctx, ProcessInput{
				ReplicaText: replica.Text,
				PrevText:    prevText,
				NextText:    nextText,
				SpeakerTrue: replica.SpeakerTrue,
			})
			if err != nil {
				return "", fmt.Errorf("annotate %s replica %d: %w", replica.ConversationID, replica.ReplicaID, err)
			}

			speakerTrace := cases.Speaker.LastTrace()
			empathyTrace := unitTrace{
				Ran:              false,
				OK:               true,
				Attempts:         0,
				ValidationErrors: []string{},
				RequestIDs:       []string{},
			}
			if replica.SpeakerTrue == speakerSalesRep {
				empathyTrace = cases.Empathy.LastTrace()
			}
			row := buildAnnotationInsert(runID, cases.Model, replica, out, speakerTrace, empathyTrace)
			if _, err := stmt.Exec(
				row.RunID,
				row.ConversationID,
				row.ReplicaID,
				row.SpeakerTrue,
				row.SpeakerPredicted,
				row.SpeakerMatch,
				row.SpeakerOK,
				row.SpeakerAttempts,
				row.SpeakerValidationErrorsJSON,
				row.SpeakerOutputJSON,
				row.EmpathyRan,
				row.EmpathyOK,
				row.EmpathyPresent,
				row.EmpathyType,
				row.EmpathyConfidence,
				row.EmpathyAttempts,
				row.EmpathyValidationErrorsJSON,
				row.EmpathyOutputJSON,
				row.ReplicaText,
				row.TurnIDsJSON,
				row.Model,
				row.TimestampUTC,
				row.RequestIDsJSON,
			); err != nil {
				return "", fmt.Errorf("insert annotation row: %w", err)
			}

			totalReplicas++
			conversationSet[replica.ConversationID] = struct{}{}
			fileReplicaCount++
			if row.SpeakerMatch == 1 {
				speakerMatchCount++
				fileSpeakerMatchCount++
			} else {
				speakerMismatchCount++
				fileSpeakerMismatchCount++
			}
			if row.SpeakerOK == 0 {
				speakerOKFalseCount++
				fileSpeakerOKFalseCount++
			}
			if hasFormatError(parseStringArray(row.SpeakerValidationErrorsJSON)) {
				speakerFormatErrorsCount++
			}
			if row.EmpathyRan == 0 {
				manualEmpathySkipped++
			}
			fmt.Printf(
				"annotate_row conversation=%s replica=%d speaker_true=%s speaker_predicted=%s speaker_match=%d speaker_ok=%d empathy_ran=%d\n",
				row.ConversationID,
				row.ReplicaID,
				row.SpeakerTrue,
				row.SpeakerPredicted,
				row.SpeakerMatch,
				row.SpeakerOK,
				row.EmpathyRan,
			)

			if totalReplicas%25 == 0 {
				fmt.Printf(
					"annotate_progress replicas=%d speaker_match=%d speaker_mismatch=%d speaker_ok_false=%d manual_empathy_skipped=%d\n",
					totalReplicas,
					speakerMatchCount,
					speakerMismatchCount,
					speakerOKFalseCount,
					manualEmpathySkipped,
				)
			}
		}
		fmt.Printf(
			"annotate_file_done %d/%d conversation=%s replicas=%d speaker_match=%d speaker_mismatch=%d speaker_ok_false=%d\n",
			fileIdx+1,
			len(selected),
			conversationID,
			fileReplicaCount,
			fileSpeakerMatchCount,
			fileSpeakerMismatchCount,
			fileSpeakerOKFalseCount,
		)
	}

	if _, err := tx.Exec(insertRunSQL,
		runID,
		cfg.ReleaseTag,
		createdAt,
		cfg.InputDir,
		cfg.FromIdx,
		cfg.ToIdx,
		cfg.Model,
		len(conversationSet),
		totalReplicas,
	); err != nil {
		return "", fmt.Errorf("insert annotation run: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit annotate transaction: %w", err)
	}
	fmt.Printf(
		"annotate_done run_id=%s conversations=%d replicas=%d speaker_match=%d speaker_mismatch=%d speaker_ok_false=%d speaker_format_errors=%d manual_empathy_skipped=%d\n",
		runID,
		len(conversationSet),
		totalReplicas,
		speakerMatchCount,
		speakerMismatchCount,
		speakerOKFalseCount,
		speakerFormatErrorsCount,
		manualEmpathySkipped,
	)
	return runID, nil
}

func firstConversationID(turns []annotateTurn, fallbackPath string) string {
	if len(turns) > 0 && strings.TrimSpace(turns[0].ConversationID) != "" {
		return turns[0].ConversationID
	}
	return strings.TrimSuffix(filepath.Base(fallbackPath), filepath.Ext(fallbackPath))
}

func hasFormatError(items []string) bool {
	for _, item := range items {
		if strings.HasPrefix(item, "format:") || strings.Contains(item, ": format:") {
			return true
		}
	}
	return false
}

func BuildAnalyticsMarkdown(dbPath, runIDOrLatest string) (string, error) {
	report, err := BuildReport(dbPath, runIDOrLatest)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("# Analytics: %s\n\n", report.Run.RunID))
	b.WriteString("## Run Metadata\n")
	b.WriteString(fmt.Sprintf("- run_id: `%s`\n", report.Run.RunID))
	b.WriteString(fmt.Sprintf("- release_tag: `%s`\n", report.Run.ReleaseTag))
	b.WriteString(fmt.Sprintf("- created_at_utc: `%s`\n", report.Run.CreatedAtUTC))
	b.WriteString(fmt.Sprintf("- input_dir: `%s`\n", report.Run.InputDir))
	b.WriteString(fmt.Sprintf("- range: `%d..%d`\n", report.Run.FromIdx, report.Run.ToIdx))
	b.WriteString(fmt.Sprintf("- model: `%s`\n\n", report.Run.Model))

	b.WriteString("## Totals\n")
	b.WriteString(fmt.Sprintf("- replicas: `%d`\n", report.TotalRows))
	b.WriteString(fmt.Sprintf("- conversations: `%d`\n", report.TotalConversations))
	b.WriteString(fmt.Sprintf("- green replicas: `%d` (%.2f%%)\n", report.GreenReplicaCount, report.GreenReplicaPercent))
	b.WriteString(fmt.Sprintf("- red replicas: `%d` (%.2f%%)\n\n", report.RedReplicaCount, report.RedReplicaPercent))

	b.WriteString("## Speaker Accuracy\n")
	b.WriteString(fmt.Sprintf("- accuracy: `%.2f%%` (`%d/%d`)\n", report.SpeakerAccuracyPercent, report.SpeakerMatchCount, report.TotalRows))
	b.WriteString(fmt.Sprintf("- speaker_ok_false_count: `%d`\n", report.SpeakerOKFalseCount))
	b.WriteString(fmt.Sprintf("- quality_speaker_mismatch_count: `%d`\n\n", report.QualitySpeakerMismatchCount))

	b.WriteString("## Empathy\n")
	b.WriteString(fmt.Sprintf("- empathy_ran_count: `%d`\n", report.EmpathyRanCount))
	b.WriteString(fmt.Sprintf("- empathy_present_count: `%d` (%.2f%% of empathy_ran)\n", report.EmpathyPresentCount, report.EmpathyPresentPercent))
	if len(report.EmpathyTypeDistribution) == 0 {
		b.WriteString("- empathy_type_distribution: `none`\n\n")
	} else {
		b.WriteString("- empathy_type_distribution:\n")
		for _, item := range report.EmpathyTypeDistribution {
			b.WriteString(fmt.Sprintf("  - `%s`: `%d`\n", item.Type, item.Count))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Top Validation Errors\n")
	b.WriteString("- speaker:\n")
	if len(report.TopSpeakerValidationErrors) == 0 {
		b.WriteString("  - none\n")
	} else {
		for _, item := range report.TopSpeakerValidationErrors {
			b.WriteString(fmt.Sprintf("  - `%s`: `%d`\n", item.Error, item.Count))
		}
	}
	b.WriteString("- empathy:\n")
	if len(report.TopEmpathyValidationErrors) == 0 {
		b.WriteString("  - none\n")
	} else {
		for _, item := range report.TopEmpathyValidationErrors {
			b.WriteString(fmt.Sprintf("  - `%s`: `%d`\n", item.Error, item.Count))
		}
	}
	b.WriteString("\n")

	b.WriteString("## Root Cause Breakdown\n")
	b.WriteString(fmt.Sprintf("- label_format_mismatch_false_red_count: `%d`\n", report.LabelFormatMismatchCount))
	b.WriteString(fmt.Sprintf("- real_model_mismatch_count: `%d`\n", report.ModelMismatchCount))
	b.WriteString(fmt.Sprintf("- transient_retry_error_count: `%d`\n", report.TransientRetryErrorCount))
	b.WriteString("- top_short_utterance_mismatches:\n")
	if len(report.ShortUtteranceMismatches) == 0 {
		b.WriteString("  - none\n")
	} else {
		for _, item := range report.ShortUtteranceMismatches {
			b.WriteString(fmt.Sprintf("  - `%s` / replica `%d` / len `%d`: `%s`\n",
				item.ConversationID,
				item.ReplicaID,
				item.TextLength,
				item.ReplicaText,
			))
		}
	}
	b.WriteString("\n")

	b.WriteString("## Short Conclusion\n")
	b.WriteString(fmt.Sprintf("- Разметка стабильна на `%.2f%%` speaker accuracy.\n", report.SpeakerAccuracyPercent))
	b.WriteString(fmt.Sprintf("- Красных реплик: `%d` из `%d`.\n", report.RedReplicaCount, report.TotalRows))
	b.WriteString(fmt.Sprintf("- Красных диалогов: `%d` из `%d`.\n", report.RedConversationCount, report.TotalConversations))

	return b.String(), nil
}

func BuildReleaseDebugMarkdown(dbPath, runIDOrLatest string) (string, error) {
	db, err := openSQLite(dbPath)
	if err != nil {
		return "", err
	}
	defer db.Close()

	runID, err := ResolveRunID(db, runIDOrLatest)
	if err != nil {
		return "", err
	}

	current, err := collectRunMetrics(db, runID)
	if err != nil {
		return "", err
	}

	delta := runDelta{}
	prevRunID, err := previousRunID(db, current.Run)
	if err != nil {
		return "", err
	}
	if prevRunID != "" {
		prev, err := collectRunMetrics(db, prevRunID)
		if err != nil {
			return "", err
		}
		delta = runDelta{
			HasPrevious:              true,
			PreviousRunID:            prevRunID,
			SpeakerAccuracyDeltaPP:   current.SpeakerAccuracyPercent - prev.SpeakerAccuracyPercent,
			GreenReplicaDeltaPP:      current.GreenReplicaPercent - prev.GreenReplicaPercent,
			RedReplicaDeltaPP:        current.RedReplicaPercent - prev.RedReplicaPercent,
			GreenConversationDeltaPP: current.GreenConversationPct - prev.GreenConversationPct,
			RedConversationDeltaPP:   current.RedConversationPct - prev.RedConversationPct,
			QualityMismatchDelta:     current.QualitySpeakerMismatchCount - prev.QualitySpeakerMismatchCount,
		}
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("# Release Debug: %s\n\n", current.Run.RunID))
	b.WriteString("## Summary\n")
	b.WriteString(fmt.Sprintf("- green replicas: `%d` (%.2f%%)\n", current.GreenReplicaCount, current.GreenReplicaPercent))
	b.WriteString(fmt.Sprintf("- red replicas: `%d` (%.2f%%)\n", current.RedReplicaCount, current.RedReplicaPercent))
	b.WriteString(fmt.Sprintf("- green conversations: `%d` (%.2f%%)\n", current.GreenConversationCount, current.GreenConversationPct))
	b.WriteString(fmt.Sprintf("- red conversations: `%d` (%.2f%%)\n\n", current.RedConversationCount, current.RedConversationPct))

	b.WriteString("## Red Conversations\n")
	if len(current.RedConversations) == 0 {
		b.WriteString("- none\n\n")
	} else {
		b.WriteString("| conversation_id | red_replicas | total_replicas | top_reasons |\n")
		b.WriteString("| --- | ---: | ---: | --- |\n")
		for _, item := range current.RedConversations {
			b.WriteString(fmt.Sprintf("| `%s` | `%d` | `%d` | %s |\n",
				item.ConversationID,
				item.RedReplicas,
				item.TotalReplicas,
				strings.Join(item.TopReasons, "; "),
			))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Root Cause Breakdown\n")
	b.WriteString(fmt.Sprintf("- label_format_mismatch_false_red_count: `%d`\n", current.LabelFormatMismatchCount))
	b.WriteString(fmt.Sprintf("- real_model_mismatch_count: `%d`\n", current.ModelMismatchCount))
	b.WriteString(fmt.Sprintf("- transient_retry_error_count: `%d`\n\n", current.TransientRetryErrorCount))

	b.WriteString("## Top Short-Utterance Mismatches\n")
	if len(current.ShortUtteranceMismatches) == 0 {
		b.WriteString("- none\n\n")
	} else {
		b.WriteString("| conversation_id | replica_id | text_length | replica_text |\n")
		b.WriteString("| --- | ---: | ---: | --- |\n")
		for _, item := range current.ShortUtteranceMismatches {
			b.WriteString(fmt.Sprintf("| `%s` | `%d` | `%d` | `%s` |\n",
				item.ConversationID,
				item.ReplicaID,
				item.TextLength,
				strings.ReplaceAll(item.ReplicaText, "`", "'"),
			))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Delta vs previous run\n")
	if !delta.HasPrevious {
		b.WriteString("- previous run: `none`\n")
	} else {
		b.WriteString(fmt.Sprintf("- previous_run_id: `%s`\n", delta.PreviousRunID))
		b.WriteString(fmt.Sprintf("- speaker_accuracy_delta_pp: `%+.2f`\n", delta.SpeakerAccuracyDeltaPP))
		b.WriteString(fmt.Sprintf("- green_replica_delta_pp: `%+.2f`\n", delta.GreenReplicaDeltaPP))
		b.WriteString(fmt.Sprintf("- red_replica_delta_pp: `%+.2f`\n", delta.RedReplicaDeltaPP))
		b.WriteString(fmt.Sprintf("- green_conversation_delta_pp: `%+.2f`\n", delta.GreenConversationDeltaPP))
		b.WriteString(fmt.Sprintf("- red_conversation_delta_pp: `%+.2f`\n", delta.RedConversationDeltaPP))
		b.WriteString(fmt.Sprintf("- quality_mismatch_delta: `%+d`\n", delta.QualityMismatchDelta))
	}

	return b.String(), nil
}

func BuildReport(dbPath, runIDOrLatest string) (reportMetrics, error) {
	db, err := openSQLite(dbPath)
	if err != nil {
		return reportMetrics{}, err
	}
	defer db.Close()

	runID, err := ResolveRunID(db, runIDOrLatest)
	if err != nil {
		return reportMetrics{}, err
	}

	return collectRunMetrics(db, runID)
}

func PrintReport(r reportMetrics) {
	fmt.Print(FormatReport(r))
}

func FormatReport(r reportMetrics) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("run_id=%s\n", r.Run.RunID))
	b.WriteString(fmt.Sprintf("release_tag=%s\n", r.Run.ReleaseTag))
	b.WriteString(fmt.Sprintf("created_at_utc=%s\n", r.Run.CreatedAtUTC))
	b.WriteString(fmt.Sprintf("range=%d..%d\n", r.Run.FromIdx, r.Run.ToIdx))
	b.WriteString(fmt.Sprintf("total_rows=%d\n", r.TotalRows))
	b.WriteString(fmt.Sprintf("total_conversations=%d\n", r.TotalConversations))
	b.WriteString(fmt.Sprintf("speaker_accuracy_percent=%.2f (%d/%d)\n", r.SpeakerAccuracyPercent, r.SpeakerMatchCount, r.TotalRows))
	b.WriteString(fmt.Sprintf("quality_speaker_mismatch_count=%d\n", r.QualitySpeakerMismatchCount))
	b.WriteString(fmt.Sprintf("label_format_mismatch_false_red_count=%d\n", r.LabelFormatMismatchCount))
	b.WriteString(fmt.Sprintf("real_model_mismatch_count=%d\n", r.ModelMismatchCount))
	b.WriteString(fmt.Sprintf("transient_retry_error_count=%d\n", r.TransientRetryErrorCount))
	b.WriteString(fmt.Sprintf("green_replicas=%d (%.2f%%)\n", r.GreenReplicaCount, r.GreenReplicaPercent))
	b.WriteString(fmt.Sprintf("red_replicas=%d (%.2f%%)\n", r.RedReplicaCount, r.RedReplicaPercent))
	return b.String()
}

func ResolveRunID(db *sql.DB, runIDOrLatest string) (string, error) {
	arg := strings.TrimSpace(runIDOrLatest)
	if arg == "" || arg == defaultRunIDArg {
		var runID string
		err := db.QueryRow(`SELECT run_id FROM annotation_runs ORDER BY created_at_utc DESC, run_id DESC LIMIT 1`).Scan(&runID)
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("no annotation runs found")
		}
		if err != nil {
			return "", fmt.Errorf("resolve latest run_id: %w", err)
		}
		return runID, nil
	}

	var exists int
	if err := db.QueryRow(`SELECT COUNT(1) FROM annotation_runs WHERE run_id = ?`, arg).Scan(&exists); err != nil {
		return "", fmt.Errorf("check run_id: %w", err)
	}
	if exists == 0 {
		return "", fmt.Errorf("run_id not found: %s", arg)
	}
	return arg, nil
}

func collectRunMetrics(db *sql.DB, runID string) (reportMetrics, error) {
	run, err := getRunRecord(db, runID)
	if err != nil {
		return reportMetrics{}, err
	}

	rows, err := db.Query(`
		SELECT
			conversation_id,
			replica_id,
			speaker_true,
			speaker_predicted,
			replica_text,
			speaker_match,
			speaker_ok,
			speaker_validation_errors_json,
			empathy_ran,
			empathy_ok,
			empathy_present,
			empathy_type,
			empathy_validation_errors_json
		FROM annotations
		WHERE run_id = ?
	`, runID)
	if err != nil {
		return reportMetrics{}, fmt.Errorf("query run rows: %w", err)
	}
	defer rows.Close()

	speakerErrCounts := map[string]int{}
	empathyErrCounts := map[string]int{}
	empathyTypeCounts := map[string]int{}
	conversations := map[string]*conversationAccumulator{}

	report := reportMetrics{Run: run}
	for rows.Next() {
		var convoID string
		var replicaID int
		var speakerTrue string
		var speakerPredicted string
		var replicaText string
		var speakerMatch int
		var speakerOK int
		var speakerErrorsJSON string
		var empathyRan int
		var empathyOK int
		var empathyPresent int
		var empathyType string
		var empathyErrorsJSON string

		if err := rows.Scan(
			&convoID,
			&replicaID,
			&speakerTrue,
			&speakerPredicted,
			&replicaText,
			&speakerMatch,
			&speakerOK,
			&speakerErrorsJSON,
			&empathyRan,
			&empathyOK,
			&empathyPresent,
			&empathyType,
			&empathyErrorsJSON,
		); err != nil {
			return reportMetrics{}, fmt.Errorf("scan run rows: %w", err)
		}

		row := analyzedReplicaRow{
			ConversationID:   convoID,
			ReplicaID:        replicaID,
			SpeakerTrue:      speakerTrue,
			SpeakerPredicted: speakerPredicted,
			ReplicaText:      replicaText,
			SpeakerMatch:     speakerMatch == 1,
			SpeakerOK:        speakerOK == 1,
			SpeakerErrors:    parseStringArray(speakerErrorsJSON),
			EmpathyRan:       empathyRan == 1,
			EmpathyOK:        empathyOK == 1,
			EmpathyPresent:   empathyPresent == 1,
			EmpathyType:      empathyType,
			EmpathyErrors:    parseStringArray(empathyErrorsJSON),
		}

		report.TotalRows++
		if row.SpeakerMatch {
			report.SpeakerMatchCount++
		}
		if !row.SpeakerOK {
			report.SpeakerOKFalseCount++
		}
		if row.EmpathyRan {
			report.EmpathyRanCount++
		}
		if row.EmpathyPresent {
			report.EmpathyPresentCount++
		}
		if row.EmpathyType == "" {
			row.EmpathyType = "none"
		}
		empathyTypeCounts[row.EmpathyType]++

		for _, errItem := range row.SpeakerErrors {
			speakerErrCounts[errItem]++
		}
		for _, errItem := range row.EmpathyErrors {
			empathyErrCounts[errItem]++
		}
		if row.SpeakerMatch && row.SpeakerOK && len(row.SpeakerErrors) > 0 {
			report.TransientRetryErrorCount++
		}
		if !row.SpeakerMatch {
			if canonicalSpeakerLabel(row.SpeakerTrue) == canonicalSpeakerLabel(row.SpeakerPredicted) {
				report.LabelFormatMismatchCount++
			} else {
				report.ModelMismatchCount++
				text := strings.TrimSpace(row.ReplicaText)
				if len([]rune(text)) <= shortUtteranceMaxLen {
					report.ShortUtteranceMismatches = append(report.ShortUtteranceMismatches, shortUtteranceMismatch{
						ConversationID: row.ConversationID,
						ReplicaID:      row.ReplicaID,
						ReplicaText:    text,
						TextLength:     len([]rune(text)),
					})
				}
			}
		}

		state, ok := conversations[row.ConversationID]
		if !ok {
			state = &conversationAccumulator{ReasonHits: map[string]int{}}
			conversations[row.ConversationID] = state
		}
		state.Total++

		if isGreenReplica(row) {
			report.GreenReplicaCount++
			state.Green++
		} else {
			report.RedReplicaCount++
			state.Red++
			for _, reason := range reasonsForRedReplica(row) {
				state.ReasonHits[reason]++
			}
		}
	}
	if err := rows.Err(); err != nil {
		return reportMetrics{}, fmt.Errorf("iterate run rows: %w", err)
	}

	report.TotalConversations = len(conversations)
	for _, state := range conversations {
		if state.Red == 0 {
			report.GreenConversationCount++
		} else {
			report.RedConversationCount++
		}
	}

	if report.TotalRows > 0 {
		report.SpeakerAccuracyPercent = float64(report.SpeakerMatchCount) * 100 / float64(report.TotalRows)
		report.GreenReplicaPercent = float64(report.GreenReplicaCount) * 100 / float64(report.TotalRows)
		report.RedReplicaPercent = float64(report.RedReplicaCount) * 100 / float64(report.TotalRows)
	}
	if report.TotalConversations > 0 {
		report.GreenConversationPct = float64(report.GreenConversationCount) * 100 / float64(report.TotalConversations)
		report.RedConversationPct = float64(report.RedConversationCount) * 100 / float64(report.TotalConversations)
	}
	if report.EmpathyRanCount > 0 {
		report.EmpathyPresentPercent = float64(report.EmpathyPresentCount) * 100 / float64(report.EmpathyRanCount)
	}

	report.EmpathyTypeDistribution = toSortedTypeCounts(empathyTypeCounts)
	report.TopSpeakerValidationErrors = toSortedErrorCounts(speakerErrCounts)
	report.TopEmpathyValidationErrors = toSortedErrorCounts(empathyErrCounts)
	report.QualitySpeakerMismatchCount = speakerErrCounts["quality:speaker_mismatch"]
	report.RedConversations = buildRedConversationItems(conversations)
	sort.Slice(report.ShortUtteranceMismatches, func(i, j int) bool {
		if report.ShortUtteranceMismatches[i].TextLength == report.ShortUtteranceMismatches[j].TextLength {
			if report.ShortUtteranceMismatches[i].ConversationID == report.ShortUtteranceMismatches[j].ConversationID {
				return report.ShortUtteranceMismatches[i].ReplicaID < report.ShortUtteranceMismatches[j].ReplicaID
			}
			return report.ShortUtteranceMismatches[i].ConversationID < report.ShortUtteranceMismatches[j].ConversationID
		}
		return report.ShortUtteranceMismatches[i].TextLength < report.ShortUtteranceMismatches[j].TextLength
	})
	if len(report.ShortUtteranceMismatches) > 10 {
		report.ShortUtteranceMismatches = report.ShortUtteranceMismatches[:10]
	}
	return report, nil
}

func getRunRecord(db *sql.DB, runID string) (runRecord, error) {
	var run runRecord
	if err := db.QueryRow(`
		SELECT
			run_id,
			release_tag,
			created_at_utc,
			input_dir,
			from_idx,
			to_idx,
			model,
			total_conversations,
			total_replicas
		FROM annotation_runs
		WHERE run_id = ?
	`, runID).Scan(
		&run.RunID,
		&run.ReleaseTag,
		&run.CreatedAtUTC,
		&run.InputDir,
		&run.FromIdx,
		&run.ToIdx,
		&run.Model,
		&run.TotalConversations,
		&run.TotalReplicas,
	); err != nil {
		if err == sql.ErrNoRows {
			return runRecord{}, fmt.Errorf("run not found: %s", runID)
		}
		return runRecord{}, fmt.Errorf("query run record: %w", err)
	}
	return run, nil
}

func previousRunID(db *sql.DB, run runRecord) (string, error) {
	var runID string
	err := db.QueryRow(`
		SELECT run_id
		FROM annotation_runs
		WHERE created_at_utc < ?
		ORDER BY created_at_utc DESC, run_id DESC
		LIMIT 1
	`, run.CreatedAtUTC).Scan(&runID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("query previous run: %w", err)
	}
	return runID, nil
}

func ensureSQLiteSchema(db *sql.DB) error {
	if err := migrateLegacyIfNeeded(db); err != nil {
		return err
	}
	for _, stmt := range schemaStatements {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("apply schema statement: %w", err)
		}
	}
	return nil
}

func migrateLegacyIfNeeded(db *sql.DB) error {
	annotationsExists, err := tableExists(db, "annotations")
	if err != nil {
		return err
	}
	if !annotationsExists {
		return nil
	}

	hasRunID, err := tableHasColumn(db, "annotations", "run_id")
	if err != nil {
		return err
	}
	if hasRunID {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin legacy migration tx: %w", err)
	}
	defer tx.Rollback()

	legacyTable := "annotations_legacy_tmp"
	if _, err := tx.Exec(`DROP TABLE IF EXISTS ` + legacyTable); err != nil {
		return fmt.Errorf("drop old legacy tmp table: %w", err)
	}
	if _, err := tx.Exec(`ALTER TABLE annotations RENAME TO ` + legacyTable); err != nil {
		return fmt.Errorf("rename legacy annotations table: %w", err)
	}

	for _, stmt := range schemaStatements {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("create new schema during legacy migration: %w", err)
		}
	}

	var totalReplicas int
	var totalConversations int
	var model string
	if err := tx.QueryRow(`
		SELECT
			COUNT(*),
			COUNT(DISTINCT conversation_id),
			COALESCE(MIN(model), 'legacy_model')
		FROM `+legacyTable).Scan(&totalReplicas, &totalConversations, &model); err != nil {
		return fmt.Errorf("read legacy stats: %w", err)
	}

	runID := fmt.Sprintf("legacy_import_%d", time.Now().UTC().Unix())
	createdAt := time.Now().UTC().Format(time.RFC3339)
	if _, err := tx.Exec(insertRunSQL,
		runID,
		"legacy",
		createdAt,
		"legacy_import",
		0,
		0,
		model,
		totalConversations,
		totalReplicas,
	); err != nil {
		return fmt.Errorf("insert legacy run row: %w", err)
	}

	if _, err := tx.Exec(`
		INSERT INTO annotations (
			run_id,
			conversation_id,
			replica_id,
			speaker_true,
			speaker_predicted,
			speaker_match,
			speaker_ok,
			speaker_attempts,
			speaker_validation_errors_json,
			speaker_output_json,
			empathy_ran,
			empathy_ok,
			empathy_present,
			empathy_type,
			empathy_confidence,
			empathy_attempts,
			empathy_validation_errors_json,
			empathy_output_json,
			replica_text,
			turn_ids_json,
			model,
			timestamp_utc,
			request_ids_json
		)
		SELECT
			?,
			conversation_id,
			replica_id,
			speaker_true,
			speaker_predicted,
			speaker_match,
			speaker_ok,
			speaker_attempts,
			speaker_validation_errors_json,
			speaker_output_json,
			empathy_ran,
			empathy_ok,
			empathy_present,
			empathy_type,
			empathy_confidence,
			empathy_attempts,
			empathy_validation_errors_json,
			empathy_output_json,
			replica_text,
			turn_ids_json,
			model,
			timestamp_utc,
			request_ids_json
		FROM `+legacyTable, runID); err != nil {
		return fmt.Errorf("copy legacy rows: %w", err)
	}

	if _, err := tx.Exec(`DROP TABLE ` + legacyTable); err != nil {
		return fmt.Errorf("drop legacy table: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit legacy migration: %w", err)
	}
	return nil
}

func tableExists(db *sql.DB, table string) (bool, error) {
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?`, table).Scan(&count); err != nil {
		return false, fmt.Errorf("check table exists: %w", err)
	}
	return count > 0, nil
}

func tableHasColumn(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return false, fmt.Errorf("pragma table_info: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var ctype string
		var notnull int
		var dfltValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err != nil {
			return false, fmt.Errorf("scan pragma table_info: %w", err)
		}
		if name == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate pragma table_info: %w", err)
	}
	return false, nil
}

func openSQLite(dbPath string) (*sql.DB, error) {
	if strings.TrimSpace(dbPath) == "" {
		return nil, fmt.Errorf("db path is required")
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite db: %w", err)
	}
	return db, nil
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

		chunkRaw := getField(rec, index["Chunk_id"])
		turnID, err := strconv.Atoi(strings.TrimSpace(chunkRaw))
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
	var current annotateReplica

	flush := func() {
		if len(current.Turns) == 0 {
			return
		}
		replicas = append(replicas, current)
	}

	for _, turn := range turns {
		if len(current.Turns) == 0 {
			current = annotateReplica{
				ConversationID: turn.ConversationID,
				ReplicaID:      len(replicas) + 1,
				TurnIDs:        []int{turn.TurnID},
				SpeakerTrue:    turn.Speaker,
				Text:           turn.Text,
				Turns:          []annotateTurn{turn},
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
		current = annotateReplica{
			ConversationID: turn.ConversationID,
			ReplicaID:      len(replicas) + 1,
			TurnIDs:        []int{turn.TurnID},
			SpeakerTrue:    turn.Speaker,
			Text:           turn.Text,
			Turns:          []annotateTurn{turn},
		}
	}
	flush()
	return replicas
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

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type speakerLLMOutput struct {
	PredictedSpeaker string  `json:"predicted_speaker"`
	Confidence       float64 `json:"confidence"`
	Evidence         struct {
		Quote string `json:"quote"`
	} `json:"evidence"`
}

type openAISpeakerCase struct {
	client     *openAIClient
	model      string
	maxRetries int
	trace      unitTrace
}

type manualEmpathyCase struct {
	trace unitTrace
}

func buildCaseBundle(cfg AnnotateConfig) (caseBundle, error) {
	if strings.TrimSpace(cfg.APIKey) == "" {
		return caseBundle{}, fmt.Errorf("OPENAI_API_KEY is required for annotate")
	}
	client := &openAIClient{
		apiKey:  cfg.APIKey,
		baseURL: strings.TrimRight(cfg.BaseURL, "/"),
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
	speaker := &openAISpeakerCase{
		client:     client,
		model:      cfg.Model,
		maxRetries: cfg.MaxRetries,
	}
	empathy := &manualEmpathyCase{}

	return caseBundle{
		Process: AnnotationBusinessProcess{
			SpeakerCase: speaker,
			EmpathyCase: empathy,
		},
		Speaker: speaker,
		Empathy: empathy,
		Model:   cfg.Model,
	}, nil
}

func (c *openAISpeakerCase) Evaluate(ctx context.Context, in ReplicaCaseInput) (ReplicaCaseResult, error) {
	c.trace = unitTrace{
		Ran:              true,
		OK:               false,
		Attempts:         0,
		ValidationErrors: []string{},
		RequestIDs:       []string{},
	}

	maxAttempts := c.maxRetries + 1
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	last := ReplicaCaseResult{}
	lastAttemptErrors := []string{}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		c.trace.Attempts++
		attemptErrors := []string{}
		content, requestID, err := c.client.generateStructured(
			ctx,
			c.model,
			speakerMessages(in.PrevText, in.ReplicaText, in.NextText),
			"unit_speaker_attribution_v1",
			speakerSchema(),
		)
		if requestID != "" {
			c.trace.RequestIDs = append(c.trace.RequestIDs, requestID)
		}
		if err != nil {
			attemptErrors = append(attemptErrors, fmt.Sprintf("attempt %d: api_error: %v", attempt, err))
			lastAttemptErrors = attemptErrors
			continue
		}

		var parsed speakerLLMOutput
		if err := json.Unmarshal([]byte(content), &parsed); err != nil {
			attemptErrors = append(attemptErrors, fmt.Sprintf("attempt %d: parse_error: %v", attempt, err))
			lastAttemptErrors = attemptErrors
			continue
		}
		parsed.Confidence = clamp01(parsed.Confidence)
		last = ReplicaCaseResult{
			PredictedSpeaker: parsed.PredictedSpeaker,
			Confidence:       parsed.Confidence,
			EvidenceQuote:    parsed.Evidence.Quote,
		}

		validation := validateSpeakerPrediction(parsed, in.ReplicaText)
		if len(validation) > 0 {
			for _, item := range validation {
				attemptErrors = append(attemptErrors, fmt.Sprintf("attempt %d: %s", attempt, item))
			}
			lastAttemptErrors = attemptErrors
			continue
		}

		c.trace.OK = true
		c.trace.ValidationErrors = []string{}
		return last, nil
	}

	c.trace.ValidationErrors = append([]string{}, lastAttemptErrors...)
	return last, nil
}

func (c *openAISpeakerCase) LastTrace() unitTrace {
	return copyTrace(c.trace)
}

func (c *manualEmpathyCase) Evaluate(ctx context.Context, in EmpathyCaseInput) (EmpathyCaseResult, error) {
	_ = ctx
	_ = in
	c.trace = unitTrace{
		Ran:              false,
		OK:               true,
		Attempts:         0,
		ValidationErrors: []string{},
		RequestIDs:       []string{},
	}
	return EmpathyCaseResult{
		Ran:            false,
		EmpathyPresent: false,
		EmpathyType:    "none",
		Confidence:     0,
		EvidenceQuote:  "",
	}, nil
}

func (c *manualEmpathyCase) LastTrace() unitTrace {
	return copyTrace(c.trace)
}

func copyTrace(in unitTrace) unitTrace {
	return unitTrace{
		Ran:              in.Ran,
		OK:               in.OK,
		Attempts:         in.Attempts,
		ValidationErrors: append([]string{}, in.ValidationErrors...),
		RequestIDs:       append([]string{}, in.RequestIDs...),
	}
}

func speakerMessages(prevText, replicaText, nextText string) []openAIMessage {
	system := "Return JSON only. Follow the schema strictly. evidence.quote must be an exact substring of current. For short or ambiguous current text (for example bye/thanks), use previous and next context to decide speaker."
	user := fmt.Sprintf(
		"previous: %q\ncurrent: %q\nnext: %q\nTask: predict who wrote current: Sales Rep or Customer. If current alone is ambiguous, rely on neighboring context.",
		prevText,
		replicaText,
		nextText,
	)
	return []openAIMessage{
		{Role: "system", Content: system},
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

func validateSpeakerPrediction(out speakerLLMOutput, replicaText string) []string {
	errs := []string{}
	if out.PredictedSpeaker != speakerSalesRep && out.PredictedSpeaker != speakerCustomer {
		errs = append(errs, "format:predicted_speaker_invalid")
	}
	if strings.TrimSpace(out.Evidence.Quote) == "" {
		errs = append(errs, "format:evidence_quote_required")
	} else if !strings.Contains(replicaText, out.Evidence.Quote) {
		errs = append(errs, "format:evidence_quote_not_substring")
	}
	return errs
}

func (c *openAIClient) generateStructured(
	ctx context.Context,
	model string,
	messages []openAIMessage,
	schemaName string,
	schema map[string]any,
) (string, string, error) {
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/chat/completions", bytes.NewReader(payload))
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

	requestID := resp.Header.Get("x-request-id")
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", requestID, fmt.Errorf("read response: %w", err)
	}
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

	msg := parsed.Choices[0].Message
	if strings.TrimSpace(msg.Refusal) != "" {
		return "", requestID, fmt.Errorf("model refusal: %s", msg.Refusal)
	}
	if strings.TrimSpace(msg.Content) == "" {
		return "", requestID, fmt.Errorf("empty content")
	}
	return msg.Content, requestID, nil
}

func buildAnnotationInsert(runID, model string, replica annotateReplica, out ProcessOutput, speakerTrace unitTrace, empathyTrace unitTrace) annotationInsert {
	speakerTrueCanonical := canonicalSpeakerLabel(replica.SpeakerTrue)
	speakerPredictedCanonical := canonicalSpeakerLabel(out.Speaker.PredictedSpeaker)
	speakerValidationErrors := append([]string{}, speakerTrace.ValidationErrors...)
	speakerOK := speakerTrace.OK

	if speakerPredictedCanonical != speakerSalesRep && speakerPredictedCanonical != speakerCustomer {
		speakerOK = false
		speakerValidationErrors = append(speakerValidationErrors, "format:predicted_speaker_invalid")
	}
	if strings.TrimSpace(out.Speaker.EvidenceQuote) == "" {
		speakerOK = false
		speakerValidationErrors = append(speakerValidationErrors, "format:evidence_quote_required")
	} else if !strings.Contains(replica.Text, out.Speaker.EvidenceQuote) {
		speakerOK = false
		speakerValidationErrors = append(speakerValidationErrors, "format:evidence_quote_not_substring")
	}

	speakerMatch := 0
	if speakerPredictedCanonical == speakerTrueCanonical {
		speakerMatch = 1
	} else {
		speakerValidationErrors = append(speakerValidationErrors, "quality:speaker_mismatch")
	}
	speakerValidationErrors = dedupeStringList(speakerValidationErrors)

	speakerOutput := mustJSON(map[string]any{
		"predicted_speaker": speakerPredictedCanonical,
		"confidence":        clamp01(out.Speaker.Confidence),
		"evidence": map[string]any{
			"quote": out.Speaker.EvidenceQuote,
		},
	})

	empathyValidationErrors := append([]string{}, empathyTrace.ValidationErrors...)
	empathyOK := empathyTrace.OK
	if !empathyTrace.Ran {
		empathyOK = true
	}
	empathyType := out.Empathy.EmpathyType
	if strings.TrimSpace(empathyType) == "" {
		empathyType = "none"
	}

	if !out.Empathy.Ran {
		if out.Empathy.EmpathyPresent || empathyType != "none" {
			empathyOK = false
			empathyValidationErrors = append(empathyValidationErrors, "format:skip_branch_inconsistent")
		}
	} else {
		if !out.Empathy.EmpathyPresent {
			if empathyType != "none" {
				empathyOK = false
				empathyValidationErrors = append(empathyValidationErrors, "format:empathy_type_must_be_none_when_false")
			}
			if strings.TrimSpace(out.Empathy.EvidenceQuote) != "" {
				empathyOK = false
				empathyValidationErrors = append(empathyValidationErrors, "format:evidence_must_be_empty_when_false")
			}
		} else {
			if strings.TrimSpace(out.Empathy.EvidenceQuote) == "" {
				empathyOK = false
				empathyValidationErrors = append(empathyValidationErrors, "format:evidence_quote_required")
			} else if !strings.Contains(replica.Text, out.Empathy.EvidenceQuote) {
				empathyOK = false
				empathyValidationErrors = append(empathyValidationErrors, "format:evidence_quote_not_substring")
			}
			if empathyType == "none" {
				empathyOK = false
				empathyValidationErrors = append(empathyValidationErrors, "format:empathy_type_none_with_true")
			}
		}
	}
	empathyValidationErrors = dedupeStringList(empathyValidationErrors)

	empathyEvidence := []map[string]string{}
	if out.Empathy.EmpathyPresent && strings.TrimSpace(out.Empathy.EvidenceQuote) != "" {
		empathyEvidence = append(empathyEvidence, map[string]string{"quote": out.Empathy.EvidenceQuote})
	}
	empathyOutput := mustJSON(map[string]any{
		"empathy_present": out.Empathy.EmpathyPresent,
		"empathy_type":    empathyType,
		"confidence":      clamp01(out.Empathy.Confidence),
		"evidence":        empathyEvidence,
	})

	empathyAttempts := empathyTrace.Attempts
	if empathyAttempts < 0 {
		empathyAttempts = 0
	}

	speakerAttempts := speakerTrace.Attempts
	if speakerAttempts < 0 {
		speakerAttempts = 0
	}

	requestIDs := append([]string{}, speakerTrace.RequestIDs...)
	requestIDs = append(requestIDs, empathyTrace.RequestIDs...)

	return annotationInsert{
		RunID:                       runID,
		ConversationID:              replica.ConversationID,
		ReplicaID:                   replica.ReplicaID,
		SpeakerTrue:                 speakerTrueCanonical,
		SpeakerPredicted:            speakerPredictedCanonical,
		SpeakerMatch:                speakerMatch,
		SpeakerOK:                   boolToInt(speakerOK),
		SpeakerAttempts:             speakerAttempts,
		SpeakerValidationErrorsJSON: mustJSON(speakerValidationErrors),
		SpeakerOutputJSON:           speakerOutput,
		EmpathyRan:                  boolToInt(out.Empathy.Ran),
		EmpathyOK:                   boolToInt(empathyOK),
		EmpathyPresent:              boolToInt(out.Empathy.EmpathyPresent),
		EmpathyType:                 empathyType,
		EmpathyConfidence:           clamp01(out.Empathy.Confidence),
		EmpathyAttempts:             empathyAttempts,
		EmpathyValidationErrorsJSON: mustJSON(empathyValidationErrors),
		EmpathyOutputJSON:           empathyOutput,
		ReplicaText:                 replica.Text,
		TurnIDsJSON:                 mustJSON(replica.TurnIDs),
		Model:                       model,
		TimestampUTC:                time.Now().UTC().Format(time.RFC3339),
		RequestIDsJSON:              mustJSON(requestIDs),
	}
}

func isGreenReplica(row analyzedReplicaRow) bool {
	if !row.SpeakerOK {
		return false
	}
	if !row.SpeakerMatch {
		return false
	}
	if len(row.SpeakerErrors) > 0 {
		return false
	}
	if !row.EmpathyRan {
		return true
	}
	if !row.EmpathyOK {
		return false
	}
	if len(row.EmpathyErrors) > 0 {
		return false
	}
	return true
}

func reasonsForRedReplica(row analyzedReplicaRow) []string {
	reasons := []string{}
	if !row.SpeakerOK {
		reasons = append(reasons, "speaker_ok_false")
	}
	if !row.SpeakerMatch {
		reasons = append(reasons, "speaker_mismatch")
	}
	for _, errItem := range row.SpeakerErrors {
		if errItem == "quality:speaker_mismatch" {
			continue
		}
		reasons = append(reasons, "speaker:"+errItem)
	}

	if row.EmpathyRan {
		if !row.EmpathyOK {
			reasons = append(reasons, "empathy_ok_false")
		}
		for _, errItem := range row.EmpathyErrors {
			reasons = append(reasons, "empathy:"+errItem)
		}
	}

	if len(reasons) == 0 {
		reasons = append(reasons, "unknown")
	}
	return dedupeStringList(reasons)
}

func buildRedConversationItems(conversations map[string]*conversationAccumulator) []conversationDebugItem {
	items := []conversationDebugItem{}
	for convoID, state := range conversations {
		if state.Red == 0 {
			continue
		}
		reasonPairs := make([]reportErrorCount, 0, len(state.ReasonHits))
		for key, count := range state.ReasonHits {
			reasonPairs = append(reasonPairs, reportErrorCount{Error: key, Count: count})
		}
		sort.Slice(reasonPairs, func(i, j int) bool {
			if reasonPairs[i].Count == reasonPairs[j].Count {
				return reasonPairs[i].Error < reasonPairs[j].Error
			}
			return reasonPairs[i].Count > reasonPairs[j].Count
		})
		topReasons := []string{}
		for i := 0; i < len(reasonPairs) && i < 3; i++ {
			topReasons = append(topReasons, fmt.Sprintf("%s (%d)", reasonPairs[i].Error, reasonPairs[i].Count))
		}

		items = append(items, conversationDebugItem{
			ConversationID: convoID,
			RedReplicas:    state.Red,
			TotalReplicas:  state.Total,
			TopReasons:     topReasons,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].RedReplicas == items[j].RedReplicas {
			return items[i].ConversationID < items[j].ConversationID
		}
		return items[i].RedReplicas > items[j].RedReplicas
	})
	return items
}

func toSortedTypeCounts(counts map[string]int) []reportTypeCount {
	out := make([]reportTypeCount, 0, len(counts))
	for key, count := range counts {
		out = append(out, reportTypeCount{Type: key, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Type < out[j].Type
		}
		return out[i].Count > out[j].Count
	})
	return out
}

func toSortedErrorCounts(counts map[string]int) []reportErrorCount {
	out := make([]reportErrorCount, 0, len(counts))
	for key, count := range counts {
		out = append(out, reportErrorCount{Error: key, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Error < out[j].Error
		}
		return out[i].Count > out[j].Count
	})
	return out
}

func parseStringArray(raw string) []string {
	items := []string{}
	if strings.TrimSpace(raw) == "" {
		return items
	}
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		return []string{}
	}
	return items
}

func dedupeStringList(items []string) []string {
	if len(items) == 0 {
		return []string{}
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func generateRunID(tag string) string {
	tag = sanitizeTag(tag)
	if tag == "" {
		tag = defaultReleaseTag
	}
	return fmt.Sprintf("run_%d_%s", time.Now().UTC().UnixNano(), tag)
}

func sanitizeTag(tag string) string {
	tag = strings.TrimSpace(strings.ToLower(tag))
	if tag == "" {
		return ""
	}
	var b strings.Builder
	for _, ch := range tag {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' {
			b.WriteRune(ch)
		}
	}
	return b.String()
}

func mustJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return "[]"
	}
	return string(data)
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

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS annotation_runs (
		run_id TEXT PRIMARY KEY,
		release_tag TEXT NOT NULL,
		created_at_utc TEXT NOT NULL,
		input_dir TEXT NOT NULL,
		from_idx INTEGER NOT NULL,
		to_idx INTEGER NOT NULL,
		model TEXT NOT NULL,
		total_conversations INTEGER NOT NULL,
		total_replicas INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS annotations (
		run_id TEXT NOT NULL,
		conversation_id TEXT NOT NULL,
		replica_id INTEGER NOT NULL,
		speaker_true TEXT NOT NULL,
		speaker_predicted TEXT,
		speaker_match INTEGER NOT NULL,
		speaker_ok INTEGER NOT NULL,
		speaker_attempts INTEGER NOT NULL,
		speaker_validation_errors_json TEXT NOT NULL,
		speaker_output_json TEXT NOT NULL,
		empathy_ran INTEGER NOT NULL,
		empathy_ok INTEGER NOT NULL,
		empathy_present INTEGER NOT NULL,
		empathy_type TEXT NOT NULL,
		empathy_confidence REAL NOT NULL,
		empathy_attempts INTEGER NOT NULL,
		empathy_validation_errors_json TEXT NOT NULL,
		empathy_output_json TEXT NOT NULL,
		replica_text TEXT NOT NULL,
		turn_ids_json TEXT NOT NULL,
		model TEXT NOT NULL,
		timestamp_utc TEXT NOT NULL,
		request_ids_json TEXT NOT NULL,
		PRIMARY KEY (run_id, conversation_id, replica_id)
	)`,
	`CREATE INDEX IF NOT EXISTS idx_annotations_run_id ON annotations(run_id)`,
	`CREATE INDEX IF NOT EXISTS idx_annotations_run_speaker_match ON annotations(run_id, speaker_match)`,
	`CREATE INDEX IF NOT EXISTS idx_annotations_run_speaker_ok ON annotations(run_id, speaker_ok)`,
	`CREATE INDEX IF NOT EXISTS idx_annotations_run_empathy_type ON annotations(run_id, empathy_type)`,
}

const insertRunSQL = `
INSERT INTO annotation_runs (
	run_id,
	release_tag,
	created_at_utc,
	input_dir,
	from_idx,
	to_idx,
	model,
	total_conversations,
	total_replicas
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`

const insertAnnotationSQL = `
INSERT INTO annotations (
	run_id,
	conversation_id,
	replica_id,
	speaker_true,
	speaker_predicted,
	speaker_match,
	speaker_ok,
	speaker_attempts,
	speaker_validation_errors_json,
	speaker_output_json,
	empathy_ran,
	empathy_ok,
	empathy_present,
	empathy_type,
	empathy_confidence,
	empathy_attempts,
	empathy_validation_errors_json,
	empathy_output_json,
	replica_text,
	turn_ids_json,
	model,
	timestamp_utc,
	request_ids_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`
