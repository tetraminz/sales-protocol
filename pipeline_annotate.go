package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

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

	store, err := OpenSQLiteStore(cfg.DBPath)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.ResetForAnnotateRun(); err != nil {
		return err
	}

	client := newOpenAIClient(cfg.APIKey, cfg.BaseURL)
	speakerCase := newOpenAISpeakerCase(client, cfg.Model, store)
	empathyCase := newOpenAIEmpathyCase(client, cfg.Model, store)
	process := AnnotationBusinessProcess{
		SpeakerUnit: speakerCase,
		EmpathyUnit: empathyCase,
	}

	totalBlocks := 0
	rawCorrect := 0
	finalCorrect := 0
	fmt.Printf("annotate_start files=%d db=%s model=%s range=%d..%d\n", len(selected), cfg.DBPath, cfg.Model, cfg.FromIdx, cfg.ToIdx)

	for fileIdx, path := range selected {
		turns, err := readConversationTurns(path)
		if err != nil {
			return fmt.Errorf("read turns %s: %w", path, err)
		}
		blocks := buildUtteranceBlocks(turns)
		conversationID := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		if len(blocks) > 0 {
			conversationID = blocks[0].ConversationID
		}

		fmt.Printf("annotate_file %d/%d conversation=%s utterance_blocks=%d\n", fileIdx+1, len(selected), conversationID, len(blocks))
		fileRawCorrect := 0
		fileFinalCorrect := 0

		for i, block := range blocks {
			prevText := ""
			nextText := ""
			if i > 0 {
				prevText = blocks[i-1].UtteranceText
			}
			if i+1 < len(blocks) {
				nextText = blocks[i+1].UtteranceText
			}

			speakerCase.setLogContext(block.ConversationID, block.UtteranceIndex)
			empathyCase.setLogContext(block.ConversationID, block.UtteranceIndex)

			out, err := process.Run(ctx, ProcessInput{
				UtteranceText:      block.UtteranceText,
				PreviousText:       prevText,
				NextText:           nextText,
				GroundTruthSpeaker: block.GroundTruthSpeaker,
			})
			if err != nil {
				return fmt.Errorf("process conversation=%s utterance_index=%d: %w", block.ConversationID, block.UtteranceIndex, err)
			}

			reviewStatus := reviewStatusNotApplicable
			if out.Empathy.EmpathyApplicable {
				reviewStatus = reviewStatusPending
			}

			annotation := AnnotationRow{
				ConversationID:                block.ConversationID,
				UtteranceIndex:                block.UtteranceIndex,
				UtteranceText:                 block.UtteranceText,
				GroundTruthSpeaker:            block.GroundTruthSpeaker,
				PredictedSpeaker:              out.Speaker.PredictedSpeaker,
				PredictedSpeakerConfidence:    out.Speaker.PredictedSpeakerConfidence,
				SpeakerIsCorrectRaw:           out.Speaker.SpeakerIsCorrectRaw,
				SpeakerIsCorrectFinal:         out.Speaker.SpeakerIsCorrectFinal,
				SpeakerQualityDecision:        out.Speaker.SpeakerQualityDecision,
				FarewellIsCurrentUtterance:    out.Speaker.FarewellIsCurrentUtterance,
				FarewellIsConversationClosing: out.Speaker.FarewellIsConversationClosing,
				FarewellContextSource:         out.Speaker.FarewellContextSource,
				SpeakerEvidenceQuote:          out.Speaker.SpeakerEvidenceQuote,
				SpeakerEvidenceIsValid:        out.Speaker.SpeakerEvidenceIsValid,
				EmpathyApplicable:             out.Empathy.EmpathyApplicable,
				EmpathyPresent:                out.Empathy.EmpathyPresent,
				EmpathyConfidence:             out.Empathy.EmpathyConfidence,
				EmpathyEvidenceQuote:          out.Empathy.EmpathyEvidenceQuote,
				EmpathyReviewStatus:           reviewStatus,
				EmpathyReviewerNote:           "",
				Model:                         cfg.Model,
				AnnotatedAtUTC:                time.Now().UTC().Format(time.RFC3339),
			}
			if err := store.InsertAnnotation(annotation); err != nil {
				return fmt.Errorf("insert annotation conversation=%s utterance_index=%d: %w", block.ConversationID, block.UtteranceIndex, err)
			}

			totalBlocks++
			if out.Speaker.SpeakerIsCorrectRaw {
				rawCorrect++
				fileRawCorrect++
			}
			if out.Speaker.SpeakerIsCorrectFinal {
				finalCorrect++
				fileFinalCorrect++
			}

			fmt.Printf(
				"annotate_row conversation=%s utterance_index=%d ground_truth=%s predicted=%s raw_correct=%d final_correct=%d quality_decision=%s empathy_applicable=%d\n",
				block.ConversationID,
				block.UtteranceIndex,
				canonicalSpeakerLabel(block.GroundTruthSpeaker),
				canonicalSpeakerLabel(out.Speaker.PredictedSpeaker),
				boolToInt(out.Speaker.SpeakerIsCorrectRaw),
				boolToInt(out.Speaker.SpeakerIsCorrectFinal),
				out.Speaker.SpeakerQualityDecision,
				boolToInt(out.Empathy.EmpathyApplicable),
			)

			if totalBlocks%25 == 0 {
				fmt.Printf(
					"annotate_progress utterance_blocks=%d raw_correct=%d raw_mismatch=%d final_correct=%d final_mismatch=%d\n",
					totalBlocks,
					rawCorrect,
					totalBlocks-rawCorrect,
					finalCorrect,
					totalBlocks-finalCorrect,
				)
			}
		}

		fmt.Printf(
			"annotate_file_done conversation=%s utterance_blocks=%d raw_correct=%d raw_mismatch=%d final_correct=%d final_mismatch=%d\n",
			conversationID,
			len(blocks),
			fileRawCorrect,
			len(blocks)-fileRawCorrect,
			fileFinalCorrect,
			len(blocks)-fileFinalCorrect,
		)
	}

	fmt.Printf(
		"annotate_done utterance_blocks=%d raw_correct=%d raw_mismatch=%d final_correct=%d final_mismatch=%d\n",
		totalBlocks,
		rawCorrect,
		totalBlocks-rawCorrect,
		finalCorrect,
		totalBlocks-finalCorrect,
	)
	return nil
}
