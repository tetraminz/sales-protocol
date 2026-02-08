package main

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

type reportMetrics struct {
	TotalRows          int
	TotalConversations int

	SpeakerCorrectRawCount      int
	SpeakerCorrectFinalCount    int
	SpeakerAccuracyRawPercent   float64
	SpeakerAccuracyFinalPercent float64
	RawMismatchCount            int
	FinalMismatchCount          int
	FarewellOverrideCount       int
	SpeakerEvidenceInvalidCount int

	EmpathyApplicableCount    int
	EmpathyConfidenceAvg      float64
	EmpathyConfidenceMin      float64
	EmpathyConfidenceMax      float64
	EmpathyReviewPendingCount int
	EmpathyReviewOKCount      int
	EmpathyReviewNotOKCount   int

	LLMEventCount            int
	LLMParseFailedCount      int
	LLMValidationFailedCount int

	RawRedConversations   []conversationDebugItem
	FinalRedConversations []conversationDebugItem
	TopRawMismatches      []utteranceDebugItem
	TopFinalMismatches    []utteranceDebugItem
	TopEvidenceInvalid    []utteranceDebugItem
	TopShortUtterances    []utteranceDebugItem
	NotOKItems            []empathyReviewItem
}

type conversationDebugItem struct {
	ConversationID string
	RedRows        int
	TotalRows      int
	TopReason      string
}

type utteranceDebugItem struct {
	ConversationID         string
	UtteranceIndex         int
	UtteranceText          string
	TextLength             int
	SpeakerQualityDecision string
}

type empathyReviewItem struct {
	ConversationID      string
	UtteranceIndex      int
	EmpathyConfidence   float64
	EmpathyReviewerNote string
}

func BuildReport(dbPath string) (reportMetrics, error) {
	db, err := openSQLite(dbPath)
	if err != nil {
		return reportMetrics{}, err
	}
	defer db.Close()
	if err := ensureStoreSchema(db); err != nil {
		return reportMetrics{}, err
	}

	rows, err := db.Query(`
		SELECT
			conversation_id,
			utterance_index,
			speaker_is_correct_raw,
			speaker_is_correct_final,
			speaker_quality_decision,
			speaker_evidence_is_valid,
			empathy_applicable,
			empathy_confidence,
			empathy_review_status,
			empathy_reviewer_note,
			utterance_text
		FROM annotations
	`)
	if err != nil {
		return reportMetrics{}, fmt.Errorf("query annotations: %w", err)
	}
	defer rows.Close()

	type convState struct {
		Total    int
		RawRed   int
		FinalRed int
	}
	conversations := map[string]*convState{}

	report := reportMetrics{}
	hasEmpathy := false
	for rows.Next() {
		var conversationID string
		var utteranceIndex int
		var rawCorrect int
		var finalCorrect int
		var qualityDecision string
		var speakerEvidenceValid int
		var empathyApplicable int
		var empathyConfidence float64
		var empathyReviewStatus string
		var empathyReviewerNote string
		var utteranceText string
		if err := rows.Scan(
			&conversationID,
			&utteranceIndex,
			&rawCorrect,
			&finalCorrect,
			&qualityDecision,
			&speakerEvidenceValid,
			&empathyApplicable,
			&empathyConfidence,
			&empathyReviewStatus,
			&empathyReviewerNote,
			&utteranceText,
		); err != nil {
			return reportMetrics{}, fmt.Errorf("scan annotation row: %w", err)
		}

		report.TotalRows++
		if rawCorrect == 1 {
			report.SpeakerCorrectRawCount++
		} else {
			report.RawMismatchCount++
		}
		if finalCorrect == 1 {
			report.SpeakerCorrectFinalCount++
		} else {
			report.FinalMismatchCount++
		}
		if strings.TrimSpace(qualityDecision) == qualityDecisionFarewellOverride {
			report.FarewellOverrideCount++
		}
		if speakerEvidenceValid == 0 {
			report.SpeakerEvidenceInvalidCount++
		}

		text := strings.TrimSpace(utteranceText)
		item := utteranceDebugItem{
			ConversationID:         conversationID,
			UtteranceIndex:         utteranceIndex,
			UtteranceText:          text,
			TextLength:             len([]rune(text)),
			SpeakerQualityDecision: strings.TrimSpace(qualityDecision),
		}
		if rawCorrect == 0 {
			report.TopRawMismatches = append(report.TopRawMismatches, item)
			if item.TextLength <= shortUtteranceMaxLen {
				report.TopShortUtterances = append(report.TopShortUtterances, item)
			}
		}
		if finalCorrect == 0 {
			report.TopFinalMismatches = append(report.TopFinalMismatches, item)
		}
		if speakerEvidenceValid == 0 {
			report.TopEvidenceInvalid = append(report.TopEvidenceInvalid, item)
		}

		state := conversations[conversationID]
		if state == nil {
			state = &convState{}
			conversations[conversationID] = state
		}
		state.Total++
		if rawCorrect == 0 {
			state.RawRed++
		}
		if finalCorrect == 0 {
			state.FinalRed++
		}

		if empathyApplicable == 1 {
			report.EmpathyApplicableCount++
			hasEmpathy = true
			if report.EmpathyApplicableCount == 1 {
				report.EmpathyConfidenceMin = empathyConfidence
				report.EmpathyConfidenceMax = empathyConfidence
			}
			if empathyConfidence < report.EmpathyConfidenceMin {
				report.EmpathyConfidenceMin = empathyConfidence
			}
			if empathyConfidence > report.EmpathyConfidenceMax {
				report.EmpathyConfidenceMax = empathyConfidence
			}
			report.EmpathyConfidenceAvg += empathyConfidence

			switch strings.TrimSpace(empathyReviewStatus) {
			case reviewStatusOK:
				report.EmpathyReviewOKCount++
			case reviewStatusNotOK:
				report.EmpathyReviewNotOKCount++
				report.NotOKItems = append(report.NotOKItems, empathyReviewItem{
					ConversationID:      conversationID,
					UtteranceIndex:      utteranceIndex,
					EmpathyConfidence:   empathyConfidence,
					EmpathyReviewerNote: strings.TrimSpace(empathyReviewerNote),
				})
			default:
				report.EmpathyReviewPendingCount++
			}
		}
	}
	if err := rows.Err(); err != nil {
		return reportMetrics{}, fmt.Errorf("iterate annotation rows: %w", err)
	}

	report.TotalConversations = len(conversations)
	for conversationID, state := range conversations {
		if state.RawRed > 0 {
			report.RawRedConversations = append(report.RawRedConversations, conversationDebugItem{
				ConversationID: conversationID,
				RedRows:        state.RawRed,
				TotalRows:      state.Total,
				TopReason:      fmt.Sprintf("raw_speaker_mismatch (%d)", state.RawRed),
			})
		}
		if state.FinalRed > 0 {
			report.FinalRedConversations = append(report.FinalRedConversations, conversationDebugItem{
				ConversationID: conversationID,
				RedRows:        state.FinalRed,
				TotalRows:      state.Total,
				TopReason:      fmt.Sprintf("final_speaker_mismatch (%d)", state.FinalRed),
			})
		}
	}

	if report.TotalRows > 0 {
		report.SpeakerAccuracyRawPercent = 100.0 * float64(report.SpeakerCorrectRawCount) / float64(report.TotalRows)
		report.SpeakerAccuracyFinalPercent = 100.0 * float64(report.SpeakerCorrectFinalCount) / float64(report.TotalRows)
	}
	if hasEmpathy && report.EmpathyApplicableCount > 0 {
		report.EmpathyConfidenceAvg = report.EmpathyConfidenceAvg / float64(report.EmpathyApplicableCount)
	}

	if err := fillLLMEventMetrics(db, &report); err != nil {
		return reportMetrics{}, err
	}

	sortConversationItems(report.RawRedConversations)
	sortConversationItems(report.FinalRedConversations)
	sortUtteranceItems(report.TopRawMismatches)
	sortUtteranceItems(report.TopFinalMismatches)
	sortUtteranceItems(report.TopEvidenceInvalid)
	sortUtteranceItems(report.TopShortUtterances)
	sortNotOKItems(report.NotOKItems)

	report.TopRawMismatches = trimUtteranceItems(report.TopRawMismatches, 10)
	report.TopFinalMismatches = trimUtteranceItems(report.TopFinalMismatches, 10)
	report.TopEvidenceInvalid = trimUtteranceItems(report.TopEvidenceInvalid, 10)
	report.TopShortUtterances = trimUtteranceItems(report.TopShortUtterances, 10)
	if len(report.NotOKItems) > 20 {
		report.NotOKItems = report.NotOKItems[:20]
	}

	return report, nil
}

func fillLLMEventMetrics(db *sql.DB, report *reportMetrics) error {
	if report == nil {
		return nil
	}
	var parseFailed int
	var validationFailed int
	var total int
	if err := db.QueryRow(`
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN parse_ok = 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN validation_ok = 0 THEN 1 ELSE 0 END), 0)
		FROM llm_events
	`).Scan(&total, &parseFailed, &validationFailed); err != nil {
		return fmt.Errorf("query llm event metrics: %w", err)
	}
	report.LLMEventCount = total
	report.LLMParseFailedCount = parseFailed
	report.LLMValidationFailedCount = validationFailed
	return nil
}

func sortConversationItems(items []conversationDebugItem) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].RedRows == items[j].RedRows {
			return items[i].ConversationID < items[j].ConversationID
		}
		return items[i].RedRows > items[j].RedRows
	})
}

func sortUtteranceItems(items []utteranceDebugItem) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].TextLength == items[j].TextLength {
			if items[i].ConversationID == items[j].ConversationID {
				return items[i].UtteranceIndex < items[j].UtteranceIndex
			}
			return items[i].ConversationID < items[j].ConversationID
		}
		return items[i].TextLength < items[j].TextLength
	})
}

func sortNotOKItems(items []empathyReviewItem) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].EmpathyConfidence == items[j].EmpathyConfidence {
			if items[i].ConversationID == items[j].ConversationID {
				return items[i].UtteranceIndex < items[j].UtteranceIndex
			}
			return items[i].ConversationID < items[j].ConversationID
		}
		return items[i].EmpathyConfidence > items[j].EmpathyConfidence
	})
}

func trimUtteranceItems(items []utteranceDebugItem, max int) []utteranceDebugItem {
	if len(items) <= max {
		return items
	}
	return items[:max]
}

func BuildAnalyticsMarkdown(dbPath string) (string, error) {
	report, err := BuildReport(dbPath)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString("# Analytics\n\n")
	b.WriteString("## Totals\n")
	b.WriteString(fmt.Sprintf("- total_rows: `%d`\n", report.TotalRows))
	b.WriteString(fmt.Sprintf("- total_conversations: `%d`\n\n", report.TotalConversations))

	b.WriteString("## Speaker Quality\n")
	b.WriteString(fmt.Sprintf("- speaker_accuracy_raw_percent: `%.2f%%` (`%d/%d`)\n", report.SpeakerAccuracyRawPercent, report.SpeakerCorrectRawCount, report.TotalRows))
	b.WriteString(fmt.Sprintf("- speaker_accuracy_final_percent: `%.2f%%` (`%d/%d`)\n", report.SpeakerAccuracyFinalPercent, report.SpeakerCorrectFinalCount, report.TotalRows))
	b.WriteString(fmt.Sprintf("- raw_mismatch_count: `%d`\n", report.RawMismatchCount))
	b.WriteString(fmt.Sprintf("- final_mismatch_count: `%d`\n", report.FinalMismatchCount))
	b.WriteString(fmt.Sprintf("- farewell_override_count: `%d`\n", report.FarewellOverrideCount))
	b.WriteString(fmt.Sprintf("- speaker_evidence_invalid_count: `%d`\n\n", report.SpeakerEvidenceInvalidCount))

	b.WriteString("## Empathy\n")
	b.WriteString(fmt.Sprintf("- empathy_applicable_rows: `%d`\n", report.EmpathyApplicableCount))
	b.WriteString(fmt.Sprintf("- empathy_confidence_avg: `%.4f`\n", report.EmpathyConfidenceAvg))
	b.WriteString(fmt.Sprintf("- empathy_confidence_min: `%.4f`\n", report.EmpathyConfidenceMin))
	b.WriteString(fmt.Sprintf("- empathy_confidence_max: `%.4f`\n", report.EmpathyConfidenceMax))
	b.WriteString(fmt.Sprintf("- empathy_review_pending_applicable: `%d`\n", report.EmpathyReviewPendingCount))
	b.WriteString(fmt.Sprintf("- empathy_review_ok: `%d`\n", report.EmpathyReviewOKCount))
	b.WriteString(fmt.Sprintf("- empathy_review_not_ok: `%d`\n\n", report.EmpathyReviewNotOKCount))

	b.WriteString("## LLM Events\n")
	b.WriteString(fmt.Sprintf("- llm_event_rows: `%d`\n", report.LLMEventCount))
	b.WriteString(fmt.Sprintf("- llm_parse_failed_count: `%d`\n", report.LLMParseFailedCount))
	b.WriteString(fmt.Sprintf("- llm_validation_failed_count: `%d`\n", report.LLMValidationFailedCount))
	return b.String(), nil
}

func BuildReleaseDebugMarkdown(dbPath string) (string, error) {
	report, err := BuildReport(dbPath)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString("# Release Debug\n\n")
	b.WriteString("## Summary\n")
	b.WriteString(fmt.Sprintf("- total_rows: `%d`\n", report.TotalRows))
	b.WriteString(fmt.Sprintf("- raw_mismatch_count: `%d`\n", report.RawMismatchCount))
	b.WriteString(fmt.Sprintf("- final_mismatch_count: `%d`\n", report.FinalMismatchCount))
	b.WriteString(fmt.Sprintf("- farewell_override_count: `%d`\n", report.FarewellOverrideCount))
	b.WriteString(fmt.Sprintf("- speaker_evidence_invalid_count: `%d`\n\n", report.SpeakerEvidenceInvalidCount))

	b.WriteString("## Red Conversations (Raw)\n")
	if len(report.RawRedConversations) == 0 {
		b.WriteString("- none\n\n")
	} else {
		b.WriteString("| conversation_id | raw_red_rows | total_rows | top_reason |\n")
		b.WriteString("| --- | ---: | ---: | --- |\n")
		for _, item := range report.RawRedConversations {
			b.WriteString(fmt.Sprintf("| `%s` | `%d` | `%d` | %s |\n", item.ConversationID, item.RedRows, item.TotalRows, item.TopReason))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Red Conversations (Final)\n")
	if len(report.FinalRedConversations) == 0 {
		b.WriteString("- none\n\n")
	} else {
		b.WriteString("| conversation_id | final_red_rows | total_rows | top_reason |\n")
		b.WriteString("| --- | ---: | ---: | --- |\n")
		for _, item := range report.FinalRedConversations {
			b.WriteString(fmt.Sprintf("| `%s` | `%d` | `%d` | %s |\n", item.ConversationID, item.RedRows, item.TotalRows, item.TopReason))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Top Raw Mismatches\n")
	writeUtteranceTable(&b, report.TopRawMismatches)
	b.WriteString("\n## Top Final Mismatches\n")
	writeUtteranceTable(&b, report.TopFinalMismatches)
	b.WriteString("\n## Top Evidence Invalid\n")
	writeUtteranceTable(&b, report.TopEvidenceInvalid)
	b.WriteString("\n## Top Short-Utterance Raw Mismatches\n")
	writeUtteranceTable(&b, report.TopShortUtterances)

	b.WriteString("\n## LLM Event Failures\n")
	b.WriteString(fmt.Sprintf("- parse_failed: `%d`\n", report.LLMParseFailedCount))
	b.WriteString(fmt.Sprintf("- validation_failed: `%d`\n", report.LLMValidationFailedCount))
	return b.String(), nil
}

func writeUtteranceTable(b *strings.Builder, items []utteranceDebugItem) {
	if len(items) == 0 {
		b.WriteString("- none\n")
		return
	}
	b.WriteString("| conversation_id | utterance_index | text_length | quality_decision | utterance_text |\n")
	b.WriteString("| --- | ---: | ---: | --- | --- |\n")
	for _, item := range items {
		b.WriteString(fmt.Sprintf(
			"| `%s` | `%d` | `%d` | `%s` | `%s` |\n",
			item.ConversationID,
			item.UtteranceIndex,
			item.TextLength,
			strings.ReplaceAll(item.SpeakerQualityDecision, "`", "'"),
			strings.ReplaceAll(item.UtteranceText, "`", "'"),
		))
	}
}

func FormatReport(r reportMetrics) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("total_rows=%d\n", r.TotalRows))
	b.WriteString(fmt.Sprintf("total_conversations=%d\n", r.TotalConversations))
	b.WriteString(fmt.Sprintf("speaker_accuracy_raw_percent=%.2f (%d/%d)\n", r.SpeakerAccuracyRawPercent, r.SpeakerCorrectRawCount, r.TotalRows))
	b.WriteString(fmt.Sprintf("speaker_accuracy_final_percent=%.2f (%d/%d)\n", r.SpeakerAccuracyFinalPercent, r.SpeakerCorrectFinalCount, r.TotalRows))
	b.WriteString(fmt.Sprintf("farewell_override_count=%d\n", r.FarewellOverrideCount))
	b.WriteString(fmt.Sprintf("speaker_evidence_invalid_count=%d\n", r.SpeakerEvidenceInvalidCount))
	b.WriteString(fmt.Sprintf("empathy_review_pending_applicable=%d\n", r.EmpathyReviewPendingCount))
	b.WriteString(fmt.Sprintf("empathy_review_ok=%d\n", r.EmpathyReviewOKCount))
	b.WriteString(fmt.Sprintf("empathy_review_not_ok=%d\n", r.EmpathyReviewNotOKCount))
	return b.String()
}

func PrintReport(r reportMetrics) {
	fmt.Print(FormatReport(r))
}
