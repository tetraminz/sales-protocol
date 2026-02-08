package main

import (
	"fmt"
	"sort"
	"strings"
)

type reportMetrics struct {
	TotalRows          int
	TotalConversations int

	SpeakerMatchCount      int
	SpeakerAccuracyPercent float64
	GreenReplicaCount      int
	RedReplicaCount        int
	GreenReplicaPercent    float64
	RedReplicaPercent      float64
	GreenConversationCount int
	RedConversationCount   int
	GreenConversationPct   float64
	RedConversationPct     float64

	EmpathyConfidenceAvg      float64
	EmpathyConfidenceMin      float64
	EmpathyConfidenceMax      float64
	EmpathyConfidenceGTE70    int
	EmpathyConfidenceRowCount int

	ReviewPendingCount int
	ReviewOKCount      int
	ReviewNotOKCount   int

	RedConversations         []conversationDebugItem
	ShortUtteranceMismatches []shortUtteranceMismatch
	NotOKItems               []empathyReviewItem
}

type conversationDebugItem struct {
	ConversationID string
	RedReplicas    int
	TotalReplicas  int
	TopReason      string
}

type shortUtteranceMismatch struct {
	ConversationID string
	ReplicaID      int
	ReplicaText    string
	TextLength     int
}

type empathyReviewItem struct {
	ConversationID      string
	ReplicaID           int
	EmpathyConfidence   float64
	EmpathyReviewerNote string
}

func BuildReport(dbPath string) (reportMetrics, error) {
	db, err := openSQLite(dbPath)
	if err != nil {
		return reportMetrics{}, err
	}
	defer db.Close()
	if err := ensureAnnotationsSchema(db); err != nil {
		return reportMetrics{}, err
	}

	rows, err := db.Query(`
		SELECT
			conversation_id,
			replica_id,
			speaker_match,
			speaker_true,
			empathy_confidence,
			empathy_review_status,
			empathy_reviewer_note,
			replica_text
		FROM annotations
	`)
	if err != nil {
		return reportMetrics{}, fmt.Errorf("query annotations: %w", err)
	}
	defer rows.Close()

	type convState struct {
		Total int
		Red   int
	}
	conversations := map[string]*convState{}

	report := reportMetrics{}
	hasEmpathy := false
	for rows.Next() {
		var conversationID string
		var replicaID int
		var speakerMatch int
		var speakerTrue string
		var empathyConfidence float64
		var reviewStatus string
		var reviewerNote string
		var replicaText string
		if err := rows.Scan(
			&conversationID,
			&replicaID,
			&speakerMatch,
			&speakerTrue,
			&empathyConfidence,
			&reviewStatus,
			&reviewerNote,
			&replicaText,
		); err != nil {
			return reportMetrics{}, fmt.Errorf("scan annotation row: %w", err)
		}

		report.TotalRows++
		if speakerMatch == 1 {
			report.SpeakerMatchCount++
			report.GreenReplicaCount++
		} else {
			report.RedReplicaCount++
			replicaText = strings.TrimSpace(replicaText)
			textLen := len([]rune(replicaText))
			if textLen <= shortUtteranceMaxLen {
				report.ShortUtteranceMismatches = append(report.ShortUtteranceMismatches, shortUtteranceMismatch{
					ConversationID: conversationID,
					ReplicaID:      replicaID,
					ReplicaText:    replicaText,
					TextLength:     textLen,
				})
			}
		}

		s := conversations[conversationID]
		if s == nil {
			s = &convState{}
			conversations[conversationID] = s
		}
		s.Total++
		if speakerMatch == 0 {
			s.Red++
		}

		if canonicalSpeakerLabel(speakerTrue) == speakerSalesRep {
			hasEmpathy = true
			report.EmpathyConfidenceRowCount++
			if report.EmpathyConfidenceRowCount == 1 {
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
			if empathyConfidence >= 0.70 {
				report.EmpathyConfidenceGTE70++
			}
		}

		switch strings.TrimSpace(reviewStatus) {
		case reviewStatusOK:
			report.ReviewOKCount++
		case reviewStatusNotOK:
			report.ReviewNotOKCount++
			report.NotOKItems = append(report.NotOKItems, empathyReviewItem{
				ConversationID:      conversationID,
				ReplicaID:           replicaID,
				EmpathyConfidence:   empathyConfidence,
				EmpathyReviewerNote: strings.TrimSpace(reviewerNote),
			})
		default:
			report.ReviewPendingCount++
		}
	}
	if err := rows.Err(); err != nil {
		return reportMetrics{}, fmt.Errorf("iterate annotation rows: %w", err)
	}

	report.TotalConversations = len(conversations)
	for conversationID, s := range conversations {
		if s.Red == 0 {
			report.GreenConversationCount++
		} else {
			report.RedConversationCount++
			report.RedConversations = append(report.RedConversations, conversationDebugItem{
				ConversationID: conversationID,
				RedReplicas:    s.Red,
				TotalReplicas:  s.Total,
				TopReason:      fmt.Sprintf("speaker_mismatch (%d)", s.Red),
			})
		}
	}

	if report.TotalRows > 0 {
		report.SpeakerAccuracyPercent = 100.0 * float64(report.SpeakerMatchCount) / float64(report.TotalRows)
		report.GreenReplicaPercent = 100.0 * float64(report.GreenReplicaCount) / float64(report.TotalRows)
		report.RedReplicaPercent = 100.0 * float64(report.RedReplicaCount) / float64(report.TotalRows)
	}
	if report.TotalConversations > 0 {
		report.GreenConversationPct = 100.0 * float64(report.GreenConversationCount) / float64(report.TotalConversations)
		report.RedConversationPct = 100.0 * float64(report.RedConversationCount) / float64(report.TotalConversations)
	}
	if hasEmpathy && report.EmpathyConfidenceRowCount > 0 {
		report.EmpathyConfidenceAvg = report.EmpathyConfidenceAvg / float64(report.EmpathyConfidenceRowCount)
	}

	sort.Slice(report.RedConversations, func(i, j int) bool {
		if report.RedConversations[i].RedReplicas == report.RedConversations[j].RedReplicas {
			return report.RedConversations[i].ConversationID < report.RedConversations[j].ConversationID
		}
		return report.RedConversations[i].RedReplicas > report.RedConversations[j].RedReplicas
	})
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
	sort.Slice(report.NotOKItems, func(i, j int) bool {
		if report.NotOKItems[i].EmpathyConfidence == report.NotOKItems[j].EmpathyConfidence {
			if report.NotOKItems[i].ConversationID == report.NotOKItems[j].ConversationID {
				return report.NotOKItems[i].ReplicaID < report.NotOKItems[j].ReplicaID
			}
			return report.NotOKItems[i].ConversationID < report.NotOKItems[j].ConversationID
		}
		return report.NotOKItems[i].EmpathyConfidence > report.NotOKItems[j].EmpathyConfidence
	})
	if len(report.NotOKItems) > 20 {
		report.NotOKItems = report.NotOKItems[:20]
	}

	return report, nil
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
	b.WriteString(fmt.Sprintf("- total_conversations: `%d`\n", report.TotalConversations))
	b.WriteString(fmt.Sprintf("- green_replicas: `%d` (%.2f%%)\n", report.GreenReplicaCount, report.GreenReplicaPercent))
	b.WriteString(fmt.Sprintf("- red_replicas: `%d` (%.2f%%)\n\n", report.RedReplicaCount, report.RedReplicaPercent))

	b.WriteString("## Speaker Accuracy\n")
	b.WriteString(fmt.Sprintf("- speaker_accuracy_percent: `%.2f%%` (`%d/%d`)\n\n", report.SpeakerAccuracyPercent, report.SpeakerMatchCount, report.TotalRows))

	b.WriteString("## Empathy Confidence\n")
	b.WriteString(fmt.Sprintf("- sales_rep_rows: `%d`\n", report.EmpathyConfidenceRowCount))
	b.WriteString(fmt.Sprintf("- avg_confidence: `%.4f`\n", report.EmpathyConfidenceAvg))
	b.WriteString(fmt.Sprintf("- min_confidence: `%.4f`\n", report.EmpathyConfidenceMin))
	b.WriteString(fmt.Sprintf("- max_confidence: `%.4f`\n", report.EmpathyConfidenceMax))
	b.WriteString(fmt.Sprintf("- confidence_gte_0_70_count: `%d`\n\n", report.EmpathyConfidenceGTE70))

	b.WriteString("## Manual Review\n")
	b.WriteString(fmt.Sprintf("- pending: `%d`\n", report.ReviewPendingCount))
	b.WriteString(fmt.Sprintf("- ok: `%d`\n", report.ReviewOKCount))
	b.WriteString(fmt.Sprintf("- not_ok: `%d`\n\n", report.ReviewNotOKCount))

	b.WriteString("## Short-Utterance Speaker Mismatches\n")
	if len(report.ShortUtteranceMismatches) == 0 {
		b.WriteString("- none\n")
	} else {
		for _, item := range report.ShortUtteranceMismatches {
			b.WriteString(fmt.Sprintf("- `%s` / replica `%d` / len `%d`: `%s`\n",
				item.ConversationID,
				item.ReplicaID,
				item.TextLength,
				item.ReplicaText,
			))
		}
	}
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
	b.WriteString(fmt.Sprintf("- green replicas: `%d` (%.2f%%)\n", report.GreenReplicaCount, report.GreenReplicaPercent))
	b.WriteString(fmt.Sprintf("- red replicas: `%d` (%.2f%%)\n", report.RedReplicaCount, report.RedReplicaPercent))
	b.WriteString(fmt.Sprintf("- green conversations: `%d` (%.2f%%)\n", report.GreenConversationCount, report.GreenConversationPct))
	b.WriteString(fmt.Sprintf("- red conversations: `%d` (%.2f%%)\n\n", report.RedConversationCount, report.RedConversationPct))

	b.WriteString("## Red Conversations\n")
	if len(report.RedConversations) == 0 {
		b.WriteString("- none\n\n")
	} else {
		b.WriteString("| conversation_id | red_replicas | total_replicas | top_reason |\n")
		b.WriteString("| --- | ---: | ---: | --- |\n")
		for _, item := range report.RedConversations {
			b.WriteString(fmt.Sprintf("| `%s` | `%d` | `%d` | %s |\n", item.ConversationID, item.RedReplicas, item.TotalReplicas, item.TopReason))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Empathy Review Backlog\n")
	b.WriteString(fmt.Sprintf("- pending_count: `%d`\n", report.ReviewPendingCount))
	b.WriteString(fmt.Sprintf("- not_ok_count: `%d`\n\n", report.ReviewNotOKCount))

	b.WriteString("## Not-OK Empathy Rows\n")
	if len(report.NotOKItems) == 0 {
		b.WriteString("- none\n\n")
	} else {
		b.WriteString("| conversation_id | replica_id | empathy_confidence | reviewer_note |\n")
		b.WriteString("| --- | ---: | ---: | --- |\n")
		for _, item := range report.NotOKItems {
			b.WriteString(fmt.Sprintf("| `%s` | `%d` | `%.4f` | `%s` |\n",
				item.ConversationID,
				item.ReplicaID,
				item.EmpathyConfidence,
				strings.ReplaceAll(item.EmpathyReviewerNote, "`", "'"),
			))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Top Short-Utterance Mismatches\n")
	if len(report.ShortUtteranceMismatches) == 0 {
		b.WriteString("- none\n")
	} else {
		b.WriteString("| conversation_id | replica_id | text_length | replica_text |\n")
		b.WriteString("| --- | ---: | ---: | --- |\n")
		for _, item := range report.ShortUtteranceMismatches {
			b.WriteString(fmt.Sprintf("| `%s` | `%d` | `%d` | `%s` |\n",
				item.ConversationID,
				item.ReplicaID,
				item.TextLength,
				strings.ReplaceAll(item.ReplicaText, "`", "'"),
			))
		}
	}

	return b.String(), nil
}

func FormatReport(r reportMetrics) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("total_rows=%d\n", r.TotalRows))
	b.WriteString(fmt.Sprintf("total_conversations=%d\n", r.TotalConversations))
	b.WriteString(fmt.Sprintf("speaker_accuracy_percent=%.2f (%d/%d)\n", r.SpeakerAccuracyPercent, r.SpeakerMatchCount, r.TotalRows))
	b.WriteString(fmt.Sprintf("green_replicas=%d (%.2f%%)\n", r.GreenReplicaCount, r.GreenReplicaPercent))
	b.WriteString(fmt.Sprintf("red_replicas=%d (%.2f%%)\n", r.RedReplicaCount, r.RedReplicaPercent))
	b.WriteString(fmt.Sprintf("empathy_confidence_avg=%.4f\n", r.EmpathyConfidenceAvg))
	b.WriteString(fmt.Sprintf("empathy_review_pending=%d\n", r.ReviewPendingCount))
	b.WriteString(fmt.Sprintf("empathy_review_ok=%d\n", r.ReviewOKCount))
	b.WriteString(fmt.Sprintf("empathy_review_not_ok=%d\n", r.ReviewNotOKCount))
	return b.String()
}

func PrintReport(r reportMetrics) {
	fmt.Print(FormatReport(r))
}
