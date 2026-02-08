package compute

import (
	"strings"

	"github.com/tetraminz/sales_protocol/internal/dataset"
)

// Metrics are deterministic values computed directly from turns.
type Metrics struct {
	TurnCountTotal        int  `json:"turn_count_total"`
	TurnCountSalesRep     int  `json:"turn_count_sales_rep"`
	TurnCountCustomer     int  `json:"turn_count_customer"`
	QuestionMarksSalesRep int  `json:"question_marks_sales_rep"`
	QuestionMarksCustomer int  `json:"question_marks_customer"`
	MentionsDiscount      bool `json:"mentions_discount"`
	MentionsReturnPolicy  bool `json:"mentions_return_policy"`
	MentionsShipping      bool `json:"mentions_shipping"`
	MentionsReviews       bool `json:"mentions_reviews"`
	MentionsSizing        bool `json:"mentions_sizing"`
}

// ComputeMetrics derives deterministic metrics from a conversation.
func ComputeMetrics(turns []dataset.Turn) Metrics {
	var metrics Metrics
	metrics.TurnCountTotal = len(turns)

	loweredLines := make([]string, 0, len(turns))
	for _, turn := range turns {
		speaker := strings.ToLower(strings.TrimSpace(turn.Speaker))
		questionMarks := strings.Count(turn.Text, "?")

		switch speaker {
		case "sales rep":
			metrics.TurnCountSalesRep++
			metrics.QuestionMarksSalesRep += questionMarks
		case "customer":
			metrics.TurnCountCustomer++
			metrics.QuestionMarksCustomer += questionMarks
		}

		loweredLines = append(loweredLines, strings.ToLower(turn.Text))
	}

	allText := strings.Join(loweredLines, " ")
	metrics.MentionsDiscount = containsAny(allText, "discount", "%", " off", "off ")
	metrics.MentionsReturnPolicy = containsAny(allText, "return", "exchange", "refund")
	metrics.MentionsShipping = containsAny(allText, "shipping", "delivery", "business days")
	metrics.MentionsReviews = containsAny(allText, "review", "reviews", "ratings", "testimonials")
	metrics.MentionsSizing = containsAny(allText, "size", "sizing chart", "fit", "measurements")

	return metrics
}

func containsAny(text string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}
