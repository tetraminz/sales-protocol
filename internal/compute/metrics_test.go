package compute

import (
	"testing"

	"github.com/tetraminz/sales_protocol/internal/dataset"
)

func TestComputeMetrics(t *testing.T) {
	t.Parallel()

	turns := []dataset.Turn{
		{TurnID: 0, Speaker: "Sales Rep", Text: "Hi, do you need a jacket? We have a 10% discount."},
		{TurnID: 1, Speaker: "Customer", Text: "I care about reviews and fit. What is your return policy?"},
		{TurnID: 2, Speaker: "Sales Rep", Text: "We offer free shipping and easy exchange. Any other questions?"},
	}

	got := ComputeMetrics(turns)

	if got.TurnCountTotal != 3 {
		t.Fatalf("TurnCountTotal got %d want %d", got.TurnCountTotal, 3)
	}
	if got.TurnCountSalesRep != 2 {
		t.Fatalf("TurnCountSalesRep got %d want %d", got.TurnCountSalesRep, 2)
	}
	if got.TurnCountCustomer != 1 {
		t.Fatalf("TurnCountCustomer got %d want %d", got.TurnCountCustomer, 1)
	}
	if got.QuestionMarksSalesRep != 2 {
		t.Fatalf("QuestionMarksSalesRep got %d want %d", got.QuestionMarksSalesRep, 2)
	}
	if got.QuestionMarksCustomer != 1 {
		t.Fatalf("QuestionMarksCustomer got %d want %d", got.QuestionMarksCustomer, 1)
	}
	if !got.MentionsDiscount {
		t.Fatalf("MentionsDiscount got false want true")
	}
	if !got.MentionsReturnPolicy {
		t.Fatalf("MentionsReturnPolicy got false want true")
	}
	if !got.MentionsShipping {
		t.Fatalf("MentionsShipping got false want true")
	}
	if !got.MentionsReviews {
		t.Fatalf("MentionsReviews got false want true")
	}
	if !got.MentionsSizing {
		t.Fatalf("MentionsSizing got false want true")
	}
}
