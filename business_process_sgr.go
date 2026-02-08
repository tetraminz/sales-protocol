package main

import (
	"context"
	"fmt"
)

const speakerSalesRep = "Sales Rep"

// AnnotationBusinessProcess is the business-level SGR documentation in code.
//
// CASCADE:
// 1) classify speaker for the replica
// 2) if needed, run empathy analysis
//
// ROUTING:
// empathy case runs only for Sales Rep replicas
//
// CYCLE:
// retry strategy lives inside case implementations; process only orchestrates cases
// and keeps control flow explicit.
//
// This file intentionally has no SQL, files, HTTP, or JSONL parsing.

type ProcessInput struct {
	ReplicaText string
	PrevText    string
	NextText    string
	SpeakerTrue string
}

type ProcessOutput struct {
	Speaker ReplicaCaseResult
	Empathy EmpathyCaseResult
}

type ReplicaCaseInput struct {
	ReplicaText string
	PrevText    string
	NextText    string
}

type ReplicaCaseResult struct {
	PredictedSpeaker string
	Confidence       float64
	EvidenceQuote    string
	QualityMismatch  bool
}

type EmpathyCaseInput struct {
	ReplicaText string
	SpeakerTrue string
}

type EmpathyCaseResult struct {
	Ran            bool
	EmpathyPresent bool
	EmpathyType    string
	Confidence     float64
	EvidenceQuote  string
}

type ReplicaSpeakerCase interface {
	Evaluate(ctx context.Context, in ReplicaCaseInput) (ReplicaCaseResult, error)
}

type EmpathyCase interface {
	Evaluate(ctx context.Context, in EmpathyCaseInput) (EmpathyCaseResult, error)
}

type AnnotationBusinessProcess struct {
	SpeakerCase ReplicaSpeakerCase
	EmpathyCase EmpathyCase
}

func (p AnnotationBusinessProcess) Run(ctx context.Context, in ProcessInput) (ProcessOutput, error) {
	if p.SpeakerCase == nil {
		return ProcessOutput{}, fmt.Errorf("speaker case is required")
	}

	speaker, err := p.SpeakerCase.Evaluate(ctx, ReplicaCaseInput{
		ReplicaText: in.ReplicaText,
		PrevText:    in.PrevText,
		NextText:    in.NextText,
	})
	if err != nil {
		return ProcessOutput{}, fmt.Errorf("speaker case: %w", err)
	}

	if in.SpeakerTrue != "" && speaker.PredictedSpeaker != "" {
		speaker.QualityMismatch = speaker.PredictedSpeaker != in.SpeakerTrue
	}

	// ROUTING: skip empathy for non-sales-rep replicas.
	empathy := EmpathyCaseResult{
		Ran:            false,
		EmpathyPresent: false,
		EmpathyType:    "none",
		Confidence:     0,
		EvidenceQuote:  "",
	}

	if in.SpeakerTrue == speakerSalesRep {
		if p.EmpathyCase == nil {
			return ProcessOutput{}, fmt.Errorf("empathy case is required for sales rep replicas")
		}
		empathy, err = p.EmpathyCase.Evaluate(ctx, EmpathyCaseInput{
			ReplicaText: in.ReplicaText,
			SpeakerTrue: in.SpeakerTrue,
		})
		if err != nil {
			return ProcessOutput{}, fmt.Errorf("empathy case: %w", err)
		}
	}

	return ProcessOutput{
		Speaker: speaker,
		Empathy: empathy,
	}, nil
}
