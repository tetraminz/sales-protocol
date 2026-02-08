package main

import (
	"context"
	"fmt"
	"strings"
)

const speakerSalesRep = "Sales Rep"

const (
	qualityDecisionStrictMatch      = "strict_match"
	qualityDecisionStrictMismatch   = "strict_mismatch"
	qualityDecisionNoGroundTruth    = "no_ground_truth"
	qualityDecisionFarewellOverride = "farewell_context_override"
)

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
// This file intentionally has no SQL, files, HTTP, or storage parsing.

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
	PredictedSpeaker      string
	Confidence            float64
	EvidenceQuote         string
	FarewellUtterance     bool
	FarewellContext       bool
	FarewellContextSource string
	QualityMismatch       bool
	QualityDecision       string
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

	// CASCADE / Шаг 1:
	// Получаем "сырой" результат классификации говорящего и сигналы про контекст прощания.
	speaker, err := p.SpeakerCase.Evaluate(ctx, ReplicaCaseInput{
		ReplicaText: in.ReplicaText,
		PrevText:    in.PrevText,
		NextText:    in.NextText,
	})
	if err != nil {
		return ProcessOutput{}, fmt.Errorf("speaker case: %w", err)
	}

	// CASCADE / Шаг 2 (отдельное бизнес-решение качества):
	// 1) Считаем raw mismatch только как сравнение true/predicted.
	// 2) ROUTING: если это mismatch, но LLM пометил "контекст прощания",
	//    снимаем mismatch на уровне quality.
	//
	// Важно: predicted speaker НЕ переписываем. Это осознанное решение:
	// - сохраняем оригинальный вывод speaker-case для аудита и отладки;
	// - меняем только бизнес-оценку качества, чтобы не краснить финальные прощания.
	speaker.QualityMismatch, speaker.QualityDecision = decideSpeakerQuality(
		in.SpeakerTrue,
		speaker.PredictedSpeaker,
		speaker.FarewellContext,
	)

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

func decideSpeakerQuality(speakerTrue, speakerPredicted string, farewellContext bool) (bool, string) {
	speakerTrue = strings.TrimSpace(speakerTrue)
	speakerPredicted = strings.TrimSpace(speakerPredicted)
	if speakerTrue == "" || speakerPredicted == "" {
		// Нет одной из опорных меток -> mismatch не считаем, чтобы не вносить шум в quality.
		return false, qualityDecisionNoGroundTruth
	}

	rawMismatch := speakerPredicted != speakerTrue
	if !rawMismatch {
		return false, qualityDecisionStrictMatch
	}

	if farewellContext {
		// Узкий целевой кейс:
		// реплика попала в финальный "прощальный" обмен, поэтому raw mismatch
		// не считаем качественной ошибкой speaker-модели.
		return false, qualityDecisionFarewellOverride
	}

	return true, qualityDecisionStrictMismatch
}
