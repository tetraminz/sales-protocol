package main

import (
	"context"
	"fmt"
	"strings"
)

// AnnotationBusinessProcess — главная SGR-спецификация процесса в коде.
//
// Термины:
// - Conversation: один диалог (обычно один CSV файл)
// - Turn: исходная строка датасета
// - Utterance Block: несколько подряд turn от одного говорящего, склеенные в один блок
//
// CASCADE (пошаговая логика):
// 1) Speaker unit определяет predicted speaker и признаки farewell-контекста.
// 2) SGR quality decision считает raw/final корректность.
// 3) ROUTING empathy: empathy применяется только для ground truth = Sales Rep.
//
// ROUTING:
// - Empathy unit не вызывается для Customer строк.
//
// CYCLE:
// - Retry находится внутри конкретных LLM unit (speaker/empathy).
//
// Важный инвариант:
// - predicted speaker НИКОГДА не переписываем на бизнес-уровне.
// - SGR меняет только оценку качества (speaker_is_correct_final).
// Это позволяет прозрачно видеть, где LLM ошиблась raw, но бизнес осознанно снял красноту
// для прощального closing-контекста.
type AnnotationBusinessProcess struct {
	SpeakerUnit SpeakerUnit
	EmpathyUnit EmpathyUnit
}

type SpeakerUnit interface {
	Evaluate(ctx context.Context, in SpeakerCaseInput) (SpeakerCaseResult, error)
}

type EmpathyUnit interface {
	Evaluate(ctx context.Context, in EmpathyCaseInput) (EmpathyCaseResult, error)
}

func (p AnnotationBusinessProcess) Run(ctx context.Context, in ProcessInput) (ProcessOutput, error) {
	if p.SpeakerUnit == nil {
		return ProcessOutput{}, fmt.Errorf("speaker unit is required")
	}

	// Step A: speaker unit (LLM) работает только по текстовому контексту.
	speakerRaw, err := p.SpeakerUnit.Evaluate(ctx, SpeakerCaseInput{
		PreviousText: in.PreviousText,
		CurrentText:  in.UtteranceText,
		NextText:     in.NextText,
	})
	if err != nil {
		return ProcessOutput{}, fmt.Errorf("speaker unit: %w", err)
	}

	// Step B: бизнес-решение качества (raw vs final).
	rawCorrect, finalCorrect, qualityDecision := decideSpeakerQuality(
		in.GroundTruthSpeaker,
		speakerRaw.PredictedSpeaker,
		speakerRaw.FarewellIsConversationClosing,
	)

	speakerDecision := SpeakerDecision{
		PredictedSpeaker:              speakerRaw.PredictedSpeaker,
		PredictedSpeakerConfidence:    clamp01(speakerRaw.PredictedSpeakerConfidence),
		FarewellIsCurrentUtterance:    speakerRaw.FarewellIsCurrentUtterance,
		FarewellIsConversationClosing: speakerRaw.FarewellIsConversationClosing,
		FarewellContextSource:         normalizeFarewellContextSource(speakerRaw.FarewellContextSource),
		SpeakerEvidenceQuote:          strings.TrimSpace(speakerRaw.SpeakerEvidenceQuote),
		SpeakerEvidenceIsValid:        speakerRaw.SpeakerEvidenceIsValid,
		SpeakerIsCorrectRaw:           rawCorrect,
		SpeakerIsCorrectFinal:         finalCorrect,
		SpeakerQualityDecision:        qualityDecision,
	}

	// Step C: routing empathy (только для Sales Rep по ground truth).
	empathyDecision := EmpathyDecision{
		EmpathyApplicable:      false,
		EmpathyPresent:         false,
		EmpathyConfidence:      0,
		EmpathyEvidenceQuote:   "",
		EmpathyEvidenceIsValid: false,
	}

	if canonicalSpeakerLabel(in.GroundTruthSpeaker) == speakerSalesRep {
		if p.EmpathyUnit == nil {
			return ProcessOutput{}, fmt.Errorf("empathy unit is required for sales-rep utterance")
		}
		empathyRaw, err := p.EmpathyUnit.Evaluate(ctx, EmpathyCaseInput{CurrentText: in.UtteranceText})
		if err != nil {
			return ProcessOutput{}, fmt.Errorf("empathy unit: %w", err)
		}
		empathyDecision = EmpathyDecision{
			EmpathyApplicable:      true,
			EmpathyPresent:         empathyRaw.EmpathyPresent,
			EmpathyConfidence:      clamp01(empathyRaw.EmpathyConfidence),
			EmpathyEvidenceQuote:   strings.TrimSpace(empathyRaw.EmpathyEvidenceQuote),
			EmpathyEvidenceIsValid: empathyRaw.EmpathyEvidenceIsValid,
		}
	}

	return ProcessOutput{
		Speaker: speakerDecision,
		Empathy: empathyDecision,
	}, nil
}

func decideSpeakerQuality(groundTruthSpeaker, predictedSpeaker string, farewellClosing bool) (bool, bool, string) {
	gt := strings.TrimSpace(groundTruthSpeaker)
	pred := strings.TrimSpace(predictedSpeaker)
	if gt == "" || pred == "" {
		return false, false, qualityDecisionNoGroundTruth
	}

	rawCorrect := pred == gt
	if rawCorrect {
		return true, true, qualityDecisionStrictMatch
	}

	if farewellClosing {
		// Смысл override: raw ошибка сохраняется как факт, но final бизнес-метрика зелёная.
		return false, true, qualityDecisionFarewellOverride
	}
	return false, false, qualityDecisionStrictMismatch
}
