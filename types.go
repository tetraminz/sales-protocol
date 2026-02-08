package main

import "strings"

const (
	defaultSQLitePath    = "out/annotations.db"
	defaultInputDir      = "sales-transcripts/data/chunked_transcripts"
	defaultAnnotateModel = "gpt-4.1-mini"
	defaultOpenAIBaseURL = "https://api.openai.com"

	speakerSalesRep = "Sales Rep"
	speakerCustomer = "Customer"

	qualityDecisionStrictMatch      = "strict_match"
	qualityDecisionStrictMismatch   = "strict_mismatch"
	qualityDecisionNoGroundTruth    = "no_ground_truth"
	qualityDecisionFarewellOverride = "farewell_context_override"

	farewellContextSourceCurrent  = "current"
	farewellContextSourcePrevious = "previous"
	farewellContextSourceNext     = "next"
	farewellContextSourceMixed    = "mixed"
	farewellContextSourceNone     = "none"

	reviewStatusPending       = "pending"
	reviewStatusOK            = "ok"
	reviewStatusNotOK         = "not_ok"
	reviewStatusNotApplicable = "not_applicable"

	llmUnitSpeaker = "speaker"
	llmUnitEmpathy = "empathy"

	maxLLMAttempts       = 2
	shortUtteranceMaxLen = 40
)

// AnnotateConfig описывает run annotate: откуда читать CSV и куда писать SQLite.
type AnnotateConfig struct {
	DBPath   string
	InputDir string
	FromIdx  int
	ToIdx    int
	Model    string
	APIKey   string
	BaseURL  string
}

// salesTurn — одна строка датасета (оригинальный turn/chunk).
type salesTurn struct {
	ConversationID string
	TurnID         int
	Speaker        string
	Text           string
}

// utteranceBlock — склейка соседних turn от одного говорящего.
type utteranceBlock struct {
	ConversationID     string
	UtteranceIndex     int
	GroundTruthSpeaker string
	UtteranceText      string
}

// ProcessInput — данные одного utterance block для SGR процесса.
type ProcessInput struct {
	UtteranceText      string
	PreviousText       string
	NextText           string
	GroundTruthSpeaker string
}

// SpeakerCaseInput — единственный допустимый input для speaker unit.
type SpeakerCaseInput struct {
	PreviousText string
	CurrentText  string
	NextText     string
}

// SpeakerCaseResult — "сырой" результат speaker unit до бизнес-решения SGR.
type SpeakerCaseResult struct {
	PredictedSpeaker              string
	PredictedSpeakerConfidence    float64
	FarewellIsCurrentUtterance    bool
	FarewellIsConversationClosing bool
	FarewellContextSource         string
	SpeakerEvidenceQuote          string
	SpeakerEvidenceIsValid        bool
}

// SpeakerDecision — итог speaker части после SGR quality decision.
type SpeakerDecision struct {
	PredictedSpeaker              string
	PredictedSpeakerConfidence    float64
	FarewellIsCurrentUtterance    bool
	FarewellIsConversationClosing bool
	FarewellContextSource         string
	SpeakerEvidenceQuote          string
	SpeakerEvidenceIsValid        bool
	SpeakerIsCorrectRaw           bool
	SpeakerIsCorrectFinal         bool
	SpeakerQualityDecision        string
}

// EmpathyCaseInput — единственный допустимый input для empathy unit.
type EmpathyCaseInput struct {
	CurrentText string
}

// EmpathyCaseResult — "сырой" результат empathy unit до routing на бизнес-уровне.
type EmpathyCaseResult struct {
	EmpathyPresent         bool
	EmpathyConfidence      float64
	EmpathyEvidenceQuote   string
	EmpathyEvidenceIsValid bool
}

// EmpathyDecision — итог routing по empathy с учетом применимости.
type EmpathyDecision struct {
	EmpathyApplicable      bool
	EmpathyPresent         bool
	EmpathyConfidence      float64
	EmpathyEvidenceQuote   string
	EmpathyEvidenceIsValid bool
}

// ProcessOutput — результат полного SGR шага для одного utterance block.
type ProcessOutput struct {
	Speaker SpeakerDecision
	Empathy EmpathyDecision
}

// AnnotationRow — одна строка таблицы annotations.
type AnnotationRow struct {
	ConversationID                string
	UtteranceIndex                int
	UtteranceText                 string
	GroundTruthSpeaker            string
	PredictedSpeaker              string
	PredictedSpeakerConfidence    float64
	SpeakerIsCorrectRaw           bool
	SpeakerIsCorrectFinal         bool
	SpeakerQualityDecision        string
	FarewellIsCurrentUtterance    bool
	FarewellIsConversationClosing bool
	FarewellContextSource         string
	SpeakerEvidenceQuote          string
	SpeakerEvidenceIsValid        bool
	EmpathyApplicable             bool
	EmpathyPresent                bool
	EmpathyConfidence             float64
	EmpathyEvidenceQuote          string
	EmpathyReviewStatus           string
	EmpathyReviewerNote           string
	Model                         string
	AnnotatedAtUTC                string
}

// LLMEvent — полный аудит одной попытки LLM вызова.
type LLMEvent struct {
	CreatedAtUTC         string
	ConversationID       string
	UtteranceIndex       int
	UnitName             string
	Attempt              int
	Model                string
	RequestJSON          string
	ResponseHTTPStatus   int
	ResponseJSON         string
	ExtractedContentJSON string
	ParseOK              bool
	ValidationOK         bool
	ErrorMessage         string
}

// LLMCallResult — технический результат HTTP вызова к OpenAI strict JSON endpoint.
type LLMCallResult struct {
	RequestJSON          string
	ResponseJSON         string
	HTTPStatus           int
	ExtractedContentJSON string
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

func normalizeFarewellContextSource(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	switch s {
	case farewellContextSourceCurrent:
		return farewellContextSourceCurrent
	case farewellContextSourcePrevious:
		return farewellContextSourcePrevious
	case farewellContextSourceNext:
		return farewellContextSourceNext
	case farewellContextSourceMixed:
		return farewellContextSourceMixed
	default:
		return farewellContextSourceNone
	}
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
