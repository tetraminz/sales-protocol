package main

import "encoding/json"

const (
	schemaVersion   = "replica_annotation_v1"
	datasetName     = "gwenshap/sales-transcripts"
	defaultBaseURL  = "https://api.openai.com"
	speakerSalesRep = "Sales Rep"
	speakerCustomer = "Customer"
)

type Config struct {
	InputDir           string
	OutJSONL           string
	LimitConversations int
	Model              string
	MaxRetries         int
	DryRun             bool
	APIKey             string
	BaseURL            string
}

type Turn struct {
	ConversationID string
	TurnID         int
	Speaker        string
	Text           string
}

type Replica struct {
	ConversationID string
	ReplicaID      int
	TurnIDs        []int
	SpeakerTrue    string
	Text           string
	Turns          []Turn
}

type ReplicaTurnOut struct {
	TurnID  int    `json:"turn_id"`
	Speaker string `json:"speaker"`
	Text    string `json:"text"`
}

type AnnotationRecord struct {
	SchemaVersion  string           `json:"schema_version"`
	Dataset        string           `json:"dataset"`
	ConversationID string           `json:"conversation_id"`
	ReplicaID      int              `json:"replica_id"`
	TurnIDs        []int            `json:"turn_ids"`
	SpeakerTrue    string           `json:"speaker_true"`
	ReplicaText    string           `json:"replica_text"`
	ReplicaTurns   []ReplicaTurnOut `json:"replica_turns"`
	Guided         GuidedBlock      `json:"guided"`
	Meta           MetaBlock        `json:"meta"`
}

type GuidedBlock struct {
	UnitSpeaker UnitSpeakerResult `json:"unit_speaker"`
	UnitEmpathy UnitEmpathyResult `json:"unit_empathy"`
}

type UnitSpeakerResult struct {
	OK               bool            `json:"ok"`
	Attempts         int             `json:"attempts"`
	ValidationErrors []string        `json:"validation_errors"`
	Output           json.RawMessage `json:"output"`
}

type UnitEmpathyResult struct {
	Ran              bool            `json:"ran"`
	OK               bool            `json:"ok"`
	Attempts         int             `json:"attempts"`
	ValidationErrors []string        `json:"validation_errors"`
	Output           json.RawMessage `json:"output"`
}

type MetaBlock struct {
	Model            string   `json:"model"`
	TimestampUTC     string   `json:"timestamp_utc"`
	OpenAIRequestIDs []string `json:"openai_request_ids"`
}

type SpeakerAttributionOutput struct {
	PredictedSpeaker string  `json:"predicted_speaker"`
	Confidence       float64 `json:"confidence"`
	Evidence         struct {
		Quote string `json:"quote"`
	} `json:"evidence"`
}

type EvidenceQuote struct {
	Quote string `json:"quote"`
}

type EmpathyDetectionOutput struct {
	EmpathyPresent bool            `json:"empathy_present"`
	EmpathyType    string          `json:"empathy_type"`
	Confidence     float64         `json:"confidence"`
	Evidence       []EvidenceQuote `json:"evidence"`
}
