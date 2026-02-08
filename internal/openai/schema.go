package openai

import "encoding/json"

const llmAnnotationSchemaName = "sales_transcript_llm_annotation_v1"

const llmAnnotationSchemaJSON = `{
  "type": "object",
  "additionalProperties": false,
  "required": [
    "summary",
    "participants",
    "scorecard",
    "signals",
    "quality_checks"
  ],
  "properties": {
    "summary": {
      "type": "object",
      "additionalProperties": false,
      "required": ["product_or_service", "one_sentence"],
      "properties": {
        "product_or_service": { "type": "string" },
        "one_sentence": { "type": "string" }
      }
    },
    "participants": {
      "type": "object",
      "additionalProperties": false,
      "required": ["company", "sales_rep_name", "customer_name"],
      "properties": {
        "company": { "type": "string" },
        "sales_rep_name": { "type": "string" },
        "customer_name": { "type": "string" }
      }
    },
    "scorecard": {
      "type": "object",
      "additionalProperties": false,
      "required": ["opening", "discovery", "objection_handling", "closing", "overall"],
      "properties": {
        "opening": { "enum": [0, 1, 2] },
        "discovery": { "enum": [0, 1, 2] },
        "objection_handling": { "enum": [0, 1, 2] },
        "closing": { "enum": [0, 1, 2] },
        "overall": { "enum": [0, 1, 2] }
      }
    },
    "signals": {
      "type": "object",
      "additionalProperties": false,
      "required": ["greeting", "needs", "objections", "upsell", "next_step", "tone"],
      "properties": {
        "greeting": {
          "type": "object",
          "additionalProperties": false,
          "required": [
            "present",
            "quality",
            "introduced_self",
            "introduced_company",
            "asked_how_are_you",
            "evidence"
          ],
          "properties": {
            "present": { "type": "boolean" },
            "quality": { "enum": ["none", "basic", "good", "excellent"] },
            "introduced_self": { "type": "boolean" },
            "introduced_company": { "type": "boolean" },
            "asked_how_are_you": { "type": "boolean" },
            "evidence": {
              "type": "array",
              "items": { "$ref": "#/$defs/evidence_snippet" }
            }
          }
        },
        "needs": {
          "type": "object",
          "additionalProperties": false,
          "required": ["stated_need", "need_categories", "evidence"],
          "properties": {
            "stated_need": { "type": "string" },
            "need_categories": {
              "type": "array",
              "items": {
                "enum": ["price", "quality", "sizing_fit", "delivery_timing", "trust_reviews", "other"]
              }
            },
            "evidence": {
              "type": "array",
              "items": { "$ref": "#/$defs/evidence_snippet" }
            }
          }
        },
        "objections": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["type", "summary", "handled", "evidence"],
            "properties": {
              "type": {
                "enum": ["price", "quality", "sizing_fit", "delivery_timing", "trust_reviews", "other"]
              },
              "summary": { "type": "string" },
              "handled": { "enum": ["yes", "no", "unclear"] },
              "evidence": {
                "type": "array",
                "items": { "$ref": "#/$defs/evidence_snippet" }
              }
            }
          }
        },
        "upsell": {
          "type": "object",
          "additionalProperties": false,
          "required": ["present", "kind", "offer_summary", "customer_response", "evidence"],
          "properties": {
            "present": { "type": "boolean" },
            "kind": { "enum": ["none", "upsell", "cross_sell", "bundle", "other"] },
            "offer_summary": { "type": "string" },
            "customer_response": { "enum": ["accepted", "declined", "unclear", "not_applicable"] },
            "evidence": {
              "type": "array",
              "items": { "$ref": "#/$defs/evidence_snippet" }
            }
          }
        },
        "next_step": {
          "type": "object",
          "additionalProperties": false,
          "required": ["type", "summary", "evidence"],
          "properties": {
            "type": { "enum": ["send_info", "schedule_call", "checkout", "follow_up", "none", "other"] },
            "summary": { "type": "string" },
            "evidence": {
              "type": "array",
              "items": { "$ref": "#/$defs/evidence_snippet" }
            }
          }
        },
        "tone": {
          "type": "object",
          "additionalProperties": false,
          "required": ["sales_rep_tone", "customer_tone", "evidence"],
          "properties": {
            "sales_rep_tone": { "enum": ["friendly", "neutral", "empathetic", "formal", "pushy", "other"] },
            "customer_tone": { "enum": ["friendly", "neutral", "frustrated", "skeptical", "other"] },
            "evidence": {
              "type": "array",
              "items": { "$ref": "#/$defs/evidence_snippet" }
            }
          }
        }
      }
    },
    "quality_checks": {
      "type": "object",
      "additionalProperties": false,
      "required": ["referenced_turn_ids", "ambiguities", "notes"],
      "properties": {
        "referenced_turn_ids": { "type": "array", "items": { "type": "integer" } },
        "ambiguities": { "type": "array", "items": { "type": "string" } },
        "notes": { "type": "string" }
      }
    }
  },
  "$defs": {
    "evidence_snippet": {
      "type": "object",
      "additionalProperties": false,
      "required": ["turn_id", "speaker", "quote"],
      "properties": {
        "turn_id": { "type": "integer" },
        "speaker": { "enum": ["Sales Rep", "Customer", "Other"] },
        "quote": { "type": "string" }
      }
    }
  }
}`

var llmAnnotationSchema = mustParseSchema(llmAnnotationSchemaJSON)

func mustParseSchema(rawSchema string) map[string]any {
	var schema map[string]any
	if err := json.Unmarshal([]byte(rawSchema), &schema); err != nil {
		panic(err)
	}
	return schema
}
