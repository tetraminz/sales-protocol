# SGR Scan Metrics

- metrics_version: `v4_bundled_full_judge`
- scan_policy: `bundled=true, judge=full, context=full, llm_trace=full`
- llm_audit_trace: `full request_json + response_json + extracted_json`
- canonical_run_id: `docs_refresh`
- current_run_id: `scan_a638d3fc8b97`
- inserted_results: `147`
- judged_results: `147`
- judge_coverage: `1.0000`
- judge_coverage_target: `1.00`

## Rule Metrics

| rule | eval_total | eval_true | evaluator_hit_rate | judge_correctness | judge_coverage |
|---|---:|---:|---:|---:|---:|
| `empathy` | 49 | 20 | 0.4082 | 0.8980 | 1.0000 |
| `greeting` | 49 | 6 | 0.1224 | 1.0000 | 1.0000 |
| `upsell` | 49 | 21 | 0.4286 | 1.0000 | 1.0000 |

## Rule Quality Delta (judge_correctness)

| rule | canonical | current | delta |
|---|---:|---:|---:|
| `greeting` | 1.0000 | 1.0000 | 0.0000 |
| `upsell` | 1.0000 | 1.0000 | 0.0000 |
| `empathy` | 1.0000 | 0.8980 | -0.1020 |

## LLM Calls

| phase | calls | errors | prompt_chars | response_chars |
|---|---:|---:|---:|---:|
| `evaluator` | 49 | 0 | 180935 | 30626 |
| `judge` | 49 | 0 | 148750 | 21638 |

## Judge-Aligned Heatmap

- thresholds: `green >= 0.90, yellow >= 0.80, red < 0.80, na = no_judged`
- conversations: `5`
- rules: `3`

| zone | cells |
|---|---:|
| `green` | 13 |
| `yellow` | 2 |
| `red` | 0 |
| `na` | 0 |

### Worst conversation x rule cells

| conversation_id | rule | score | judged_total |
|---|---|---:|---:|
| `modamart__2_transcript` | `empathy` | 0.8000 | 10 |
| `modamart__3_transcript` | `empathy` | 0.8889 | 9 |
| `modamart__0_transcript` | `empathy` | 0.9000 | 10 |
| `modamart__4_transcript` | `empathy` | 0.9000 | 10 |
| `modamart__0_transcript` | `greeting` | 1.0000 | 10 |
| `modamart__0_transcript` | `upsell` | 1.0000 | 10 |
| `modamart__1_transcript` | `empathy` | 1.0000 | 10 |
| `modamart__1_transcript` | `greeting` | 1.0000 | 10 |
| `modamart__1_transcript` | `upsell` | 1.0000 | 10 |
| `modamart__2_transcript` | `greeting` | 1.0000 | 10 |

## Judge-Confirmed Bad Cases (judge_label=0)

| conversation_id | seller_message_id | rule | eval_hit | expected_hit | reason_code | evidence_quote |
|---|---:|---|---:|---:|---|---|
| `modamart__0_transcript` | 17 | `empathy` | 0 | 1 | `informational_without_empathy` | `We’re here to help. Thank you for your time and have a great day!` |
| `modamart__2_transcript` | 44 | `empathy` | 0 | 1 | `informational_without_empathy` | `That's great to hear you've been checking out our new collection! What specific…` |
| `modamart__2_transcript` | 52 | `empathy` | 0 | 1 | `informational_without_empathy` | `I highly recommend it. They can be very informative. Plus, if you find that you…` |
| `modamart__3_transcript` | 70 | `empathy` | 0 | 1 | `informational_without_empathy` | `` |
| `modamart__4_transcript` | 87 | `empathy` | 0 | 1 | `informational_without_empathy` | `` |

### Bad Case Details

1. `modamart__0_transcript` seller_msg=17 rule=`empathy` eval_hit=0 expected_hit=1
   customer_text: No, I think that covers it.
   seller_text: Fantastic. I’ll email you the information, and if you have any more questions or need further assistance, feel free to reach out. We’re here to help. Thank you for your time and have a great day!
   evaluator_reason: Продавец выражает вежливость и готовность помочь, но не признаёт и не отражает состояние или ситуацию клиента.
   judge_rationale: Продавец выражает готовность помочь, что можно считать признанием ситуации клиента, но evaluator поставил false.
   evidence_quote: We’re here to help. Thank you for your time and have a great day!
2. `modamart__2_transcript` seller_msg=44 rule=`empathy` eval_hit=0 expected_hit=1
   customer_text: Well, I have been looking at some of your new fall collection, but I'm honestly not sure about a few things.
   seller_text: That's great to hear you've been checking out our new collection! What specifically has you unsure?
   evaluator_reason: Реплика позитивна и информативна, но нет признания чувств или ситуации клиента.
   judge_rationale: В контексте есть признание ситуации клиента (строка 3), но реплика не выражает эмпатию, поэтому hit=false неверно.
   evidence_quote: That's great to hear you've been checking out our new collection! What specifically has you unsure?
3. `modamart__2_transcript` seller_msg=52 rule=`empathy` eval_hit=0 expected_hit=1
   customer_text: No, I haven't really looked at the reviews yet.
   seller_text: I highly recommend it. They can be very informative. Plus, if you find that you're still not satisfied with the product, our customer service team is here to ensure you have a positive experience.
   evaluator_reason: Продавец не признает ситуацию или переживания клиента, а лишь предоставляет информацию и рекомендацию.
   judge_rationale: В реплике продавец выражает понимание (рекомендация отзывов и поддержка), значит есть эмпатия, оценка hit=false ошибочна.
   evidence_quote: I highly recommend it. They can be very informative. Plus, if you find that you're still not satisfied with the product…
4. `modamart__3_transcript` seller_msg=70 rule=`empathy` eval_hit=0 expected_hit=1
   customer_text: That’s a nice touch. I’m also concerned about the pricing. I want good quality but at a reasonable price.
   seller_text: We strive to offer quality products at competitive prices. Plus, if you sign up for our ModaMart Rewards program, you’ll receive exclusive discounts and early access to sales. As a new customer, we c…
   evaluator_reason: Реплика информативна по цене и качеству, но не содержит признания или понимания беспокойств клиента.
   judge_rationale: Реплика не признаёт явно беспокойства клиента, хотя должна.
   evidence_quote: 
5. `modamart__4_transcript` seller_msg=87 rule=`empathy` eval_hit=0 expected_hit=1
   customer_text: That sounds reassuring, but it's still quite a lot of money. Do you offer any payment plans?
   seller_text: Yes, we do offer financing options. You can break down the cost into manageable monthly payments at 0% interest if paid within 6 months. We also have a 30-day return policy if you’re not completely s…
   evaluator_reason: Продавец информирует о вариантах оплаты и политике возврата, но не признаёт непосредственно чувства или ситуацию клиента.
   judge_rationale: Продавец ранее признал проблему клиента, текущая реплика об оплате свидетельствует о признании ситуации.
   evidence_quote: 

## Run Summary JSON

- summary_json: `{"conversation_from":0,"conversation_to":4,"selected_conversations":5,"messages":96,"seller_messages":49,"customer_messages_context_only":47,"rules":3,"metrics_version":"v4_bundled_full_judge","bundle_rules":true,"judge_mode":"full","context_mode":"full","llm_trace":"full","seller_turns":49,"processed":147,"inserted":147,"judged":147,"schema_errors":0,"non_schema_errors":0,"judge_coverage":1.0,"canonical_run_id":"docs_refresh"}`
