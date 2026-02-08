# Schema-Guided Reasoning (SGR)

**Purpose**: Guide LLMs through predefined reasoning steps using structured schemas to produce consistent, auditable, and accurate outputs.

**When to use**: When you need LLMs to follow specific reasoning logic, maintain consistency across runs, or require auditable decision-making processes.

## Core Concept

SGR controls HOW an LLM reasons by encoding reasoning pathways in schemas and enforcing them via constrained decoding (Structured Output). Instead of hoping the prompt will guide reasoning correctly, you mechanically enforce the reasoning structure.

**Key principle**: Translate expert mental checklists into executable schemas.

## Three Fundamental Patterns

### 1. CASCADE - Sequential Reasoning

Forces explicit step-by-step reasoning where each field allocates thinking budget.

**When to use**:
- Multi-step analysis required
- Need to prevent skipped reasoning steps
- Want explicit intermediate outputs for debugging

**Python Example**:
```python
from pydantic import BaseModel, Field
from typing import Literal, Annotated
from annotated_types import Ge, Le

class CandidateEvaluation(BaseModel):
    # Step 1: Review knowledge
    brief_candidate_summary: str
    
    # Step 2: Rate skill match (1-10 enforced)
    rate_skill_match: Annotated[int, Ge(1), Le(10)]
    
    # Step 3: Final decision
    final_recommendation: Literal["hire", "reject", "hold"]

# Usage
completion = client.chat.completions.parse(
    model="gpt-4o-mini",
    response_format=CandidateEvaluation,
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": "Evaluate Sam Altman for DevOps Role"}
    ]
)
```

**Key insight**: Order matters. Start broad (summary) → narrow down (rating) → conclude (decision).

### 2. ROUTING - Branching Logic

Forces explicit choice between different reasoning paths with specific requirements per path.

**When to use**:
- Different categories need different handling
- Tool selection from multiple options
- Classification with category-specific analysis

**Python Example**:
```python
from pydantic import BaseModel, Field
from typing import Union, Literal, List

class HardwareIssue(BaseModel):
    kind: Literal["hardware"]
    component: Literal["battery", "display", "keyboard"]

class SoftwareIssue(BaseModel):
    kind: Literal["software"]
    software_name: str

class UnknownIssue(BaseModel):
    kind: Literal["unknown"]
    category: str
    summary: str

class SupportTriage(BaseModel):
    issue: Union[HardwareIssue, SoftwareIssue, UnknownIssue]

# For tools - use Literal for the tool field
class SendEmail(BaseModel):
    tool: Literal["send_email"]
    recipient_email: str
    subject: str
    message: str

class SearchKnowledgeBase(BaseModel):
    tool: Literal["search_knowledge_base"]
    query: str

class CreateTicket(BaseModel):
    tool: Literal["create_support_ticket"]
    customer_id: int
    issue_summary: str
    priority: Literal["low", "medium", "high"]

class Response(BaseModel):
    action: Union[SendEmail, SearchKnowledgeBase, CreateTicket]
    summary: str
```

**Critical**: The `tool` field with Literal enables pydantic to discriminate between union members.

### 3. CYCLE - Repeated Reasoning

Forces LLM to repeat reasoning steps multiple times.

**When to use**:
- Need multiple items (risk factors, recommendations, alternatives)
- Parallel tool execution
- Exhaustive analysis required

**Python Example**:
```python
from pydantic import BaseModel
from typing import List, Literal
from annotated_types import MinLen, MaxLen

class RiskFactor(BaseModel):
    explanation: str
    severity: Literal["low", "medium", "high"]

class RiskAssessment(BaseModel):
    # Forces 2-4 risk factors
    factors: Annotated[List[RiskFactor], MinLen(2), MaxLen(4)]

# For parallel tool execution
class Response(BaseModel):
    # LLM can call 1-5 tools in parallel
    actions: Annotated[
        List[Union[SendEmail, SearchKnowledgeBase, CreateTicket]],
        MinLen(1),
        MaxLen(5)
    ]
    summary: str
```

## Advanced Pattern: Adaptive Planning

Combines CASCADE + ROUTING for agentic behavior.

**Key mechanism**: Plan multiple steps ahead BUT execute only the first step. After execution, replan with new context.

```python
from typing import Union, List
from annotated_types import MinLen, MaxLen

class NextStep(BaseModel):
    # Thinking space
    current_state: str
    
    # Plan ahead (1-5 steps) for coherence
    plan_remaining_steps_brief: Annotated[List[str], MinLen(1), MaxLen(5)]
    
    # Check if done
    task_completed: bool
    
    # EXECUTE ONLY FIRST STEP (rest discarded)
    function: Union[
        ReportTaskCompletion,
        SendEmail,
        GetCustomerData,
        IssueInvoice,
        VoidInvoice,
        CreateRule,
    ] = Field(..., description="execute first remaining step")

# Execution loop
for i in range(max_steps):
    response = client.chat.completions.parse(
        model="gpt-4o-mini",
        response_format=NextStep,
        messages=conversation_log
    )
    
    if isinstance(response.function, ReportTaskCompletion):
        break  # Task done
    
    # Execute tool
    result = dispatch(response.function)
    
    # Add to conversation log
    conversation_log.append({
        "role": "assistant",
        "content": response.model_dump_json()
    })
    conversation_log.append({
        "role": "tool",
        "content": result
    })
    # Next iteration replans with new information
```

**Why this works**: LLMs don't pay cognitive cost for replanning. Plans stay fresh and adapt to new information (like discovered rules in memory).

## Practical Examples

### Example 1: Text-to-SQL with Validation

```python
class SolutionChecklist(BaseModel):
    tables_to_query: List[str]
    columns_to_query: List[str]
    dependency_kind: Literal["direct", "indirect", "N/A"]
    
    is_subject_system_from_or_to: Literal[
        "from_system_id points to our system",
        "to_system_id points to our system",
        "N/A"
    ] = Field(default=..., description="Are we looking for systems that depend on the target system?")
    
    does_this_require_recursive_query: bool
    does_this_require_subquery: bool
    is_this_forward_or_backward_pass: Literal["forward", "backward", "N/A"] = Field(
        default=..., 
        description="Determines if we start with .to_system_id or .from_system_id"
    )
    
    should_we_filter_out_subject_system_from_results_to_avoid_overcounting: bool

class Response(BaseModel):
    strategy: SolutionChecklist  # Forces thinking BEFORE query
    sql_query: str
```

**Result**: +6% accuracy improvement from forcing structured pre-analysis.

### Example 2: Document Classification

```python
DOCUMENT_TYPES = ["invoice", "contract", "receipt", "email"]
ENTITY_TYPES = ["payment", "risk", "regulator", "employee"]

class DocumentClassification(BaseModel):
    # Step 1: Identify type (constrained choice)
    document_type: Literal[tuple(DOCUMENT_TYPES)]
    
    # Step 2: Summarize
    brief_summary: str
    
    # Step 3: Extract entities (constrained list)
    key_entities_mentioned: List[Literal[tuple(ENTITY_TYPES)]]
    
    # Step 4: Keywords (up to 10)
    keywords: Annotated[List[str], MaxLen(10)] = Field(
        ..., 
        description="Up to 10 keywords describing this document"
    )
```

**Note**: First two fields (type, summary) can be discarded after use - they're just to force proper thinking angle.

### Example 3: Compliance Gap Analysis

```python
class Applicability(BaseModel):
    is_applicable: bool = Field(
        ..., 
        description="Is this regulation clause applicable to the document?"
    )
    missing_items: List[str] = Field(
        ...,
        description="Brief explanation of what should be changed in document to comply"
    )
    answer: Literal["compliant", "needs_clarification", "non_compliant"]

class ConclusionBaseModel(BaseModel):
    # CASCADE: Force preliminary analysis first
    does_this_regulation_apply_to_the_document: Applicability
    
    # Then identify gaps
    missing: List[str] = Field(
        ..., 
        description="Does the document comply with the requirements?"
    )
    
    # Verification step (catches over-pessimistic flagging)
    reasonForNoncompliance: Literal[
        "None",
        "CompletelyMissing",
        "PartiallyComplete",
        "UnrelatedRequirements",
        "DirectlyContradicted"
    ]
    
    gapSeverity: Literal["minor", "moderate", "severe"]
    
    # Supporting evidence with exact references
    relevant_clauses: List[str] = Field(
        ...,
        description="List of clause IDs that address the requirement directly"
    )
```

**Key**: Verification fields allow LLM to review its own work and downgrade false positives.

## Implementation Guidelines

### 1. Schema Design Principles

**DO**:
- Use Literal for constrained choices
- Use Annotated[int, Ge(X), Le(Y)] for bounded numbers
- Use List with MinLen/MaxLen for cycles
- Put field descriptions - they guide the LLM
- Order fields to create reasoning flow
- Use Union for routing/branching
- Add verification/review steps after critical decisions

**DON'T**:
- Over-explain in prompts what schema already enforces
- Use generic str fields for everything
- Skip intermediate reasoning fields to "save tokens"
- Forget that LLMs read field names (make them descriptive)

### 2. When to Apply Each Pattern

**CASCADE**:
- Any multi-step analysis
- When order of thinking matters
- When you need audit trail
- Default pattern for most tasks

**ROUTING**:
- Tool selection
- Classification with different follow-ups
- When branches need different schemas

**CYCLE**:
- Multiple items needed
- Parallel tool execution
- Exhaustive coverage required

**Combine them**: Most production systems use CASCADE + ROUTING + CYCLE together.

### 3. Testing Strategy

SGR makes testing easier because you can test each field:

```python
# Test dataset example
test_cases = [
    {
        "input": "Invoice from ACME Corp dated 2024-01-15",
        "expected": {
            "document_type": "invoice",  # Can verify this
            "key_entities_mentioned": ["payment"],  # And this
            # Final answer tested here
        }
    }
]

# Each field becomes a test assertion point
def test_classification():
    result = classify_document(test_input)
    assert result.document_type == expected.document_type
    assert result.brief_summary is not None
    assert len(result.keywords) <= 10
```

### 4. Common Pitfalls

**Pitfall 1**: Using `conint(ge=1, le=10)` instead of `Annotated[int, Ge(1), Le(10)]`
- conint is deprecated
- Use Annotated with annotated_types

**Pitfall 2**: Forgetting Union discriminator
```python
# BAD - pydantic can't discriminate
class Response(BaseModel):
    action: Union[SendEmail, SearchKB]

# GOOD - tool field is discriminator
class SendEmail(BaseModel):
    tool: Literal["send_email"]
    # ...

class SearchKB(BaseModel):
    tool: Literal["search_kb"]
    # ...
```

**Pitfall 3**: Not using field descriptions
```python
# BAD
task_completed: bool

# GOOD
task_completed: bool = Field(
    ...,
    description="True if task is done, False if more steps needed"
)
```

**Pitfall 4**: Putting reasoning fields at the END
```python
# BAD - LLM makes decision then justifies
class Bad(BaseModel):
    answer: Literal["yes", "no"]
    reasoning: str

# GOOD - LLM reasons then decides
class Good(BaseModel):
    reasoning: str
    answer: Literal["yes", "no"]
```

## Provider Support Matrix

| Provider | Support | Method |
|----------|---------|--------|
| OpenAI | ✅ Full | Structured Outputs (JSON Schema via llguidance) |
| Anthropic | ✅ Full | response_format with tool choice |
| Mistral | ✅ Full | response_format |
| Google/Gemini | ✅ Full | response_schema (since Nov 2024) |
| Grok | ✅ Full | Structured Outputs |
| Fireworks AI | ✅ Full | JSON Schema |
| OpenRouter | ⚠️ Depends | Varies by upstream provider |

**Local inference**:
- ollama: ✅ Structured Outputs
- vllm: ✅ xgrammar or guidance backends
- SGLang: ✅ Outlines, XGrammar, or llguidance
- TensorRT-LLM: ✅ GuidedDecoding

## Performance Impact

**Typical improvements**:
- +5-10% accuracy for complex tasks
- +50% for weaker models on structured tasks
- 3-5x faster debugging (explicit intermediate states)
- 10x faster test dataset creation

**Trade-offs**:
- +20-30% token usage (reasoning overhead)
- Fixed schema maintenance cost
- Less "creative" responses (by design)

## Production Checklist

Before production deployment:

- [ ] Test with representative edge cases
- [ ] Verify all Literal constraints are exhaustive
- [ ] Add field descriptions for critical fields
- [ ] Implement proper error handling for constraint violations
- [ ] Log full reasoning traces for debugging
- [ ] Create eval dataset covering each schema field
- [ ] Test with multiple models (cloud + local)
- [ ] Benchmark token cost vs accuracy gain
- [ ] Add monitoring for reasoning pattern drift
- [ ] Document schema design decisions

## Complete Working Example

See `sgr_demo.py` in resources for full 160-line business assistant with:
- Cascade pattern (NextStep planner)
- Routing (tool selection)
- Cycle (multi-step plans)
- Adaptive replanning
- Memory/rules system
- Invoice generation with math validation

## Quick Reference

```python
# PATTERN: Cascade (step-by-step)
class Analysis(BaseModel):
    step1: str  # Broad
    step2: int  # Narrow
    step3: Literal["a", "b"]  # Conclude

# PATTERN: Routing (branching)
class Choice(BaseModel):
    path: Union[PathA, PathB, PathC]

# PATTERN: Cycle (repetition)
class Multiple(BaseModel):
    items: Annotated[List[Item], MinLen(2), MaxLen(5)]

# PATTERN: Adaptive Planning
class Agent(BaseModel):
    plan: List[str]  # Think ahead
    task_completed: bool  # Check if done
    function: Union[Tools]  # Execute ONE step
```

## Resources

- Original blog: https://abdullin.com/schema-guided-reasoning/
- Demo code (Python): https://gist.github.com/abdullin/sgr-demo
- OpenAI Structured Outputs: https://platform.openai.com/docs/guides/structured-outputs

---

*Author: Based on Rinat Abdullin's Schema-Guided Reasoning methodology*
*Version: 1.0*
*Last updated: January 2025*