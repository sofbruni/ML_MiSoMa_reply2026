"""
Debug script for local LM Studio model.
Tests structured output and basic tool calling without touching any pipeline code.
Delete this file when done — it has no effect on the pipeline.

Usage:
    python debug_local_llm.py
"""

import json, re
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel
from typing import Literal

LM_STUDIO_BASE_URL = "http://localhost:1234/v1"
MODEL_NAME = "qwen/qwen3.5-9b"   # adjust if needed

llm = ChatOpenAI(
    model=MODEL_NAME,
    base_url=LM_STUDIO_BASE_URL,
    api_key="lm-studio",
    temperature=0,
)

# ── Test 1: plain text response ──────────────────────────────────────────────
print("=" * 60)
print("TEST 1: Plain text response")
print("=" * 60)
try:
    response = llm.invoke([HumanMessage(content="Say hello in one sentence.")])
    print("RAW RESPONSE TYPE:", type(response))
    print("CONTENT:", repr(response.content))
    print("ADDITIONAL KWARGS:", response.additional_kwargs)
except Exception as e:
    print("FAILED:", e)

# ── Test 2: structured output (the part that crashes) ────────────────────────
print()
print("=" * 60)
print("TEST 2: Structured output (Router schema)")
print("=" * 60)

class Router(BaseModel):
    next: Literal["schema_team", "completeness_team", "consistency_team",
                  "anomaly_team", "remediation_team", "FINISH"]

try:
    structured_llm = llm.with_structured_output(Router)
    result = structured_llm.invoke([
        HumanMessage(content=(
            "You are a pipeline supervisor. The schema team has not run yet. "
            "Which team should run next? "
            "Choose from: schema_team, completeness_team, consistency_team, "
            "anomaly_team, remediation_team, FINISH."
        ))
    ])
    print("RESULT:", result)
except Exception as e:
    print("FAILED:", type(e).__name__, "—", e)

# ── Test 3–4 unchanged (already ran, all failed) ─────────────────────────────
# ── Test 5: /no_think token in system message ────────────────────────────────
print()
print("=" * 60)
print("TEST 5: /no_think token + structured output")
print("=" * 60)
try:
    structured_llm = llm.with_structured_output(Router)
    result = structured_llm.invoke([
        SystemMessage(content="/no_think"),
        HumanMessage(content=(
            "You are a pipeline supervisor. The schema team has not run yet. "
            "Which team should run next? "
            "Choose from: schema_team, completeness_team, consistency_team, "
            "anomaly_team, remediation_team, FINISH."
        ))
    ])
    print("RESULT:", result)
except Exception as e:
    print("FAILED:", type(e).__name__, "—", e)

# ── Test 6: plain text JSON prompt (no structured output at all) ──────────────
print()
print("=" * 60)
print("TEST 6: Plain text JSON prompt (no structured output)")
print("=" * 60)
try:
    response = llm.invoke([
        SystemMessage(content="/no_think"),
        HumanMessage(content=(
            "You are a pipeline supervisor. Reply with ONLY a JSON object, no other text.\n"
            'Format: {"next": "<team_name>"}\n'
            "Valid values: schema_team, completeness_team, consistency_team, "
            "anomaly_team, remediation_team, FINISH\n\n"
            "The schema team has not run yet. Which team should run next?"
        ))
    ])
    raw = response.content
    print("RAW CONTENT:", repr(raw))
    # Try to parse it
    match = re.search(r'\{.*?\}', raw, re.DOTALL)
    if match:
        parsed = json.loads(match.group())
        print("PARSED:", parsed)
    else:
        print("No JSON found in response")
except Exception as e:
    print("FAILED:", type(e).__name__, "—", e)

# ── OLD Test 3: Disable thinking mode via extra_body ─────────────────────────
print()
print("=" * 60)
print("TEST 3: Structured output WITH thinking disabled")
print("=" * 60)
try:
    llm_no_think = ChatOpenAI(
        model=MODEL_NAME,
        base_url=LM_STUDIO_BASE_URL,
        api_key="lm-studio",
        temperature=0,
        extra_body={"chat_template_kwargs": {"enable_thinking": False}},
    )
    structured_llm = llm_no_think.with_structured_output(Router)
    result = structured_llm.invoke([
        HumanMessage(content=(
            "You are a pipeline supervisor. The schema team has not run yet. "
            "Which team should run next? "
            "Choose from: schema_team, completeness_team, consistency_team, "
            "anomaly_team, remediation_team, FINISH."
        ))
    ])
    print("RESULT:", result)
except Exception as e:
    print("FAILED:", type(e).__name__, "—", e)

# ── Test 4: json_schema response format ──────────────────────────────────────
print()
print("=" * 60)
print("TEST 4: with_structured_output using method='json_schema'")
print("=" * 60)
try:
    result = llm.with_structured_output(Router, method="json_schema").invoke([
        HumanMessage(content=(
            "You are a pipeline supervisor. The schema team has not run yet. "
            "Which team should run next? "
            "Choose from: schema_team, completeness_team, consistency_team, "
            "anomaly_team, remediation_team, FINISH."
        ))
    ])
    print("RESULT:", result)
except Exception as e:
    print("FAILED:", type(e).__name__, "—", e)
