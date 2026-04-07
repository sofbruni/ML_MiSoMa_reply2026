"""
Semantic Enricher
─────────────────
A single LLM call that reads column names + sample values and decides the
*intended* semantic fix for each column.

Design principle: use the data's own dominant format — do NOT impose external
formats. If a column stores months as integers 1-12, keep them as integers.
If it stores years as 4-digit integers, keep them as integers. The enricher
only normalises within the existing format (precision, integer/float noise,
year shorthand). Cross-column temporal consistency (e.g. YYYYMM vs year/month
columns) is handled by the consistency team, not here.

IMPORTANT design rules (anti-overfitting):
  - Column name is a HINT, not a rule. Always verify against sample values.
  - If values do not support the hypothesis from the name, use fix_action "none".
  - Works for any language (Italian, English, French, Spanish, German, …).
  - The fix_action vocabulary is fixed — the LLM parameterises, never invents.

Fix-action vocabulary
─────────────────────
  round_Ndp  params: {decimals: int}
      Round numeric values to N decimal places.
      Use when values have unnecessary float precision (e.g. 129474.23000000003).

  coerce_int params: {}
      Strip trailing .0 from integer-valued floats (e.g. 3.0 → 3).
      ONLY valid if ALL non-null values are whole numbers.

  none       params: {}
      No semantic fix needed. Use this when uncertain.

  Note: year-shorthand expansion (23 → 2023) and cross-column temporal consistency
  are handled deterministically by schema_tools and consistency_tools respectively.
"""

import json
import re

import pandas as pd


VALID_ACTIONS = {"round_Ndp", "coerce_int", "coerce_year", "none"}


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def _build_prompt(profile: dict) -> str:
    col_profiles = profile.get("columns", {})
    lines = []
    for col, meta in col_profiles.items():
        samples = meta.get("sample_values", [])
        sem = meta.get("semantic_type", "unknown")
        lines.append(f'  "{col}": type={sem}, samples={samples}')

    columns_block = "\n".join(lines)

    return f"""You are a data quality expert. Analyse the columns below and decide whether each one needs a semantic fix.

PRINCIPLE: Use the data's own dominant format — do NOT convert to a different format.
If months are stored as integers 1-12, keep them as integers. If period codes are
6-digit integers (YYYYMM), keep them as integers. Only normalise within the existing
format (fix precision noise, strip .0 from integers, expand 2-digit years).

COLUMNS (name, current pandas type, sample values):
{columns_block}

RULES — read carefully:
1. The column NAME is a HINT only. Always verify the hypothesis against the SAMPLE VALUES.
2. If sample values do not support your hypothesis, use fix_action "none".
3. You MUST choose fix_action from this exact vocabulary:
   - "round_Ndp" — numeric column with excessive decimal precision due to float arithmetic noise.
                   Set params.decimals to the correct number of decimal places.
                   Example: 129474.23000000003 → decimals=2.
                   Do NOT flag columns with legitimately many decimal places (e.g. 1234.5678).
   - "coerce_int"  — numeric column where all values are whole numbers stored as floats (e.g. 3.0, 12.0).
                    Use to strip the unnecessary .0 suffix.
                    ONLY valid if ALL non-null values are whole numbers.
   - "coerce_year" — year column where some values are 2-digit shortcuts (e.g. 23, 95) while others
                    are 4-digit (e.g. 2023, 1995). Expand 2-digit values to 4-digit.
                    STRICT requirements — ALL three must be true:
                      (a) column name explicitly suggests a calendar year (e.g. "anno", "year", "yr", "jahr", "année")
                      (b) sample values contain BOTH 2-digit values AND 4-digit year values
                      (c) the 2-digit values are plausible year shortcuts (>= 20), not codes like 1, 2, 3
                    Do NOT use for entity codes, region codes, ministry codes, or any numeric ID.
   - "none"        — no semantic fix needed, or you are not confident.
4. Do NOT invent fix actions outside this list.
5. Omit columns that clearly need "none" — only return columns that need an actual fix.
6. Do NOT suggest converting integers to month names or period codes to date strings.
7. For "coerce_year": when in doubt, use "none". Only flag if column name + sample values leave no ambiguity.

Respond with ONLY a JSON object. No explanation, no markdown. Example:
{{
  "column_a": {{"fix_action": "round_Ndp", "params": {{"decimals": 2}}, "notes": "float noise: 129474.23000000003"}},
  "column_b": {{"fix_action": "coerce_int", "params": {{}}, "notes": "all whole numbers stored as floats"}}
}}"""


# ---------------------------------------------------------------------------
# Main enricher function
# ---------------------------------------------------------------------------

def enrich_profile(profile: dict, llm) -> dict:
    """
    Makes a single LLM call to enrich the dataset profile with semantic fix actions.
    Returns the enrichments dict: {col_name: {fix_action, params, notes}}.
    Stored in profile["enrichments"] by the caller.
    """
    prompt = _build_prompt(profile)

    try:
        response = llm.invoke([{"role": "user", "content": prompt}])
        raw = response.content if hasattr(response, "content") else str(response)

        # Gemini may return a list of content blocks: [{'type': 'text', 'text': '...'}]
        if isinstance(raw, list):
            raw = " ".join(
                item.get("text", "") if isinstance(item, dict) else str(item)
                for item in raw
            )

        # Strip thinking tags (Qwen3 / reasoning models)
        raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()

        # Extract JSON — handle markdown code fences
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            return {}
        enrichments = json.loads(match.group())

        # Validate: keep only entries with known fix_actions
        validated: dict = {}
        for col, entry in enrichments.items():
            if not isinstance(entry, dict):
                continue
            action = entry.get("fix_action", "none")
            if action not in VALID_ACTIONS or action == "none":
                continue
            validated[col] = {
                "fix_action": action,
                "params": entry.get("params", {}),
                "notes": entry.get("notes", ""),
            }
        return validated

    except Exception:
        # Enrichment is best-effort — never crash the pipeline
        return {}
