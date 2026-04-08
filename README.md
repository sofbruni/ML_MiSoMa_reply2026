# NoiPA Data Quality — Multi-Agent System

A hierarchical LangGraph pipeline that autonomously inspects and cleans any CSV dataset.
Built for the **LUISS × Reply** university project (2026), originally targeting Italian Public Administration (NoiPA) payroll data.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [What the System Does](#what-the-system-does)
3. [High-Level Architecture](#high-level-architecture)
4. [Pipeline Execution Flow](#pipeline-execution-flow)
5. [State Management](#state-management)
6. [Team-by-Team Breakdown](#team-by-team-breakdown)
   - [Team 1 — Schema Validation](#team-1--schema-validation)
   - [Team 2 — Completeness Analysis](#team-2--completeness-analysis)
   - [Team 3 — Consistency Validation](#team-3--consistency-validation)
   - [Team 4 — Anomaly Detection](#team-4--anomaly-detection)
   - [Team 5 — Remediation & Reliability](#team-5--remediation--reliability)
7. [Tools Reference](#tools-reference)
8. [Fix Functions Reference](#fix-functions-reference)
9. [Scoring Formula](#scoring-formula)
10. [Output Files](#output-files)
11. [File Structure](#file-structure)
12. [LLM Configuration](#llm-configuration)

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set your Google API key
export GOOGLE_API_KEY="your_key_here"
# — or — add it to a .env file in the project root:
# GOOGLE_API_KEY=your_key_here

# 3. Run the pipeline
python main.py                                  # uses data/spesa.csv by default
python main.py data/attivazioniCessazioni.csv   # any other CSV
python main.py /absolute/path/to/any.csv        # absolute path
```

---

## What the System Does

Given any CSV file, the system:

1. Routes through **5 specialist teams** in fixed sequence
2. Each team **inspects** the current working CSV using LLM-powered ReAct agents
3. After each team's inspection, a deterministic **fix function** applies corrections and writes a new versioned CSV
4. A final **reliability score** (0–100, graded A–F) is computed from all findings
5. Two output reports are saved: a machine-readable JSON and a human-readable Markdown file

The pipeline is **dataset-agnostic** — it uses pandas heuristics to auto-detect column types, date formats, key columns, numeric outliers, and categorical anomalies without any hardcoded column names.

---

## High-Level Architecture

```
main.py
  └── graph.stream(initial_state)
        │
        └── TOP SUPERVISOR (LLM, structured output routing)
              │
              ├── [1] schema_team  ──────────────────────────────────────────
              │         ├── data_type_validator        (ReAct agent)
              │         └── naming_convention_checker  (ReAct agent)
              │         → apply_schema_fixes()  → spesa_v1.csv
              │
              ├── [2] completeness_team  ────────────────────────────────────
              │         ├── null_detector               (ReAct agent)
              │         ├── completeness_rate_calculator(ReAct agent)
              │         └── sparse_column_detector      (ReAct agent)
              │         → apply_completeness_fixes()  → spesa_v2.csv
              │
              ├── [3] consistency_team  ─────────────────────────────────────
              │         ├── format_consistency_checker  (ReAct agent)
              │         ├── cross_column_checker        (ReAct agent)
              │         └── duplicate_detector          (ReAct agent)
              │         → apply_consistency_fixes()  → spesa_v3.csv
              │
              ├── [4] anomaly_team  ──────────────────────────────────────────
              │         ├── numerical_outlier_detector  (ReAct agent)
              │         └── categorical_anomaly_detector(ReAct agent)
              │         → no CSV change (detection only)
              │
              └── [5] remediation_team  ──────────────────────────────────────
                        ├── correction_suggester         (ReAct agent)
                        └── reliability_scorer           (ReAct agent)
                        → spesa_v4.csv (copy of v3) + quality_report.json + quality_report.md
```

### Two Supervision Levels

| Level | Role | Mechanism |
|---|---|---|
| **Top supervisor** | Routes control between the 5 teams in order 1→2→3→4→5→FINISH | LLM with structured output (`Router` TypedDict) |
| **Team supervisor** (Consistency & Anomaly) | Routes between workers inside those teams | LLM with structured output |
| **Fixed edges** (Schema, Completeness, Remediation) | Workers run in deterministic sequence — no LLM supervisor needed | LangGraph `add_edge()` |

---

## Pipeline Execution Flow

```
START
  │
  ▼
top_supervisor  ──routes──►  schema_team node
                               │
                               ├─ invokes schema_graph (subgraph)
                               │     data_type_validator → naming_convention_checker
                               │
                               ├─ reads last AI message (findings summary)
                               ├─ calls apply_schema_fixes(path, v1_path, profile)
                               └─ updates working_dataset_path → v1.csv
                               └─ returns to top_supervisor
  │
  ▼
top_supervisor  ──routes──►  completeness_team node
                               │  (same pattern: subgraph → fix function → v2.csv)
  │
  ▼
top_supervisor  ──routes──►  consistency_team node
                               │  (subgraph with internal LLM supervisor → v3.csv)
  │
  ▼
top_supervisor  ──routes──►  anomaly_team node
                               │  (subgraph → no fix; working_dataset_path unchanged)
  │
  ▼
top_supervisor  ──routes──►  remediation_team node
                               │  (collects all prior findings from message history)
                               │  (subgraph → suggestions + score)
                               │  (saves v4.csv, .json, .md)
  │
  ▼
top_supervisor  ──routes──►  FINISH → END
```

Each team node follows the same 4-step pattern:
1. **Invoke** the compiled team subgraph with the current `working_dataset_path`
2. **Extract** the last AI message (the team's natural-language findings)
3. **Call** the fix function (deterministic Python, no LLM) to produce the next versioned CSV
4. **Update** `working_dataset_path` in the top-level state and return to the supervisor

---

## State Management

### `DataQualityState` (top-level, `data_quality/state.py`)

Extends LangGraph's `MessagesState` (which holds a list of `BaseMessage` objects):

| Field | Type | Purpose |
|---|---|---|
| `messages` | `list[BaseMessage]` | Accumulated conversation: every team's findings summary as a `HumanMessage` |
| `original_dataset_path` | `str` | Path to the input CSV — never changes after initialization |
| `working_dataset_path` | `str` | Path to the current cleaned CSV — updated after each team applies fixes |
| `dataset_profile` | `dict` | Column-level semantic profile (populated before pipeline start; read-only after) |
| `next` | `str` | Routing target set by the top supervisor |

### `TeamState` (per-team subgraph, each `teams/*.py`)

Also extends `MessagesState`:

| Field | Type | Purpose |
|---|---|---|
| `messages` | `list[BaseMessage]` | The initial task message + each worker's output |
| `next` | `str` | Routing target set by the team supervisor (used in consistency/anomaly teams only) |

### Message flow between teams

When the top-level graph passes work to a team, it sends a `HumanMessage` like:
```
Perform schema validation: check data types and naming conventions.

Dataset path: /path/to/spesa.csv
```
When the team finishes, the top-level node wraps the team's last message into a new `HumanMessage` tagged with `name="schema_team"` and appends it to the top-level `messages` list. This means the top supervisor sees a growing history of all prior teams' outputs.

### `_task_only` pattern

Schema, completeness, and remediation teams use a `_task_only` helper that passes **only the first message** (the task + path) to each worker agent. This prevents workers from being confused by prior workers' outputs — each agent sees only the dataset path it needs.

---

## Team-by-Team Breakdown

---

### Team 1 — Schema Validation

**File:** `data_quality/teams/schema_team.py`
**Subgraph routing:** Fixed edges (no LLM supervisor)
**Produces:** `spesa_v1.csv`

```
START → data_type_validator → naming_convention_checker → END
```

#### Agent 1: `data_type_validator`
**Tool used:** `validate_data_types`

Scans every column for type contamination. Two checks are performed:
- **Numeric contamination**: if ≥90% of values parse as float but there are leftover non-numeric strings → flags them (common causes: `€` symbols, unit suffixes, placeholder strings like `N.D.`)
- **Date contamination**: if ≥70% of values parse as dates but there are unparseable strings → flags them (common causes: mixed separators, Italian month abbreviations mixed with ISO dates)

Reports findings grouped by severity: CRITICAL (>10% contaminated), MODERATE (1–10%), MINOR (<1%).

#### Agent 2: `naming_convention_checker`
**Tool used:** `check_naming_conventions`

Two checks:
1. **Naming violations**: checks each column name for uppercase letters, spaces, digit-first starts, special characters (`%@!#`), and hyphens. Suggests a corrected `snake_case` name for each.
2. **Duplicate columns**: uses a two-step detection — normalized-name substring matching + Jaccard value overlap > 80%. Flags columns that are likely redundant copies (e.g., `"SPESA TOTALE"` vs `"spesa"` if they share >80% of unique values).

#### Fix function: `apply_schema_fixes(input_path, output_path, profile)`

Deterministic corrections (no LLM):
1. **Exact-name dedup**: drops columns where the normalized form (all lowercase, non-alphanumeric removed) is identical to a prior column
2. **Substring-name + value-overlap dedup**: for column pairs where one normalized name contains the other, confirms duplication via Pearson correlation (numeric) or Jaccard (categorical) > 0.85/0.80
3. **Currency stripping**: strips `€`, `$`, `£`, `%`, `EUR` from numeric columns and coerces to float
4. **Placeholder → NaN**: replaces strings like `"N/A"`, `"-"`, `"??"`, `"null"` with NaN in numeric/identifier columns
5. **Year shorthand expansion**: if the profile flags a column with 2-digit years (e.g., `24` → `2024`)
6. **Hyphen → underscore renaming**: renames all `col-name` style columns to `col_name`
7. **Integer preservation**: coerces whole-number float columns to `Int64` to avoid `.0` suffixes in CSV output

---

### Team 2 — Completeness Analysis

**File:** `data_quality/teams/completeness_team.py`
**Subgraph routing:** Fixed edges (no LLM supervisor)
**Produces:** `spesa_v2.csv`

```
START → null_detector → completeness_rate_calculator → sparse_column_detector → END
```

#### Agent 1: `null_detector`
**Tool used:** `detect_missing_values`

Counts true NaN/null values AND placeholder strings that masquerade as real data (the `PLACEHOLDER_VALUES` set from `config.py`: `"n.d."`, `"NULL"`, `"unknown"`, `"?"`, `"//"`, `"-"`, `"null"`, `"N/A"`, `"undefined"`, `"ND"`, `""`, `" "`, `"nan"`).

Reports: null count, null%, effective missing%, and severity rating per column. Also counts how many rows have at least one missing value anywhere.

#### Agent 2: `completeness_rate_calculator`
**Tool used:** `calculate_completeness_rate`

Computes the completeness percentage (= 100% − missing%) for every column and an overall dataset figure. Reports columns sorted worst-first, with analysis implications for columns below 80% complete.

#### Agent 3: `sparse_column_detector`
**Tool used:** `detect_sparse_columns`

Identifies columns above a 50% missing threshold. Classifies them into:
- **Removal candidates** (>95% missing): recommend immediate drop
- **High-sparsity columns** (50–95% missing): recommend drop vs. impute vs. add a binary indicator column

#### Fix function: `apply_completeness_fixes(input_path, output_path, profile)`

1. **Placeholder → NaN**: all `PLACEHOLDER_VALUES` strings replaced with `pd.NA`
2. **Drop ultra-sparse columns**: drops any column >95% empty
3. **Numeric median fill**: fills NaN in columns profiled as `semantic_type: numeric` with the column median
4. **Categorical 'Unknown' fill**: fills remaining NaN in string columns with `"Unknown"`
5. **Integer preservation**: coerces whole-number float columns to `Int64`

---

### Team 3 — Consistency Validation

**File:** `data_quality/teams/consistency_team.py`
**Subgraph routing:** LLM supervisor (`consistency_supervisor_node`)
**Produces:** `spesa_v3.csv`

```
START → consistency_supervisor
              │
              ├──► format_consistency_checker → consistency_supervisor
              ├──► cross_column_checker       → consistency_supervisor
              └──► duplicate_detector         → consistency_supervisor → END
```

This is one of two teams with an LLM mid-level supervisor. After each worker reports, the supervisor decides the next worker to call. It enforces a fixed order (format → cross → duplicates) and only responds `FINISH` once all three have completed.

#### Agent 1: `format_consistency_checker`
**Tool used:** `check_format_consistency`

Auto-detects three column categories using pandas heuristics, then checks each:
- **Period columns** (YYYYMM): >80% of values match a 6-digit numeric where `month ∈ 1–12` and `year ∈ 1900–2099`. Flags any value not in pure YYYYMM format.
- **Date columns**: >60% of values match any of 8 date regex patterns. Flags mixed formats within a column (e.g., some rows `DD/MM/YYYY`, others `YYYY-MM-DD`).
- **Text columns**: flags mixed casing (e.g., some values all-caps, others title case) if no single case style covers >70% of the column.

#### Agent 2: `cross_column_checker`
**Tool used:** `check_cross_column_logic`

Two checks:
1. **Code-label consistency**: for any column named `cod_X`, looks for a matching label column (`X`, `tipo_X`, `desc_X`). If the same code maps to multiple different labels across rows, flags it as inconsistent.
2. **Unexpected negatives**: for every numeric column with ≥10 non-null values, flags if any negative values are present (typically a sign violation for spending amounts, counts, etc.).

#### Agent 3: `duplicate_detector`
**Tool used:** `detect_duplicates`

1. **Exact duplicates**: counts rows where all column values are identical.
2. **Near-duplicates on key columns**: auto-detects key columns by name pattern (`id`, `cod`, `code`, `key`, `codice`). If none found, falls back to columns where >50% of values are unique. Finds rows that share all key-column values but differ elsewhere.

#### Fix function: `apply_consistency_fixes(input_path, output_path)`

1. **Normalise period columns** to YYYYMM: handles `YYYY-MM`, `MM/YYYY`, `MON-YYYY` formats
2. **Normalise date columns** to `YYYY-MM-DD`: handles `DD/MM/YYYY`, `YYYY/MM/DD`, `DD.MM.YYYY`, `DD-MM-YYYY`, `DD-MM-YY`, ISO-8601 datetimes, and Italian/English month abbreviations
3. **Drop exact duplicate rows**

---

### Team 4 — Anomaly Detection

**File:** `data_quality/teams/anomaly_team.py`
**Subgraph routing:** LLM supervisor (`anomaly_supervisor_node`)
**Produces:** No CSV change (detection only — `working_dataset_path` is not updated)

```
START → anomaly_supervisor
              │
              ├──► numerical_outlier_detector   → anomaly_supervisor
              └──► categorical_anomaly_detector → anomaly_supervisor → END
```

#### Agent 1: `numerical_outlier_detector`
**Tool used:** `detect_numerical_outliers`

Auto-detects every numeric column (either already numeric dtype, or ≥80% of non-null values parse as float). For each column with ≥10 values, runs two methods:

- **IQR method**: fences at Q1 − 1.5×IQR and Q3 + 1.5×IQR. Any value outside is flagged.
- **Z-score method**: flags values with `|z| > 3` (i.e., more than 3 standard deviations from the mean).

Reports per column: descriptive stats (min/max/mean/std), outlier counts, example outlier values, and a qualitative verdict.

#### Agent 2: `categorical_anomaly_detector`
**Tool used:** `detect_categorical_anomalies`

Auto-detects categorical columns: string columns with <50 unique values OR unique/total ratio < 5%. For each:
- **Rare values**: flags any category appearing in <0.5% of rows
- **Invalid markers**: checks for remaining placeholder strings (`"Unknown"`, `"N/A"`, `"?"`, `"ND"`, etc.) that may have survived earlier fixes

Reports per column: top-5 most frequent values, rare values with counts, and a verdict.

---

### Team 5 — Remediation & Reliability

**File:** `data_quality/teams/remediation_team.py`
**Subgraph routing:** Fixed edges (no LLM supervisor)
**Produces:** `spesa_v4.csv` (copy of v3) + `quality_report.json` + `quality_report.md`

```
START → correction_suggester → reliability_scorer → END
```

This team is special: it does **not** receive a dataset path. Instead, the top-level graph node (`call_remediation_team`) collects all findings from the message history and packages them as a JSON string. Both agents receive this consolidated findings payload.

#### How findings are collected

```python
# Inside call_remediation_team (graph.py)
for m in state["messages"]:
    if isinstance(m, HumanMessage) and m.name in team_names:
        findings[m.name] = _extract_text(m.content)
all_findings_text = "\n\n".join(findings.values())
findings_payload = json.dumps({"all_findings_text": all_findings_text})
```

#### Agent 1: `correction_suggester`
**Tool used:** `generate_correction_suggestions`

Parses the findings JSON and produces a prioritized list of fix recommendations:

| Priority | Category | Examples |
|---|---|---|
| 1 | Remove | Drop >95% empty columns; drop duplicate columns/rows |
| 2 | Rename/Restructure | Fix snake_case violations; convert hyphens to underscores |
| 3 | Type coercion | Strip currency symbols; convert placeholders to NaN; normalise dates |
| 4 | Impute/Fill | Fill numeric NaN with median; fill categorical NaN with 'Unknown' |
| 5 | Standardise | Unify date formats; normalise casing; map rare categories to 'Other' |
| 6 | Investigate | Near-duplicate rows; outliers; cross-column violations |

#### Agent 2: `reliability_scorer`
**Tool used:** `calculate_reliability_score`

Computes a 0–100 numeric score by applying deductions to a perfect 100. See [Scoring Formula](#scoring-formula).
Presents the result as: Final Score → Deduction Breakdown → Score Improvement Roadmap → Expected Post-Fix Score.

#### After the subgraph completes (`call_remediation_team` in `graph.py`)

1. Copies `spesa_v3.csv` → `spesa_v4.csv` (final cleaned dataset)
2. Builds a full `report` dict with all team findings
3. Saves `data/quality_report.json`
4. Calls `_build_markdown(report)` and saves `data/quality_report.md`

---

## Tools Reference

All `@tool`-decorated functions are callable by LLM agents via the ReAct pattern.

| Tool | File | What it reads | What it returns |
|---|---|---|---|
| `validate_data_types` | `schema_tools.py` | CSV via pandas (dtype=str) | JSON: `{issues: {col: [message]}, total_rows, status}` |
| `check_naming_conventions` | `schema_tools.py` | First 200 rows of CSV | JSON: `{naming_issues, duplicate_columns, status}` |
| `detect_missing_values` | `completeness_tools.py` | CSV (placeholders → NaN) | JSON: `{missing_per_column, rows_with_any_null, total_rows, status}` |
| `calculate_completeness_rate` | `completeness_tools.py` | CSV (placeholders → NaN) | JSON: `{completeness_per_column_pct, overall_completeness_pct}` |
| `detect_sparse_columns` | `completeness_tools.py` | CSV (placeholders → NaN) | JSON: `{sparse_columns: {col: missing_pct}, threshold_pct}` |
| `check_format_consistency` | `consistency_tools.py` | Full CSV (dtype=str) | JSON: `{period_cols, date_cols, issues, status}` |
| `check_cross_column_logic` | `consistency_tools.py` | Full CSV (dtype=str) | JSON: `{issues: {col: {problem, count, examples}}, status}` |
| `detect_duplicates` | `consistency_tools.py` | Full CSV (dtype=str) | JSON: `{issues: {exact_duplicates, near_duplicates_on_key_cols}, status}` |
| `detect_numerical_outliers` | `anomaly_tools.py` | Full CSV (default dtypes) | JSON: `{numeric_columns_checked, findings: {col: {iqr_outliers, zscore_outliers}}}` |
| `detect_categorical_anomalies` | `anomaly_tools.py` | Full CSV (dtype=str) | JSON: `{categorical_columns_detected, findings: {col: {rare_values, invalid_markers}}}` |
| `generate_correction_suggestions` | `remediation_tools.py` | JSON string of all findings | JSON: `{correction_suggestions: [{field, issue, action}]}` |
| `calculate_reliability_score` | `remediation_tools.py` | JSON string of all findings | JSON: `{reliability_score, grade, deductions, interpretation}` |

### Path resolution safety net

`consistency_tools.py` and `anomaly_tools.py` include a `_resolve_csv_path()` helper. If the LLM passes a slightly hallucinated filename (e.g., `spesa_v2.2.csv` instead of `spesa_v2.csv`), the helper:
1. Strips decimal sub-version suffixes: `spesa_v2.2` → `spesa_v2`
2. Falls back to the most-recently-modified CSV in the same directory matching the base name

---

## Fix Functions Reference

Fix functions are **not** callable by LLM agents. They are plain Python called directly by graph node wrappers in `graph.py` after a team's subgraph completes. They are fully deterministic — no LLM involved.

| Function | File | Input → Output |
|---|---|---|
| `apply_schema_fixes(in, out, profile)` | `schema_tools.py` | original CSV → `_v1.csv` |
| `apply_completeness_fixes(in, out, profile)` | `completeness_tools.py` | `_v1.csv` → `_v2.csv` |
| `apply_consistency_fixes(in, out)` | `consistency_tools.py` | `_v2.csv` → `_v3.csv` |
| `build_final_report(...)` | `remediation_tools.py` | all findings dicts → report dict |

---

## Scoring Formula

The reliability score starts at **100** and deductions are subtracted. Per-column deductions are scaled by `col_scale = min(10 / column_count, 1.0)` so wide datasets are not over-penalised.

| Dimension | Deduction logic | Cap |
|---|---|---|
| **Schema** | −3 pts × type issues (scaled) + −1 pt × naming/duplicate issues (scaled) | −20 pts |
| **Completeness** | −(100 − overall_completeness_pct) × 0.5 | none |
| **Consistency** | −3 pts × format issues (scaled); −5 pts for any cross-column violations; −min(duplicate_count/100, 10) | varies |
| **Anomaly** | −min(outlier_count/50, 10) per column with IQR outliers | −10 pts/col |

**Grade thresholds:**

| Grade | Score range |
|---|---|
| A | ≥ 90 |
| B | ≥ 75 |
| C | ≥ 60 |
| D | ≥ 45 |
| F | < 45 |

Score floors at 0.

---

## Output Files

For a dataset named `spesa.csv`, after a successful run:

| File | Contents |
|---|---|
| `data/spesa_v1.csv` | After schema fixes (dedup columns, type coercion, renames) |
| `data/spesa_v2.csv` | After completeness fixes (null fill, sparse column drop) |
| `data/spesa_v3.csv` | After consistency fixes (date normalisation, duplicate row removal) |
| `data/spesa_v4.csv` | Final dataset (copy of v3, for auditability) |
| `data/quality_report.json` | Machine-readable report: all findings + score + suggestions |
| `data/quality_report.md` | Human-readable Markdown report with tables and executive summary |

---

## File Structure

```
ML_MiSoMa_reply2026/
├── main.py                          # CLI entry point (argparse, UTF-8 output fix)
├── requirements.txt
├── .env                             # GOOGLE_API_KEY (not committed)
│
├── data/
│   ├── spesa.csv                    # original dataset
│   ├── spesa_v1.csv                 # post-schema
│   ├── spesa_v2.csv                 # post-completeness
│   ├── spesa_v3.csv                 # post-consistency
│   ├── spesa_v4.csv                 # final
│   ├── quality_report.json
│   └── quality_report.md
│
└── data_quality/
    ├── __init__.py
    ├── config.py                    # LLM factory + GOOGLE_API_KEY + PLACEHOLDER_VALUES
    ├── state.py                     # DataQualityState, TeamState
    ├── graph.py                     # top supervisor + team node wrappers + report builder
    │
    ├── teams/
    │   ├── schema_team.py           # 2 workers, fixed edges
    │   ├── completeness_team.py     # 3 workers, fixed edges
    │   ├── consistency_team.py      # 3 workers + LLM supervisor
    │   ├── anomaly_team.py          # 2 workers + LLM supervisor
    │   └── remediation_team.py      # 2 workers, fixed edges
    │
    └── tools/
        ├── schema_tools.py          # validate_data_types, check_naming_conventions, apply_schema_fixes
        ├── completeness_tools.py    # detect_missing_values, calculate_completeness_rate,
        │                            # detect_sparse_columns, apply_completeness_fixes
        ├── consistency_tools.py     # check_format_consistency, check_cross_column_logic,
        │                            # detect_duplicates, apply_consistency_fixes
        ├── anomaly_tools.py         # detect_numerical_outliers, detect_categorical_anomalies
        └── remediation_tools.py     # generate_correction_suggestions, calculate_reliability_score,
                                     # build_final_report
```

---

## LLM Configuration

**File:** `data_quality/config.py`

| Setting | Value |
|---|---|
| Model | `gemini-3.1-flash-lite-preview` via `langchain-google-genai` |
| Temperature | `0` (deterministic routing and structured output) |
| API key source | `GOOGLE_API_KEY` env var or `.env` file |

All 5 teams and the top supervisor share the same `get_llm()` factory. The LLM serves two roles:

1. **Structured output routing** — `llm.with_structured_output(Router)` returns a single `next` field; used by the top supervisor and the consistency/anomaly team supervisors.
2. **ReAct agents** — `create_react_agent(llm, tools=[...], prompt=...)` — each worker reasons about the task, calls its tool once, interprets the JSON result, and writes a natural-language findings report.

Fix functions, path resolution helpers, and report builders are **pure Python** — the LLM is never involved in data modification.
