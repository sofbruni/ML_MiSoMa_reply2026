"""
Consistency Validation tools:
  - check_format_consistency  : detects mixed formats in ALL date-like and categorical columns
  - check_cross_column_logic  : auto-detects code-label pairs and flags negative amounts
  - detect_duplicates         : finds exact and near-duplicate rows with auto-detected key cols
  - apply_consistency_fixes   : (non-tool) normalises date formats and removes duplicates
"""

import json
import math
import re
import pandas as pd
from langchain_core.tools import tool


def _safe_json(obj, **kwargs) -> str:
    """json.dumps that replaces float NaN/Inf with None (valid JSON null)."""
    def _sanitize(o):
        if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
            return None
        if isinstance(o, dict):
            return {k: _sanitize(v) for k, v in o.items()}
        if isinstance(o, list):
            return [_sanitize(v) for v in o]
        return o
    return json.dumps(_sanitize(obj), **kwargs)


def _safe_dict_list(df_slice) -> list:
    """Convert a DataFrame slice to a list of dicts, replacing NaN with None."""
    records = df_slice.to_dict("records")
    return [
        {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
        for row in records
    ]


# Date/period regex patterns used for format detection
_DATE_PATTERNS = [
    (r"^\d{4}-\d{2}-\d{2}T", "ISO-8601 datetime"),
    (r"^\d{4}-\d{2}-\d{2}$", "YYYY-MM-DD"),
    (r"^\d{4}/\d{2}/\d{2}$", "YYYY/MM/DD"),
    (r"^\d{2}/\d{2}/\d{4}$", "DD/MM/YYYY"),
    (r"^\d{2}\.\d{2}\.\d{4}$", "DD.MM.YYYY"),
    (r"^\d{2}-\d{2}-\d{2}$", "DD-MM-YY"),
    (r"^\d{6}$", "YYYYMM"),
    (r"^\d{4}-\d{2}$", "YYYY-MM"),
    (r"^\d{2}/\d{4}$", "MM/YYYY"),
    (r"^[A-Z]{3}-\d{4}$", "MON-YYYY"),
]

# Language-neutral financial/quantity keywords that suggest a column should be non-negative.
# Used as an escalation signal (higher priority flag).
# All numeric columns are checked for negatives regardless of name (lower priority).
_AMOUNT_KEYWORDS = {
    # English
    "amount", "total", "count", "qty", "quantity", "price", "cost",
    "revenue", "value", "salary", "wage", "payment", "fee", "tax",
    "income", "expense", "balance", "sum", "rate",
    # French
    "montant", "cout", "revenu", "prix", "salaire",
    # German
    "betrag", "kosten", "preis", "gehalt",
    # Spanish
    "cantidad", "costo", "ingreso", "precio",
}


@tool
def check_format_consistency(dataset_path: str) -> str:
    """Check that values follow a consistent format within each column.
    For ALL columns: detects mixed date/period formats dynamically.
    For low-cardinality columns: flags case and whitespace inconsistencies.
    Returns a JSON findings report."""
    df = pd.read_csv(dataset_path, dtype=str)
    issues: dict[str, dict] = {}

    for col in df.columns:
        series = df[col].dropna()
        if len(series) == 0:
            continue

        col_issues: dict = {}

        # --- Date/period format check ---
        # Count how many values match each pattern
        fmt_counts: dict[str, int] = {}
        date_match_count = 0
        for v in series:
            v_str = str(v).strip()
            matched = False
            for pattern, fmt_name in _DATE_PATTERNS:
                if re.match(pattern, v_str):
                    fmt_counts[fmt_name] = fmt_counts.get(fmt_name, 0) + 1
                    date_match_count += 1
                    matched = True
                    break
            if not matched:
                fmt_counts["other"] = fmt_counts.get(fmt_name := "other", 0) + 1

        date_ratio = date_match_count / len(series)
        named_formats = [f for f in fmt_counts if f != "other"]

        # Flag if >50% date-like AND multiple named formats are present
        if date_ratio > 0.5 and len(named_formats) > 1:
            col_issues["mixed_date_formats"] = {
                "problem": "Mixed date/period formats",
                "format_distribution": fmt_counts,
                "examples": series.unique()[:5].tolist(),
            }

        # --- Categorical consistency: low-cardinality string columns ---
        n_unique = series.nunique()
        if n_unique <= 30:
            # Whitespace inconsistency: same value with/without surrounding spaces
            if series.str.strip().nunique() < n_unique:
                inconsistent = series[series != series.str.strip()].unique()[:5].tolist()
                if inconsistent:
                    col_issues["whitespace_inconsistency"] = {
                        "problem": "Values differ only by leading/trailing whitespace",
                        "examples": inconsistent,
                    }
            # Case inconsistency: same value with different casing
            if series.str.strip().str.lower().nunique() < series.str.strip().nunique():
                lower_stripped = series.str.strip().str.lower()
                # Find groups where the same lower value has multiple case variants
                case_groups = (
                    series.str.strip()
                    .groupby(lower_stripped)
                    .apply(lambda g: g.unique().tolist())
                )
                mixed = {k: v for k, v in case_groups.items() if len(v) > 1}
                if mixed:
                    col_issues["case_inconsistency"] = {
                        "problem": "Values differ only by case",
                        "examples": dict(list(mixed.items())[:5]),
                    }

        if col_issues:
            issues[col] = col_issues

    return _safe_json({
        "check": "format_consistency",
        "issues": issues,
        "status": "issues_found" if issues else "ok",
    }, indent=2)


@tool
def check_cross_column_logic(dataset_path: str) -> str:
    """Validate logical relationships between columns without hardcoded business rules:
    1. Detects code→label column pairs (numeric with cardinality 2–15 paired with
       low-cardinality categorical) and checks that the mapping is 1-to-1.
    2. Flags negative values in columns whose name suggests a non-negative amount.
    Returns a JSON findings report."""
    df = pd.read_csv(dataset_path, dtype=str)
    issues: dict = {}

    # --- Auto-detect code columns and label columns ---
    candidate_codes: list[str] = []
    candidate_labels: list[str] = []

    for col in df.columns:
        series = df[col].dropna()
        if len(series) == 0:
            continue
        numeric = pd.to_numeric(series, errors="coerce")
        num_ratio = numeric.notna().sum() / len(series)
        cardinality = int(numeric.dropna().nunique())

        if num_ratio >= 0.9 and 2 <= cardinality <= 15:
            candidate_codes.append(col)
        elif num_ratio < 0.5 and df[col].nunique() <= 20:
            candidate_labels.append(col)

    # Check 1-to-1 consistency for each (code, label) pair
    for code_col in candidate_codes:
        for label_col in candidate_labels:
            if code_col == label_col:
                continue
            grouped = df.groupby(code_col)[label_col].nunique()
            inconsistent = grouped[grouped > 1]
            if len(inconsistent) > 0:
                pct = round(len(inconsistent) / len(grouped) * 100, 1)
                pair_key = f"{code_col}<->{label_col}"
                issues[pair_key] = {
                    "problem": (
                        f"Code column '{code_col}' maps to multiple values in '{label_col}'"
                    ),
                    "inconsistent_code_count": int(len(inconsistent)),
                    "pct_inconsistent": pct,
                    "examples": (
                        df[df[code_col].isin(inconsistent.index)][[code_col, label_col]]
                        .drop_duplicates()
                        .head(5)
                        .to_dict("records")
                    ),
                }

    # --- Period-code consistency check ---
    # Auto-detect YYYYMM period-code columns (6-digit integers: first 4 = year, last 2 = month).
    # For each one, find candidate year columns (4-digit integers 1900-2100) and month columns
    # (integers 1-12) and verify they are consistent with the period code.
    # Fully generic — no hardcoded column names.
    period_code_cols: list[str] = []
    for col in df.columns:
        series = df[col].dropna()
        if len(series) < 5:
            continue
        numeric = pd.to_numeric(series, errors="coerce").dropna()
        if numeric.notna().sum() / len(series) < 0.9:
            continue
        int_vals = numeric.astype(int)
        valid_yyyymm = int_vals.apply(
            lambda x: len(str(abs(x))) == 6
            and 1900 <= x // 100 <= 2100
            and 1 <= x % 100 <= 12
        )
        if valid_yyyymm.mean() >= 0.9:
            period_code_cols.append(col)

    for period_col in period_code_cols:
        period_numeric = pd.to_numeric(df[period_col], errors="coerce")

        for other_col in df.columns:
            if other_col == period_col:
                continue
            other_numeric = pd.to_numeric(df[other_col], errors="coerce")
            if other_numeric.notna().mean() < 0.9:
                continue

            aligned = pd.concat(
                [period_numeric.rename("period"), other_numeric.rename("other")], axis=1
            ).dropna()
            if len(aligned) < 5:
                continue

            period_int = aligned["period"].astype(int)
            other_int = aligned["other"].astype(int)

            # Year column: 4-digit values in 1900-2100 range
            if other_int.apply(lambda x: 1900 <= x <= 2100).mean() >= 0.9:
                expected = period_int // 100
                mismatch = expected != other_int
                mismatch_count = int(mismatch.sum())
                if mismatch_count > 0:
                    pct = round(mismatch_count / len(aligned) * 100, 1)
                    issues[f"{period_col}↔{other_col}(year)"] = {
                        "problem": (
                            f"Year column '{other_col}' is inconsistent with the year "
                            f"portion of period code '{period_col}' (YYYYMM // 100)"
                        ),
                        "mismatch_count": mismatch_count,
                        "pct_mismatch": pct,
                        "examples": _safe_dict_list(
                            df[[period_col, other_col]].loc[mismatch[mismatch].index[:5]]
                        ),
                    }

            # Month column: integers 1-12
            elif other_int.apply(lambda x: 1 <= x <= 12).mean() >= 0.9:
                expected = period_int % 100
                mismatch = expected != other_int
                mismatch_count = int(mismatch.sum())
                if mismatch_count > 0:
                    pct = round(mismatch_count / len(aligned) * 100, 1)
                    issues[f"{period_col}↔{other_col}(month)"] = {
                        "problem": (
                            f"Month column '{other_col}' is inconsistent with the month "
                            f"portion of period code '{period_col}' (YYYYMM % 100)"
                        ),
                        "mismatch_count": mismatch_count,
                        "pct_mismatch": pct,
                        "examples": _safe_dict_list(
                            df[[period_col, other_col]].loc[mismatch[mismatch].index[:5]]
                        ),
                    }

    # --- Negative value check: all numeric columns ---
    # Priority HIGH if name matches a financial/quantity keyword.
    # Priority MODERATE for all other numeric columns (unexpected negatives may still be errors).
    for col in df.columns:
        numeric = pd.to_numeric(df[col], errors="coerce")
        valid = numeric.dropna()
        if len(valid) < 5:
            continue
        valid_ratio = numeric.notna().sum() / len(df[col].dropna())
        if valid_ratio < 0.9:
            continue  # not a numeric column
        negative = numeric[numeric < 0]
        if len(negative) == 0:
            continue
        keyword_match = any(kw in col.lower() for kw in _AMOUNT_KEYWORDS)
        issues[col] = {
            "problem": (
                f"Negative values in '{col}' "
                f"({'FINANCIAL KEYWORD MATCH — expected non-negative' if keyword_match else 'numeric column with unexpected negatives'})"
            ),
            "priority": "HIGH" if keyword_match else "MODERATE",
            "count": int(len(negative)),
            "pct_of_rows": round(len(negative) / len(df) * 100, 2),
            "examples": negative.head(5).round(2).tolist(),
        }

    return _safe_json({
        "check": "cross_column_logic",
        "issues": issues,
        "status": "issues_found" if issues else "ok",
    }, indent=2)


@tool
def detect_duplicates(dataset_path: str) -> str:
    """Identify exact duplicate rows and near-duplicates on auto-detected key columns.
    Key columns are chosen by cardinality ratio (moderate uniqueness, not free text).
    Returns a JSON findings report."""
    df = pd.read_csv(dataset_path, dtype=str)
    issues: dict = {}
    n = len(df)

    # Exact duplicates
    exact_dupes = int(df.duplicated().sum())
    if exact_dupes > 0:
        issues["exact_duplicates"] = {
            "count": exact_dupes,
            "examples": df[df.duplicated()].head(3).to_dict("records"),
        }

    # Auto-detect key columns: moderate cardinality, not free-text
    key_candidates: list[tuple[str, float]] = []
    for col in df.columns:
        series = df[col].dropna()
        if len(series) == 0:
            continue
        cardinality_ratio = series.nunique() / n
        avg_len = series.astype(str).str.len().mean()
        if 0.05 <= cardinality_ratio <= 0.95 and avg_len < 30:
            key_candidates.append((col, cardinality_ratio))

    # Pick top 4 by highest cardinality ratio
    key_candidates.sort(key=lambda x: x[1], reverse=True)
    key_cols = [col for col, _ in key_candidates[:4]]

    if key_cols:
        near_dupes = df[df.duplicated(subset=key_cols, keep=False)]
        if len(near_dupes) > 0:
            issues["near_duplicates_on_key_cols"] = {
                "key_columns": key_cols,
                "affected_rows": len(near_dupes),
                "examples": near_dupes.head(3).to_dict("records"),
            }

    return _safe_json({
        "check": "duplicate_detection",
        "issues": issues,
        "status": "issues_found" if issues else "ok",
    }, indent=2)


# ---------------------------------------------------------------------------
# Fix function (called by the consistency team node, not by the LLM)
# ---------------------------------------------------------------------------

def _resolve_profile(col: str, col_profiles: dict) -> dict:
    """Look up a column's profile entry, accounting for renames applied by schema fixes.
    Schema fixes may have renamed 'col-name' → 'col_name', so we try multiple keys."""
    if col in col_profiles:
        return col_profiles[col]
    # Try reversing hyphen→underscore rename
    hyphenated = col.replace("_", "-")
    if hyphenated in col_profiles:
        return col_profiles[hyphenated]
    # Try any profile key whose normalized form matches
    import re
    col_norm = re.sub(r"[^a-z0-9]", "", col.lower())
    for key, cp in col_profiles.items():
        if re.sub(r"[^a-z0-9]", "", key.lower()) == col_norm:
            return cp
    return {}


def apply_consistency_fixes(input_path: str, output_path: str, profile: dict) -> dict:
    """
    Applies generic consistency corrections driven by the dataset profile:
      1. Normalises ALL detected date columns to ISO-8601 (YYYY-MM-DD) using pandas
      2. Strips leading/trailing whitespace and normalises case in categorical/text columns
      3. Removes exact duplicate rows
    Returns a summary dict.
    """
    import warnings
    df = pd.read_csv(input_path, dtype=str)
    changes = []
    col_profiles = profile.get("columns", {})

    # 1. Normalise date columns → YYYY-MM-DD
    # Uses _resolve_profile to handle columns renamed by schema fixes (e.g. aggregation-time →
    # aggregation_time). Also falls back to pd.to_datetime heuristic for any column that parses
    # as ≥70% dates even if not in the profile.
    date_cols = []
    for col in df.columns:
        cp = _resolve_profile(col, col_profiles)
        if cp.get("semantic_type") == "date":
            date_cols.append(col)
        elif not cp:
            # Column not in profile (e.g. added after profiling) — try heuristic
            series = df[col].dropna()
            if len(series) == 0:
                continue
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                parsed_ratio = pd.to_datetime(series, errors="coerce").notna().sum() / len(series)
            if parsed_ratio >= 0.7:
                date_cols.append(col)
    # Italian month abbreviation → English, so pd.to_datetime can parse them
    _IT_MONTHS = {
        "GEN": "Jan", "FEB": "Feb", "MAR": "Mar", "APR": "Apr",
        "MAG": "May", "GIU": "Jun", "LUG": "Jul", "AGO": "Aug",
        "SET": "Sep", "OTT": "Oct", "NOV": "Nov", "DIC": "Dec",
    }
    _it_pattern = re.compile(
        r'\b(' + '|'.join(_IT_MONTHS) + r')\b', re.IGNORECASE
    )

    def _translate_italian(val: str) -> str:
        return _it_pattern.sub(lambda m: _IT_MONTHS[m.group().upper()], val)

    for col in date_cols:
        original = df[col].copy()
        # Pre-translate Italian month abbreviations before parsing
        series = df[col].where(df[col].isna(), df[col].astype(str).apply(_translate_italian))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            parsed = pd.to_datetime(series, errors="coerce", format="mixed", dayfirst=True)
        # Write back only where parsing succeeded; leave unparseable values unchanged
        mask = parsed.notna()
        df.loc[mask, col] = parsed[mask].dt.strftime("%Y-%m-%d")
        changed = int((df[col] != original).sum())
        if changed:
            changes.append(f"Normalised {changed} value(s) in '{col}' to YYYY-MM-DD")

    # 2. Strip whitespace and normalise case variants in categorical and text columns
    str_cols = [
        col for col in df.columns
        if _resolve_profile(col, col_profiles).get("semantic_type") in ("categorical", "text")
    ]
    for col in str_cols:
        original = df[col].copy()
        # 2a. Strip leading/trailing whitespace
        df[col] = df[col].str.strip()

        # 2b. Case normalisation: for each group of values that differ only in case,
        #     replace all variants with the most common form.
        #     Example: "erariali", "ERARIALI", "Erariali" → whichever appears most.
        series = df[col].dropna()
        if len(series) == 0:
            continue
        lower_series = series.str.lower()
        unique_lower = lower_series.unique()
        # Only process if there are actually multiple case variants
        if series.nunique() > lower_series.nunique():
            canonical_map: dict[str, str] = {}
            for lv in unique_lower:
                mask = lower_series == lv
                variants = series[mask].value_counts()
                canonical_map[lv] = variants.index[0]  # most frequent casing wins
            df[col] = df[col].apply(
                lambda x: canonical_map.get(str(x).strip().lower(), x)
                if pd.notna(x) else x
            )

        changed = int((df[col] != original).fillna(False).sum())
        if changed:
            changes.append(f"Standardised {changed} value(s) in '{col}' (whitespace/case)")

    # 3. Drop exact duplicates
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    if removed:
        changes.append(f"Removed {removed} exact duplicate row(s)")

    df.to_csv(output_path, index=False, encoding="utf-8")
    return {
        "fixes_applied": changes,
        "output_path": output_path,
        "rows_removed": removed,
    }
