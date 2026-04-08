"""
Schema Validation tools:
  - validate_data_types       : checks columns for type violations (generic, profile-driven)
  - check_naming_conventions  : flags naming issues and detects duplicate columns via Jaccard
  - apply_schema_fixes        : (non-tool) applies schema corrections using the dataset profile
"""

import json
import re
import warnings
import pandas as pd
from langchain_core.tools import tool
from data_quality.config import PLACEHOLDER_VALUES


@tool
def validate_data_types(dataset_path: str) -> str:
    """Check every column for data-type violations.
    For each column, attempts numeric and date coercion and flags contamination.
    Returns a JSON findings report."""
    df = pd.read_csv(dataset_path, dtype=str)
    issues: dict[str, list[str]] = {}

    for col in df.columns:
        series = df[col].dropna()
        if len(series) == 0:
            continue
        n = len(series)

        # Try numeric coercion
        numeric_parsed = pd.to_numeric(series, errors="coerce")
        numeric_ratio = numeric_parsed.notna().sum() / n

        # Try date coercion
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            date_parsed = pd.to_datetime(series, errors="coerce")
        date_ratio = date_parsed.notna().sum() / n

        if numeric_ratio >= 0.9:
            # Column is mostly numeric — flag non-numeric contamination
            bad = series[numeric_parsed.isna()]
            bad_clean = [
                v for v in bad.unique()[:10].tolist()
                if str(v).strip() not in PLACEHOLDER_VALUES
            ]
            if bad_clean:
                issues.setdefault(col, []).append(
                    f"Mostly numeric but {len(bad)} non-numeric value(s) found: {bad_clean[:5]}"
                )
        elif date_ratio >= 0.7 and numeric_ratio < 0.5:
            # Column is mostly date — flag unparseable contamination
            bad = series[date_parsed.isna()]
            bad_clean = [
                v for v in bad.unique()[:10].tolist()
                if str(v).strip() not in PLACEHOLDER_VALUES
            ]
            if bad_clean:
                issues.setdefault(col, []).append(
                    f"Mostly date/datetime but {len(bad)} unparseable value(s): {bad_clean[:5]}"
                )

    return json.dumps({
        "check": "data_type_validation",
        "issues": issues,
        "total_rows": len(df),
        "status": "issues_found" if issues else "ok",
    }, indent=2)


@tool
def check_naming_conventions(dataset_path: str) -> str:
    """Check column names for violations (uppercase, spaces, digit-start, special chars, hyphens).
    Detect likely duplicate columns using normalized-name similarity + Jaccard value overlap.
    Returns a JSON findings report."""
    df = pd.read_csv(dataset_path, nrows=200)
    cols = df.columns.tolist()
    naming_issues: dict[str, list[str]] = {}
    duplicate_cols: dict[str, str] = {}

    # Naming convention checks
    for col in cols:
        problems: list[str] = []
        stripped = col.strip()
        if stripped != stripped.lower():
            problems.append("not snake_case / contains uppercase")
        if " " in stripped:
            problems.append("contains space")
        if stripped and stripped[0].isdigit():
            problems.append("starts with digit")
        if re.search(r"[%@!#]", stripped):
            problems.append(f"contains special character ({re.findall(r'[%@!#]', stripped)})")
        if "-" in stripped:
            problems.append("contains hyphen (use underscore)")
        if problems:
            naming_issues[col] = problems

    # Duplicate column detection via normalized name + Jaccard value overlap
    normalized = {col: re.sub(r"[^a-z0-9]", "", col.lower()) for col in cols}
    checked: set[tuple] = set()
    for i, col_a in enumerate(cols):
        for col_b in cols[i + 1:]:
            pair = (col_a, col_b)
            if pair in checked:
                continue
            checked.add(pair)
            norm_a = normalized[col_a]
            norm_b = normalized[col_b]
            # Name similarity: one normalized name is a substring of the other (min length 3)
            if len(norm_a) < 3 or len(norm_b) < 3:
                continue
            name_similar = norm_a in norm_b or norm_b in norm_a
            if not name_similar:
                continue
            # Value overlap: Jaccard similarity on string-cast unique values
            vals_a = set(df[col_a].dropna().astype(str).unique())
            vals_b = set(df[col_b].dropna().astype(str).unique())
            if not vals_a or not vals_b:
                continue
            intersection = len(vals_a & vals_b)
            union = len(vals_a | vals_b)
            jaccard = intersection / union if union > 0 else 0.0
            if jaccard > 0.8:
                duplicate_cols[col_a] = (
                    f"likely duplicate of '{col_b}' "
                    f"(name overlap + {jaccard:.0%} value overlap)"
                )

    return json.dumps({
        "check": "naming_convention_check",
        "naming_issues": naming_issues,
        "duplicate_columns": duplicate_cols,
        "status": "issues_found" if (naming_issues or duplicate_cols) else "ok",
    }, indent=2)


# ---------------------------------------------------------------------------
# Fix function (called by the schema team node, not by the LLM)
# ---------------------------------------------------------------------------

def apply_schema_fixes(input_path: str, output_path: str, profile: dict) -> dict:
    """
    Applies generic schema corrections driven by the dataset profile:
      1. Drops likely-duplicate columns (first occurrence of each normalized name wins)
      2. Strips currency/unit symbols from numeric columns and coerces to float
      3. Replaces placeholder strings with NaN in numeric/identifier columns
      4. Renames hyphenated column names to use underscores
    Returns a summary dict of what was changed.
    """
    df = pd.read_csv(input_path, dtype=str)
    changes = []
    type_changed_columns: set[str] = set()
    col_profiles = profile.get("columns", {})

    # 1a. Exact normalized-name dedup: keep first occurrence of identical normalized names
    # (e.g. "Tipo Imposta" and "tipo_imposta" → same norm "tipoimposta")
    seen_norm: dict[str, str] = {}
    to_drop_exact: list[str] = []
    for col in df.columns:
        norm = re.sub(r"[^a-z0-9]", "", col.lower())
        if norm in seen_norm:
            to_drop_exact.append(col)
        else:
            seen_norm[norm] = col
    if to_drop_exact:
        df = df.drop(columns=to_drop_exact)
        changes.append(f"Dropped exact-name-duplicate columns: {to_drop_exact}")

    # 1b. Substring-name + high-value-overlap dedup:
    # catches cases like "SPESA TOTALE" ⊃ "spesa", "cod imposta ext" ⊃ "cod_imposta"
    cols_now = df.columns.tolist()
    norms_now = {col: re.sub(r"[^a-z0-9]", "", col.lower()) for col in cols_now}
    kept_cols: list[str] = []
    to_drop_sub: list[str] = []
    for col in cols_now:
        norm = norms_now[col]
        is_dup = False
        for kept_col in kept_cols:
            kept_norm = norms_now[kept_col]
            # Substring relationship: one norm contains the other (min 4 chars each)
            if min(len(norm), len(kept_norm)) < 4:
                continue
            if norm not in kept_norm and kept_norm not in norm:
                continue
            # Confirm with value-level evidence
            series_a = df[kept_col].dropna()
            series_b = df[col].dropna()
            if len(series_a) < 5 or len(series_b) < 5:
                continue
            num_a = pd.to_numeric(series_a, errors="coerce")
            num_b = pd.to_numeric(series_b, errors="coerce")
            if num_a.notna().sum() > 10 and num_b.notna().sum() > 10:
                # Numeric: use Pearson correlation on aligned rows
                aligned = pd.concat([num_a.rename("a"), num_b.rename("b")], axis=1).dropna()
                if len(aligned) > 10:
                    corr = aligned["a"].corr(aligned["b"])
                    if abs(corr) > 0.85:
                        is_dup = True
                        break
            else:
                # Categorical/text: Jaccard on unique string values
                vals_a = set(series_a.astype(str).unique())
                vals_b = set(series_b.astype(str).unique())
                intersection = len(vals_a & vals_b)
                union = len(vals_a | vals_b)
                if union > 0 and intersection / union > 0.8:
                    is_dup = True
                    break
        if is_dup:
            to_drop_sub.append(col)
        else:
            kept_cols.append(col)
    if to_drop_sub:
        df = df.drop(columns=to_drop_sub)
        changes.append(f"Dropped likely-duplicate columns: {to_drop_sub}")

    # 2. Strip currency/unit symbols from numeric columns and coerce to float
    CURRENCY_PATTERNS = [
        ("€", ""), ("EUR", ""), ("\ufffd", ""),  # corrupted euro sign
        ("$", ""), ("£", ""), ("¥", ""), ("%", ""),
    ]
    for col in df.columns:
        cp = col_profiles.get(col, {})
        if cp.get("semantic_type") == "numeric":
            original = df[col].copy()
            for symbol, replacement in CURRENCY_PATTERNS:
                df[col] = df[col].str.replace(symbol, replacement, regex=False)
            df[col] = df[col].str.strip()
            coerced = pd.to_numeric(df[col], errors="coerce")
            original_coerced = pd.to_numeric(original, errors="coerce")
            if not coerced.equals(original_coerced):
                changes.append(f"Stripped currency/unit symbols and coerced '{col}' to float")
                type_changed_columns.add(col)
            # Preserve integer representation — avoid YYYYMM → YYYYMM.0
            if coerced.dropna().apply(lambda x: x == int(x)).all():
                df[col] = coerced.astype("Int64")
            else:
                df[col] = coerced

    # 3. Replace placeholder strings with NaN in numeric/identifier columns.
    # Keep identifier columns as strings: coercing IDs (e.g., Mongo-like hex keys)
    # to numeric would turn valid values into NaN and break downstream completeness.
    for col in df.columns:
        cp = col_profiles.get(col, {})
        if cp.get("semantic_type") in ("numeric", "identifier"):
            before = df[col].isna().sum()
            df[col] = df[col].apply(
                lambda x: pd.NA if pd.notna(x) and str(x).strip() in PLACEHOLDER_VALUES else x
            )
            after = df[col].isna().sum()
            if after > before:
                changes.append(
                    f"Replaced {after - before} placeholder string(s) with NaN in '{col}'"
                )
            if cp.get("semantic_type") == "numeric":
                df[col] = pd.to_numeric(df[col], errors="coerce")
                type_changed_columns.add(col)

    # 4a. Year-shorthand expansion — only for columns the LLM enricher explicitly flagged.
    # The enricher checks column name + sample values; we then expand the FULL column
    # (not just the sample) to catch any 2-digit values not in the sample.
    enrichments = profile.get("enrichments", {})

    def _expand_year(x):
        if pd.isna(x):
            return x
        y = int(x)
        if 1900 <= y <= 2100:
            return y
        if 0 <= y < 30:
            return 2000 + y
        if 30 <= y < 100:
            return 1900 + y
        return x  # out of range — leave unchanged

    for col in df.columns:
        if enrichments.get(col, {}).get("fix_action") != "coerce_year":
            continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        valid = numeric.dropna()
        if len(valid) < 5:
            continue
        if valid.apply(lambda x: 0 <= x < 100).any():  # has actual shorthand values
            df[col] = numeric.apply(_expand_year).astype("Int64")
            changes.append(f"Expanded 2-digit year shorthand in '{col}' to 4-digit integers")
            type_changed_columns.add(col)

    # 4b. Apply other LLM semantic enrichment fixes (round_Ndp, coerce_int).
    for col in df.columns:
        enr = enrichments.get(col, {})
        action = enr.get("fix_action", "none")
        params = enr.get("params", {})
        if action in ("none", "coerce_year") or not action:
            continue

        if action == "round_Ndp":
            decimals = int(params.get("decimals", 2))
            numeric = pd.to_numeric(df[col], errors="coerce")
            if numeric.notna().any():
                df[col] = numeric.round(decimals)
                changes.append(f"Rounded '{col}' to {decimals} decimal place(s)")
                type_changed_columns.add(col)

        elif action == "coerce_int":
            numeric = pd.to_numeric(df[col], errors="coerce")
            valid = numeric.dropna()
            if len(valid) > 0 and (valid % 1 == 0).all():
                df[col] = numeric.apply(
                    lambda x: int(x) if pd.notna(x) else pd.NA
                ).astype("Int64")
                changes.append(f"Stripped decimal suffix from integer column '{col}'")
                type_changed_columns.add(col)

    # 5. Rename hyphenated column names to use underscores
    rename_map = {col: col.replace("-", "_") for col in df.columns if "-" in col}
    if rename_map:
        df = df.rename(columns=rename_map)
        changes.append(f"Renamed hyphenated columns: {rename_map}")

    # 6. Coerce whole-number numeric columns to Int64 (avoids .0 suffix in CSV output).
    # Any column profiled as numeric where every non-null value is a whole number should
    # be stored as integer — not float. This includes counts, codes, periods, years, etc.
    for col in df.columns:
        cp = col_profiles.get(col, {})
        if cp.get("semantic_type") != "numeric":
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        non_null = series.dropna()
        if len(non_null) == 0:
            continue
        if (non_null % 1 == 0).all():
            df[col] = series.apply(lambda x: int(x) if pd.notna(x) else pd.NA).astype("Int64")
            type_changed_columns.add(col)

    df.to_csv(output_path, index=False, encoding="utf-8")
    return {
        "fixes_applied": changes,
        "output_path": output_path,
        "types_changed_count": len(type_changed_columns),
    }
