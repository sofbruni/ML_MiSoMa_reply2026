"""
Consistency Validation tools:
  - check_format_consistency  : detects mixed formats in dates and rata
  - check_cross_column_logic  : validates logical relationships between columns
  - detect_duplicates         : finds exact and near-duplicate rows
  - apply_consistency_fixes   : (non-tool) normalizes formats and removes duplicates
"""

import json
import re
import pandas as pd
from langchain_core.tools import tool


# Valid categories for tipo_imposta
VALID_TIPO_IMPOSTA = {"Erariali", "Previdenziali", "Varie", "Netto"}

# Canonical rata format: 6-digit YYYYMM
RATA_PATTERN = re.compile(r"^\d{6}$")

# Month name → number mapping (Italian)
MONTH_IT = {
    "GEN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAG": "05", "GIU": "06", "LUG": "07", "AGO": "08",
    "SET": "09", "OTT": "10", "NOV": "11", "DIC": "12",
}


@tool
def check_format_consistency(dataset_path: str) -> str:
    """Check that values follow a consistent format within each column
    (focus: rata period codes and aggregation_time date strings).
    Returns a JSON findings report."""
    df = pd.read_csv(dataset_path, dtype=str)
    issues: dict[str, dict] = {}

    # --- rata format check ---
    rata_col = "rata"
    if rata_col in df.columns:
        invalid = df[~df[rata_col].str.match(r"^\d{6}$", na=False)][rata_col]
        if len(invalid) > 0:
            format_counts: dict[str, int] = {}
            for v in invalid.dropna():
                if re.match(r"^\d{4}-\d{2}$", v):
                    fmt = "YYYY-MM"
                elif re.match(r"^\d{2}/\d{4}$", v):
                    fmt = "MM/YYYY"
                elif re.match(r"^[A-Z]{3}-\d{4}$", v):
                    fmt = "MON-YYYY (Italian abbrev)"
                elif re.match(r"^Rata \d{4}$", v, re.IGNORECASE):
                    fmt = "Rata YYYY (text)"
                else:
                    fmt = "other"
                format_counts[fmt] = format_counts.get(fmt, 0) + 1
            issues[rata_col] = {
                "problem": "Mixed rata formats (expected YYYYMM)",
                "invalid_count": len(invalid),
                "detected_formats": format_counts,
                "examples": invalid.unique()[:5].tolist(),
            }

    # --- aggregation_time / aggregation-time format check ---
    date_col = next((c for c in df.columns if "aggregation" in c.lower()), None)
    if date_col:
        fmt_counts: dict[str, int] = {}
        for v in df[date_col].dropna():
            if "T" in str(v):
                fmt = "ISO-8601 (YYYY-MM-DDTHH:MM:SS)"
            elif re.match(r"^\d{4}-\d{2}-\d{2}$", str(v)):
                fmt = "YYYY-MM-DD"
            elif re.match(r"^\d{4}/\d{2}/\d{2}$", str(v)):
                fmt = "YYYY/MM/DD"
            elif re.match(r"^\d{2}/\d{2}/\d{4}$", str(v)):
                fmt = "DD/MM/YYYY"
            elif re.match(r"^\d{2}\.\d{2}\.\d{4}$", str(v)):
                fmt = "DD.MM.YYYY"
            elif re.match(r"^\d{2}-\d{2}-\d{2}$", str(v)):
                fmt = "DD-MM-YY"
            else:
                fmt = f"other ({str(v)[:12]})"
            fmt_counts[fmt] = fmt_counts.get(fmt, 0) + 1
        if len(fmt_counts) > 1:
            issues[date_col] = {
                "problem": "Mixed date formats",
                "format_distribution": fmt_counts,
            }

    # --- tipo_imposta: unexpected categories ---
    if "tipo_imposta" in df.columns:
        unexpected = df[~df["tipo_imposta"].isin(VALID_TIPO_IMPOSTA)]["tipo_imposta"].dropna().unique().tolist()
        if unexpected:
            issues["tipo_imposta"] = {
                "problem": "Unexpected category values",
                "unexpected_values": unexpected,
                "expected": list(VALID_TIPO_IMPOSTA),
            }

    return json.dumps({
        "check": "format_consistency",
        "issues": issues,
        "status": "issues_found" if issues else "ok",
    }, indent=2)


@tool
def check_cross_column_logic(dataset_path: str) -> str:
    """Validate logical relationships between columns:
      - cod_tipoimposta must match tipo_imposta category
      - spesa must be a positive float
    Returns a JSON findings report."""
    df = pd.read_csv(dataset_path, dtype=str)
    issues: dict[str, list] = {}

    # cod_tipoimposta ↔ tipo_imposta mapping
    expected_mapping = {1: "Netto", 2: "Erariali", 3: "Previdenziali", 4: "Varie"}
    if "cod_tipoimposta" in df.columns and "tipo_imposta" in df.columns:
        df["_cod"] = pd.to_numeric(df["cod_tipoimposta"], errors="coerce")
        mismatches = df.apply(
            lambda r: (
                r["tipo_imposta"] != expected_mapping.get(r["_cod"])
                and pd.notna(r["_cod"])
                and r["tipo_imposta"] not in ("Da definire", "Unknown")
            ),
            axis=1,
        )
        n_mismatch = int(mismatches.sum())
        if n_mismatch > 0:
            issues["cod_tipoimposta<->tipo_imposta"] = {
                "problem": "cod_tipoimposta does not match tipo_imposta",
                "mismatch_count": n_mismatch,
                "examples": df[mismatches][["cod_tipoimposta", "tipo_imposta"]].head(5).to_dict("records"),
            }
        df.drop(columns=["_cod"], inplace=True)

    # spesa must be positive
    if "spesa" in df.columns:
        df["_spesa"] = pd.to_numeric(df["spesa"], errors="coerce")
        negative = df[df["_spesa"] < 0]
        if len(negative) > 0:
            issues["spesa"] = {
                "problem": "Negative spesa values",
                "count": len(negative),
                "examples": df["_spesa"][df["_spesa"] < 0].head(5).tolist(),
            }
        df.drop(columns=["_spesa"], inplace=True)

    return json.dumps({
        "check": "cross_column_logic",
        "issues": issues,
        "status": "issues_found" if issues else "ok",
    }, indent=2)


@tool
def detect_duplicates(dataset_path: str) -> str:
    """Identify exact duplicate rows and near-duplicates based on
    key columns (rata, ente, cod_imposta). Returns a JSON findings report."""
    df = pd.read_csv(dataset_path, dtype=str)
    issues: dict[str, dict] = {}

    # Exact duplicates
    exact_dupes = int(df.duplicated().sum())
    if exact_dupes > 0:
        issues["exact_duplicates"] = {
            "count": exact_dupes,
            "examples": df[df.duplicated()].head(3).to_dict("records"),
        }

    # Near-duplicates: same (rata, ente, cod_imposta) combination
    key_cols = [c for c in ["rata", "ente", "cod_imposta"] if c in df.columns]
    if key_cols:
        near_dupes = df[df.duplicated(subset=key_cols, keep=False)]
        if len(near_dupes) > 0:
            issues["near_duplicates_on_key_cols"] = {
                "key_columns": key_cols,
                "affected_rows": len(near_dupes),
                "examples": near_dupes.head(3).to_dict("records"),
            }

    return json.dumps({
        "check": "duplicate_detection",
        "issues": issues,
        "status": "issues_found" if issues else "ok",
    }, indent=2)


# ---------------------------------------------------------------------------
# Fix function (called by the consistency team node, not by the LLM)
# ---------------------------------------------------------------------------

def apply_consistency_fixes(input_path: str, output_path: str) -> dict:
    """
    Applies consistency corrections:
      1. Normalises all rata values to YYYYMM integer format
      2. Normalises aggregation_time to ISO-8601
      3. Maps 'Da definire' in tipo_imposta to the correct value via cod_tipoimposta
      4. Removes exact duplicate rows
    Returns a summary dict.
    """
    df = pd.read_csv(input_path, dtype=str)
    changes = []

    # 1. Normalise rata → YYYYMM
    def _parse_rata(v: str) -> str:
        v = str(v).strip()
        if re.match(r"^\d{6}$", v):
            return v
        # YYYY-MM or YYYY/MM
        m = re.match(r"^(\d{4})[-/](\d{2})$", v)
        if m:
            return m.group(1) + m.group(2)
        # MM/YYYY
        m = re.match(r"^(\d{2})/(\d{4})$", v)
        if m:
            return m.group(2) + m.group(1)
        # MON-YYYY (Italian)
        m = re.match(r"^([A-Z]{3})-(\d{4})$", v)
        if m and m.group(1) in MONTH_IT:
            return m.group(2) + MONTH_IT[m.group(1)]
        # Rata YYYY
        m = re.match(r"^Rata (\d{4})$", v, re.IGNORECASE)
        if m:
            return m.group(1) + "00"   # year only, month unknown → 00
        return v  # leave unchanged if unrecognised

    if "rata" in df.columns:
        original = df["rata"].copy()
        df["rata"] = df["rata"].apply(_parse_rata)
        changed = int((df["rata"] != original).sum())
        if changed:
            changes.append(f"Normalised {changed} 'rata' values to YYYYMM format")

    # 2. Normalise aggregation_time to ISO date (YYYY-MM-DD)
    date_col = next((c for c in df.columns if "aggregation" in c.lower()), None)
    if date_col:
        def _parse_date(v: str) -> str:
            v = str(v).strip()
            if re.match(r"^\d{4}-\d{2}-\d{2}T", v):
                return v[:10]   # already ISO — keep date part
            if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
                return v        # already YYYY-MM-DD
            m = re.match(r"^(\d{4})/(\d{2})/(\d{2})$", v)
            if m:
                return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", v)
            if m:
                return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
            m = re.match(r"^(\d{2})-(\d{2})-(\d{2})$", v)
            if m:
                return f"20{m.group(3)}-{m.group(2)}-{m.group(1)}"
            # DD/MM/YYYY  e.g. "24/10/2024"
            m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", v)
            if m:
                return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
            # Italian text format: MMM DD YYYY  e.g. "AGO 16 2024", "GEN 11 2024"
            m = re.match(r"^([A-Z]{3})\s+(\d{1,2})\s+(\d{4})$", v)
            if m and m.group(1) in MONTH_IT:
                return f"{m.group(3)}-{MONTH_IT[m.group(1)]}-{m.group(2).zfill(2)}"
            return v

        original = df[date_col].copy()
        df[date_col] = df[date_col].apply(_parse_date)
        changed = int((df[date_col] != original).sum())
        if changed:
            changes.append(f"Normalised {changed} '{date_col}' values to YYYY-MM-DD")

    # 3. Resolve 'Da definire' in tipo_imposta using cod_tipoimposta
    expected_mapping = {"1": "Netto", "2": "Erariali", "3": "Previdenziali", "4": "Varie"}
    if "tipo_imposta" in df.columns and "cod_tipoimposta" in df.columns:
        mask = df["tipo_imposta"] == "Da definire"
        if mask.any():
            df.loc[mask, "tipo_imposta"] = df.loc[mask, "cod_tipoimposta"].map(
                lambda x: expected_mapping.get(str(x).strip(), "Da definire")
            )
            resolved = int(mask.sum() - (df["tipo_imposta"] == "Da definire").sum())
            changes.append(f"Resolved {resolved} 'Da definire' tipo_imposta entries")

    # 4. Drop exact duplicates
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    if removed:
        changes.append(f"Removed {removed} exact duplicate rows")

    df.to_csv(output_path, index=False)
    return {"fixes_applied": changes, "output_path": output_path}
