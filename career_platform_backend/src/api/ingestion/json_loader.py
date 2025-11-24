"""
JSON ingestion utilities to load deduplicated datasets into SQLite tables.

Follows PySecure-4-Minimal-Standard:
- Validates inputs and controls resource usage.
- Error handling with minimal, non-sensitive logs.
- Clean, modular code with type hints and docstrings.

Usage:
- call run_json_ingestion() to scan kavia-docs/data and load all JSON files.

Table naming:
- <basefilename>__<sheetname> normalized to snake_case, alnum + underscore only, max 60 chars.

Schema inference:
- Union of keys across rows per sheet.
- Types: INTEGER (int), REAL (float), DATETIME (ISO 8601), TEXT otherwise.
- Nulls preserved. Truncate-and-load per run.

Security:
- No dynamic SQL for identifiers; all identifiers are sanitized.
- Data values inserted via parameterized queries.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .database import get_db_connection


DATA_ROOT_DEFAULT = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "kavia-docs", "data")


def _is_iso_datetime(value: str) -> bool:
    try:
        # Accept common ISO formats
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except Exception:
        return False


def _normalize_identifier(name: str, max_len: int = 60) -> str:
    # snake_case and strip invalid characters
    s = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()
    if not s:
        s = "unnamed"
    return s[:max_len]


def _infer_type(current: str, value: Any) -> str:
    # Returns SQLite type name
    if value is None:
        return current or "TEXT"
    if isinstance(value, bool):
        # Store booleans as INTEGER 0/1
        return "INTEGER" if current in ("", "INTEGER") else current
    if isinstance(value, int):
        return "INTEGER" if current in ("", "INTEGER") else current
    if isinstance(value, float):
        # If already INTEGER, keep as REAL to accommodate floats
        return "REAL" if current in ("", "INTEGER", "REAL") else current
    if isinstance(value, str):
        if _is_iso_datetime(value):
            return "DATETIME" if current in ("", "TEXT", "DATETIME") else current
        # numeric-looking?
        try:
            if "." in value:
                float(value)
                return "REAL" if current in ("", "INTEGER", "REAL", "TEXT") else current
            else:
                int(value)
                return "INTEGER" if current in ("", "INTEGER", "TEXT") else current
        except Exception:
            return "TEXT"
    # Fallback serialize to TEXT for lists/dicts/etc.
    return "TEXT"


def _union_schema(rows: Iterable[Mapping[str, Any]]) -> Dict[str, str]:
    schema: Dict[str, str] = {}
    for r in rows:
        if not isinstance(r, Mapping):
            # skip invalid rows
            continue
        for k, v in r.items():
            col = _normalize_identifier(str(k))
            prev = schema.get(col, "")
            schema[col] = _infer_type(prev, v)
    # Ensure at least one column
    if not schema:
        schema["_row"] = "TEXT"
    return schema


def _coerce_value(sql_type: str, value: Any) -> Any:
    if value is None:
        return None
    try:
        if sql_type == "INTEGER":
            if isinstance(value, bool):
                return 1 if value else 0
            if isinstance(value, (int,)):
                return value
            return int(value)
        if sql_type == "REAL":
            if isinstance(value, (int, float)):
                return float(value)
            return float(str(value))
        if sql_type == "DATETIME":
            if isinstance(value, str):
                # store standardized ISO format
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return dt.isoformat()
            return str(value)
        # TEXT
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)
    except Exception:
        # On coercion error, store as TEXT representation
        return str(value)


def _latest_json_files(data_root: str) -> List[str]:
    if not os.path.isdir(data_root):
        return []
    files = []
    for fn in os.listdir(data_root):
        if not fn.lower().endswith(".json"):
            continue
        full = os.path.join(data_root, fn)
        if os.path.isfile(full):
            files.append(full)
    # For "latest", we can sort by modified time descending
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files


def _safe_execute(conn: sqlite3.Connection, sql: str) -> None:
    conn.execute(sql)


def _ensure_table(conn: sqlite3.Connection, table: str, schema: Dict[str, str]) -> None:
    cols_sql = ", ".join(f'"{c}" {t}' for c, t in schema.items())
    _safe_execute(conn, f'CREATE TABLE IF NOT EXISTS "{table}" ({cols_sql})')
    # Ensure columns exist (add missing)
    cur = conn.execute(f'PRAGMA table_info("{table}")')
    existing_cols = {row["name"] for row in cur.fetchall()}
    for c, t in schema.items():
        if c not in existing_cols:
            _safe_execute(conn, f'ALTER TABLE "{table}" ADD COLUMN "{c}" {t}')


def _truncate_table(conn: sqlite3.Connection, table: str) -> None:
    _safe_execute(conn, f'DELETE FROM "{table}"')


def _insert_rows(conn: sqlite3.Connection, table: str, schema: Dict[str, str], rows: Iterable[Mapping[str, Any]]) -> int:
    cols = list(schema.keys())
    placeholders = ", ".join(["?"] * len(cols))
    # Build column list safely without nested f-strings that break escaping
    column_list = ", ".join([f'"{c}"' for c in cols])
    sql = f'INSERT INTO "{table}" ({column_list}) VALUES ({placeholders})'
    count = 0
    # Use executemany in manageable batches
    batch: List[Tuple[Any, ...]] = []
    batch_size = 500
    for r in rows:
        if not isinstance(r, Mapping):
            continue
        vals = []
        for c in cols:
            # Find original key closest to c by normalization
            orig_val = None
            for k, v in r.items():
                if _normalize_identifier(str(k)) == c:
                    orig_val = v
                    break
            vals.append(_coerce_value(schema[c], orig_val))
        batch.append(tuple(vals))
        if len(batch) >= batch_size:
            conn.executemany(sql, batch)
            count += len(batch)
            batch = []
    if batch:
        conn.executemany(sql, batch)
        count += len(batch)
    return count


# PUBLIC_INTERFACE
def run_json_ingestion(data_root: Optional[str] = None) -> Dict[str, Any]:
    """Ingest deduplicated JSON files into SQLite preview tables.

    PUBLIC_INTERFACE
    Args:
        data_root: Optional path to data root; defaults to kavia-docs/data within repository.

    Returns:
        Summary dict with per-file and per-sheet counts.
    """
    root = data_root or os.environ.get("DATA_PREVIEW_ROOT") or DATA_ROOT_DEFAULT
    summary: Dict[str, Any] = {"root": root, "files": []}
    files = _latest_json_files(root)
    if not files:
        return {**summary, "message": "No JSON files found"}

    with get_db_connection() as conn:
        for fp in files:
            base = os.path.splitext(os.path.basename(fp))[0]
            dataset_name = _normalize_identifier(base)
            file_entry = {"file": fp, "dataset": dataset_name, "sheets": []}
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    # If file is an array, treat as single sheet "data"
                    data = {"data": data}
                for raw_sheet, rows in data.items():
                    sheet_name = _normalize_identifier(str(raw_sheet))
                    table = _normalize_identifier(f"{dataset_name}__{sheet_name}")
                    if not isinstance(rows, list):
                        # Normalize to list of rows
                        rows = [rows]
                    # Build schema
                    schema = _union_schema(rows)
                    _ensure_table(conn, table, schema)
                    _truncate_table(conn, table)
                    inserted = _insert_rows(conn, table, schema, rows)
                    file_entry["sheets"].append(
                        {"sheet": sheet_name, "table": table, "inserted": inserted, "columns": schema}
                    )
            except Exception as exc:
                # Minimal error info without sensitive details
                file_entry["error"] = str(exc.__class__.__name__)
            summary["files"].append(file_entry)
    return summary
