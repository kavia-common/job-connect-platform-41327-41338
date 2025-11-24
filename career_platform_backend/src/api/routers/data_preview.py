"""
FastAPI routes for dataset preview and ingestion trigger.

Security:
- Admin ingestion protected by shared secret header X-Admin-Secret if present in env ADMIN_SHARED_SECRET.
- If not set, allows only from local origins (127.0.0.1/localhost) and logs a warning message.

Endpoints:
- POST /admin/ingest-json: trigger ingestion run.
- GET /datasets: list datasets and their tables.
- GET /datasets/{dataset}/sheets: list sheet tables for dataset with counts.
- GET /datasets/{dataset}/sheets/{sheet}/rows: paginated rows with search and ordering.
- GET /schemas/{table}: return inferred schema.

All SQL uses sanitized identifiers and parameterized queries for values.
"""
from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Header, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from ..database import get_db_connection
from ..ingestion.json_loader import run_json_ingestion, _normalize_identifier

router = APIRouter()

ADMIN_SECRET = os.environ.get("ADMIN_SHARED_SECRET")


class IngestionResult(BaseModel):
    root: str = Field(..., description="Root path scanned")
    files: List[Dict[str, Any]] = Field(..., description="Per-file ingestion summary")
    message: Optional[str] = Field(None, description="Message for empty or status cases")


def _client_is_local(request: Request) -> bool:
    client_host = (request.client.host if request.client else "") or ""
    return client_host.startswith("127.0.0.1") or client_host == "localhost" or client_host == "::1"


def _sanitize_table(table: str) -> str:
    # Ensure only allowed characters in identifiers
    s = _normalize_identifier(table, max_len=60)
    if not re.fullmatch(r"[a-z0-9_]+", s):
        raise HTTPException(status_code=400, detail="Invalid identifier")
    return s


def _dataset_tables(dataset: str) -> List[Tuple[str, str]]:
    """Return list of (table, sheet) for dataset prefix."""
    ds = _sanitize_table(dataset)
    prefix = f"{ds}__"
    results: List[Tuple[str, str]] = []
    with get_db_connection() as conn:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ? ORDER BY name ASC", (f"{prefix}%",)
        )
        rows = cur.fetchall()
        for r in rows:
            tname = r["name"]
            sheet = tname[len(prefix) :]
            results.append((tname, sheet))
    return results


# PUBLIC_INTERFACE
@router.post(
    "/admin/ingest-json",
    response_model=IngestionResult,
    summary="Trigger JSON ingestion",
    description="Scans kavia-docs/data for deduplicated JSON and loads into SQLite preview tables.",
    tags=["admin"],
)
async def ingest_json(
    request: Request, x_admin_secret: Optional[str] = Header(default=None, alias="X-Admin-Secret")
):
    """Trigger ingestion with a simple shared-secret check or local-only fallback.

    PUBLIC_INTERFACE
    Parameters:
    - X-Admin-Secret: optional shared secret header; if ADMIN_SHARED_SECRET env is set, it must match.

    Returns:
    - IngestionResult summary of the run.
    """
    if ADMIN_SECRET:
        if not x_admin_secret or x_admin_secret != ADMIN_SECRET:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    else:
        # allow local only
        if not _client_is_local(request):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized (local-only)")
    result = run_json_ingestion()
    return IngestionResult(**result)


class DatasetListItem(BaseModel):
    dataset: str = Field(..., description="Dataset base name")
    tables: List[str] = Field(..., description="Table names associated with dataset")


# PUBLIC_INTERFACE
@router.get(
    "/datasets",
    response_model=List[DatasetListItem],
    summary="List datasets",
    description="Lists available dataset base names and their sheet tables.",
    tags=["preview"],
)
async def list_datasets():
    """List datasets available in the database.

    PUBLIC_INTERFACE
    Returns:
        List of dataset names with tables.
    """
    items: Dict[str, List[str]] = {}
    with get_db_connection() as conn:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name ASC")
        for row in cur.fetchall():
            name = row["name"]
            parts = name.split("__", 1)
            if len(parts) != 2:
                # skip non-preview tables
                continue
            ds = parts[0]
            items.setdefault(ds, []).append(name)
    return [DatasetListItem(dataset=k, tables=v) for k, v in items.items()]


class SheetInfo(BaseModel):
    sheet: str = Field(..., description="Sheet name")
    table: str = Field(..., description="Fully qualified table name")
    count: int = Field(..., description="Row count")


# PUBLIC_INTERFACE
@router.get(
    "/datasets/{dataset}/sheets",
    response_model=List[SheetInfo],
    summary="List sheets for a dataset",
    description="Lists sheets (tables) for a dataset with row counts.",
    tags=["preview"],
)
async def list_sheets(dataset: str):
    """List sheets within a dataset with row counts.

    PUBLIC_INTERFACE
    Args:
        dataset: dataset base name.

    Returns:
        List of sheets with counts.
    """
    tbls = _dataset_tables(dataset)
    infos: List[SheetInfo] = []
    with get_db_connection() as conn:
        for tname, sheet in tbls:
            cur = conn.execute(f'SELECT COUNT(1) AS c FROM "{tname}"')
            cnt = int(cur.fetchone()["c"])
            infos.append(SheetInfo(sheet=sheet, table=tname, count=cnt))
    return infos


class RowPage(BaseModel):
    total: int = Field(..., description="Total rows (after filter)")
    limit: int = Field(..., ge=1, le=200, description="Page size")
    offset: int = Field(..., ge=0, description="Offset")
    rows: List[Dict[str, Any]] = Field(..., description="Rows")


_ALLOWED_ORDER_DIR = {"asc", "desc"}


# PUBLIC_INTERFACE
@router.get(
    "/datasets/{dataset}/sheets/{sheet}/rows",
    response_model=RowPage,
    summary="Get rows for a sheet",
    description="Returns a page of rows with optional search and ordering.",
    tags=["preview"],
)
async def get_rows(
    dataset: str,
    sheet: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    search: Optional[str] = Query(None, max_length=200),
    order_by: Optional[str] = Query(None, description="Column to order by"),
    order_dir: str = Query("asc", pattern="^(?i)(asc|desc)$"),
):
    """Paginated rows from a dataset sheet with optional search and ordering.

    PUBLIC_INTERFACE
    Parameters:
    - dataset, sheet: path params identifying the table.
    - limit (1..200), offset (>=0).
    - search: optional string; applies LIKE across TEXT columns.
    - order_by: optional column name; sanitized.
    - order_dir: asc or desc (case-insensitive).

    Returns:
    - RowPage with total, limit, offset, rows list.
    """
    tname = _sanitize_table(f"{dataset}__{sheet}")
    with get_db_connection() as conn:
        # Determine schema to build safe query
        cur = conn.execute(f'PRAGMA table_info("{tname}")')
        cols = [r["name"] for r in cur.fetchall()]
        if not cols:
            raise HTTPException(status_code=404, detail="Table not found")
        # Validate order_by
        order_sql = ""
        if order_by:
            ob = _sanitize_table(order_by)
            if ob not in cols:
                raise HTTPException(status_code=400, detail="Invalid order_by")
            dir_norm = "ASC" if order_dir.lower() == "asc" else "DESC"
            order_sql = f' ORDER BY "{ob}" {dir_norm}'

        where_sql = ""
        params: List[Any] = []
        if search:
            # find TEXT columns
            cur = conn.execute(f'PRAGMA table_info("{tname}")')
            info = cur.fetchall()
            text_cols = [r["name"] for r in info if "TEXT" in (r["type"] or "").upper()]
            if text_cols:
                where_clauses = [f'"{c}" LIKE ?' for c in text_cols]
                where_sql = " WHERE " + " OR ".join(where_clauses)
                like = f"%{search}%"
                params.extend([like] * len(text_cols))

        # total count
        count_sql = f'SELECT COUNT(1) AS c FROM "{tname}"{where_sql}'
        cur = conn.execute(count_sql, params)
        total = int(cur.fetchone()["c"])

        # page
        select_cols = ", ".join(f'"{c}"' for c in cols)
        page_sql = f'SELECT {select_cols} FROM "{tname}"{where_sql}{order_sql} LIMIT ? OFFSET ?'
        page_params = list(params) + [limit, offset]
        cur = conn.execute(page_sql, page_params)
        rows = [dict(r) for r in cur.fetchall()]

    return RowPage(total=total, limit=limit, offset=offset, rows=rows)


class TableSchema(BaseModel):
    table: str = Field(..., description="Table name")
    columns: List[Dict[str, str]] = Field(..., description="List of columns with types")


# PUBLIC_INTERFACE
@router.get(
    "/schemas/{table}",
    response_model=TableSchema,
    summary="Get table schema",
    description="Returns inferred schema (columns and types) for a table.",
    tags=["preview"],
)
async def get_schema(table: str):
    """Return table schema.

    PUBLIC_INTERFACE
    Args:
        table: table name (sanitized).

    Returns:
        TableSchema with columns and types.
    """
    tname = _sanitize_table(table)
    with get_db_connection() as conn:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (tname,)
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Table not found")
        cur = conn.execute(f'PRAGMA table_info("{tname}")')
        cols = [{"name": r["name"], "type": r["type"]} for r in cur.fetchall()]
    return TableSchema(table=tname, columns=cols)
