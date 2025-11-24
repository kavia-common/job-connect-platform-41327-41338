"""
Database utilities for SQLite connection management.

PUBLIC_INTERFACE
get_db_connection(): Provides a context-managed SQLite connection using the SQLITE_DB env var if present, else defaults to a local file.
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import Iterator, Optional


DEFAULT_DB_PATH = os.environ.get("SQLITE_DB") or os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data_preview.sqlite"
)


@contextmanager
# PUBLIC_INTERFACE
def get_db_connection(db_path: Optional[str] = None) -> Iterator[sqlite3.Connection]:
    """Get a SQLite connection with safe defaults.

    PUBLIC_INTERFACE
    Args:
        db_path: Optional override for DB path; if not provided, uses env SQLITE_DB or default file.

    Yields:
        sqlite3.Connection: A connection with row factory for dict-like access.

    Notes:
        - Uses check_same_thread=False to support FastAPI async workers safely per-request.
        - Enables foreign keys pragma.
    """
    path = db_path or DEFAULT_DB_PATH
    conn = sqlite3.connect(path, check_same_thread=False, timeout=30.0, isolation_level=None)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        yield conn
    finally:
        conn.close()
