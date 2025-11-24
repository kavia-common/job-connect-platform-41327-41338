"""
Simple CLI entrypoint to trigger JSON ingestion into SQLite.

Usage:
    python -m src.api.ingest_cli
"""
from __future__ import annotations

import json

from .ingestion.json_loader import run_json_ingestion


def main() -> None:
    """Run ingestion and print summary as JSON."""
    result = run_json_ingestion()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
