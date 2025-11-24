# Data Preview: JSON → SQLite Ingestion

This backend can ingest deduplicated JSON files from `kavia-docs/data/` and expose preview endpoints.

How it works
- Each JSON file is treated as a dataset. Top-level keys are sheets (arrays of row objects).
- A SQLite table is created per sheet named `<dataset>__<sheet>` (snake_case).
- Columns are inferred from the union of keys; types are INTEGER, REAL, DATETIME (ISO), or TEXT.
- Ingestion uses truncate-and-load to keep previews consistent.

Run ingestion
- CLI: `python -m src.api.ingest_cli`
- API (admin): `POST /admin/ingest-json`
  - If env `ADMIN_SHARED_SECRET` is set, include header `X-Admin-Secret: <value>`.
  - If not set, only local requests are allowed.

Preview endpoints
- GET `/datasets` → list datasets and tables
- GET `/datasets/{dataset}/sheets` → list sheets (tables) with counts
- GET `/datasets/{dataset}/sheets/{sheet}/rows?limit=50&offset=0&search=&order_by=&order_dir=asc`
- GET `/schemas/{table}` → table schema

Notes
- SQLite file path uses `SQLITE_DB` env var if present; else defaults to `career_platform_backend/data_preview.sqlite`.
- Data folder defaults to `<repo>/kavia-docs/data` (can override via `DATA_PREVIEW_ROOT`).
