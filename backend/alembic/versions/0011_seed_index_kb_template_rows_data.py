"""seed index_kb_template_rows (SZI + UIB) without parsing Excel

Revision ID: 0011_seed_index_kb_rows
Revises: 0010_index_kb_template_rows
Create Date: 2026-01-28
"""

from __future__ import annotations

import json
from pathlib import Path

import sqlalchemy as sa
from alembic import op


revision = "0011_seed_index_kb_rows"
down_revision = "0010_index_kb_template_rows"
branch_labels = None
depends_on = None


_SHEETS = ("СЗИ", "Управление ИБ")


def _seed_path() -> Path:
    # backend/alembic/versions/<this_file>
    backend_dir = Path(__file__).resolve().parents[2]
    return backend_dir / "app" / "index_kb" / "template_seed_rows.json"


def _try_load_seed_rows_from_json(seed_path: Path) -> list[dict] | None:
    """
    Returns list of dict rows or None if seed is missing/empty/invalid.

    Why: in some setups this file may be empty (e.g. truncated or not generated),
    but we still want migrations to be able to run (fallback to Excel parsing).
    """
    if not seed_path.exists():
        return None

    raw = seed_path.read_text(encoding="utf-8", errors="replace").lstrip("\ufeff").strip()
    if not raw:
        return None

    # In some environments stdout-captured seed files may contain harmless prefixes
    # (e.g. tool banners). Be defensive and extract the JSON array payload.
    start = raw.find("[{")
    if start == -1:
        start = raw.find("[]")
    if start == -1:
        start = raw.find("[")
    end = raw.rfind("]")
    if start == -1 or end == -1 or end <= start:
        return None

    payload = raw[start : end + 1]
    try:
        data = json.loads(payload)
    except Exception:
        return None
    if not isinstance(data, list):
        return None
    return data  # type: ignore[return-value]


def upgrade() -> None:
    seed_path = _seed_path()
    data = _try_load_seed_rows_from_json(seed_path)
    if not data:
        raise RuntimeError(f"Index KB seed JSON is missing/empty/invalid: {seed_path}")

    # Make it idempotent: replace known sheets.
    op.execute(
        sa.text("DELETE FROM index_kb_template_rows WHERE sheet_name = ANY(:sheets)").bindparams(
            sa.bindparam("sheets", list(_SHEETS), type_=sa.ARRAY(sa.String()))
        )
    )

    tbl = sa.table(
        "index_kb_template_rows",
        sa.column("sheet_name", sa.String()),
        sa.column("sort_order", sa.Integer()),
        sa.column("kind", sa.String()),
        sa.column("row_key", sa.String()),
        sa.column("title", sa.Text()),
        sa.column("short_name", sa.String()),
        sa.column("group_code", sa.String()),
    )

    rows = []
    for r in data:
        if not isinstance(r, dict):
            continue
        if r.get("sheet_name") not in _SHEETS:
            continue
        rows.append(
            {
                "sheet_name": r.get("sheet_name") or "",
                "sort_order": int(r.get("sort_order") or 0),
                "kind": r.get("kind") or "item",
                "row_key": r.get("row_key") or "",
                "title": r.get("title") or "",
                "short_name": r.get("short_name") or "",
                "group_code": r.get("group_code") or "",
            }
        )

    if rows:
        op.bulk_insert(tbl, rows)


def downgrade() -> None:
    op.execute(
        sa.text("DELETE FROM index_kb_template_rows WHERE sheet_name = ANY(:sheets)").bindparams(
            sa.bindparam("sheets", list(_SHEETS), type_=sa.ARRAY(sa.String()))
        )
    )

