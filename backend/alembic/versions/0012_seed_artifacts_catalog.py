"""seed artifacts catalog (default reference data)

Revision ID: 0012_seed_artifacts
Revises: 0011_seed_index_kb_rows
Create Date: 2026-01-29
"""

from __future__ import annotations

import csv
import io
from datetime import datetime
from pathlib import Path

import sqlalchemy as sa
from alembic import op


revision = "0012_seed_artifacts"
down_revision = "0011_seed_index_kb_rows"
branch_labels = None
depends_on = None


def _seed_csv_path() -> Path:
    # backend/alembic/versions/<this_file>
    backend_dir = Path(__file__).resolve().parents[2]
    return backend_dir / "app" / "seeds" / "artifacts_seed.csv"


def _ensure_node_path(
    conn: sa.Connection,
    *,
    node_cache: dict[str, int],
    segments: list[str],
    created_at: datetime,
) -> int:
    parent_id: int | None = None
    full_parts: list[str] = []
    leaf_id: int | None = None

    for seg in segments:
        seg = (seg or "").strip()
        if not seg:
            continue
        full_parts.append(seg)
        full_path = ".".join(full_parts)

        cached = node_cache.get(full_path)
        if cached:
            leaf_id = int(cached)
            parent_id = int(cached)
            continue

        existing = conn.execute(
            sa.text("SELECT id FROM artifact_nodes WHERE full_path = :p LIMIT 1"),
            {"p": full_path},
        ).scalar()
        if existing is not None:
            leaf_id = int(existing)
            node_cache[full_path] = leaf_id
            parent_id = leaf_id
            continue

        leaf_id = int(
            conn.execute(
                sa.text(
                    "INSERT INTO artifact_nodes (parent_id, segment, full_path, sort_order, created_at) "
                    "VALUES (:parent_id, :segment, :full_path, 0, :created_at) "
                    "RETURNING id"
                ),
                {"parent_id": parent_id, "segment": seg, "full_path": full_path, "created_at": created_at},
            ).scalar_one()
        )
        node_cache[full_path] = leaf_id
        parent_id = leaf_id

    if leaf_id is None:
        raise RuntimeError("Пустой путь узлов (segments)")
    return leaf_id


def upgrade() -> None:
    seed_path = _seed_csv_path()
    if not seed_path.exists():
        raise RuntimeError(f"Artifacts seed CSV not found: {seed_path}")
    raw = seed_path.read_text(encoding="utf-8", errors="strict")
    if not raw.strip():
        raise RuntimeError(f"Artifacts seed CSV is empty: {seed_path}")

    conn = op.get_bind()
    created_at = datetime.utcnow()

    node_cache: dict[str, int] = {}
    # CSV may include embedded newlines in quoted fields, so parse from full text.
    # Strip UTF-8 BOM if present.
    if raw.startswith("\ufeff"):
        raw = raw.lstrip("\ufeff")

    reader = csv.DictReader(io.StringIO(raw, newline=""))
    for r in reader:
        key = (r.get("artifact_key") or "").strip()
        node_full_path = (r.get("node_full_path") or "").strip()
        if not key or not node_full_path:
            continue

        segments = [p.strip() for p in node_full_path.split(".") if p.strip()]
        node_id = _ensure_node_path(conn, node_cache=node_cache, segments=segments, created_at=created_at)

        item_no_raw = (r.get("achievement_item_no") or "").strip()
        item_no = int(item_no_raw) if item_no_raw else None

        topic = (r.get("topic") or "").strip()
        domain = (r.get("domain") or "").strip()
        indicator_name = (r.get("indicator_name") or "").strip()
        short_name = (r.get("short_name") or "").strip()
        kb_level = (r.get("kb_level") or "").strip()
        achievement_text = (r.get("achievement_text") or "").strip()
        achievement_item_text = (r.get("achievement_item_text") or "").strip()
        title = (r.get("title") or achievement_item_text).strip()
        description = (r.get("description") or "").strip()

        payload = {
            "node_id": int(node_id),
            "artifact_key": key,
            "topic": topic,
            "domain": domain,
            "indicator_name": indicator_name,
            "short_name": short_name,
            "kb_level": kb_level,
            "achievement_text": achievement_text or achievement_item_text,
            "achievement_item_no": item_no,
            "achievement_item_text": achievement_item_text,
            "title": title,
            "description": description,
            "created_at": created_at,
        }

        conn.execute(
            sa.text(
                "INSERT INTO artifacts ("
                "  node_id, artifact_key, topic, domain, indicator_name, short_name, kb_level,"
                "  achievement_text, achievement_item_no, achievement_item_text, title, description, created_at"
                ") VALUES ("
                "  :node_id, :artifact_key, :topic, :domain, :indicator_name, :short_name, :kb_level,"
                "  :achievement_text, :achievement_item_no, :achievement_item_text, :title, :description, :created_at"
                ") ON CONFLICT (artifact_key) DO UPDATE SET "
                "  node_id = EXCLUDED.node_id,"
                "  topic = EXCLUDED.topic,"
                "  domain = EXCLUDED.domain,"
                "  indicator_name = EXCLUDED.indicator_name,"
                "  short_name = EXCLUDED.short_name,"
                "  kb_level = EXCLUDED.kb_level,"
                "  achievement_text = EXCLUDED.achievement_text,"
                "  achievement_item_no = EXCLUDED.achievement_item_no,"
                "  achievement_item_text = EXCLUDED.achievement_item_text,"
                "  title = EXCLUDED.title,"
                "  description = EXCLUDED.description"
            ),
            payload,
        )


def downgrade() -> None:
    # Do not delete user data on downgrade; keep it safe.
    # (If you really need to purge seeded artifacts, do it with a dedicated maintenance migration.)
    pass

