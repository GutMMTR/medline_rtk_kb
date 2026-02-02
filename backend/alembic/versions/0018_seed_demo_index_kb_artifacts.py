"""seed demo artifacts for Index KB templates (make radar non-empty)

Why:
- Index KB templates (index_kb_template_rows) reference short_name tokens like "СУИБ.УР.1".
- The main artifacts catalog (seeded from artifacts_seed.csv) uses different short_name tokens (e.g. "ВССТ.ПЛН.5").
- As a result, Index KB auto-scores treat most template rows as "нет артефакта" and radar looks empty.

This migration creates a dedicated subtree of artifacts ("Индекс КБ") with short_name matching
template rows for sheets "Управление ИБ" and "СЗИ", assigns sensible topic/domain and kb_level,
and seeds a mixed set of uploads/audits for demo org "Медлайн".

Revision ID: 0018_seed_kb_artifacts
Revises: 0017_seed_demo_radar_full
Create Date: 2026-02-02
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta

import sqlalchemy as sa
from alembic import op


revision = "0018_seed_kb_artifacts"
down_revision = "0017_seed_demo_radar_full"
branch_labels = None
depends_on = None


ORG_NAME = "Медлайн"
AUDITOR_LOGIN = "auditor"
CUSTOMER_LOGIN = "medline"


def _utc(dt: datetime) -> datetime:
    return dt


def _sha256_hex(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def _get_org_id(conn: sa.Connection) -> int | None:
    oid = conn.execute(sa.text("SELECT id FROM organizations WHERE name = :n"), {"n": ORG_NAME}).scalar()
    return int(oid) if oid is not None else None


def _get_user_id(conn: sa.Connection, *, login: str) -> int | None:
    uid = conn.execute(sa.text("SELECT id FROM users WHERE login = :l"), {"l": login}).scalar()
    return int(uid) if uid is not None else None


def _level_id(conn: sa.Connection, *, code: str) -> int | None:
    x = conn.execute(
        sa.text("SELECT id FROM artifact_levels WHERE code=:c ORDER BY id DESC LIMIT 1"),
        {"c": (code or "").strip().upper()},
    ).scalar()
    return int(x) if x is not None else None


def _get_node_id(conn: sa.Connection, *, full_path: str) -> int | None:
    x = conn.execute(sa.text("SELECT id FROM artifact_nodes WHERE full_path=:p"), {"p": full_path}).scalar()
    return int(x) if x is not None else None


def _ensure_node(conn: sa.Connection, *, parent_id: int | None, segment: str, full_path: str, sort_order: int) -> int:
    existing = _get_node_id(conn, full_path=full_path)
    if existing:
        return existing
    nid = conn.execute(
        sa.text(
            """
            INSERT INTO artifact_nodes (parent_id, segment, full_path, sort_order, created_at)
            VALUES (:pid, :seg, :path, :so, :at)
            RETURNING id
            """
        ),
        {
            "pid": int(parent_id) if parent_id else None,
            "seg": segment,
            "path": full_path,
            "so": int(sort_order),
            "at": _utc(datetime(2026, 2, 2, 11, 0, 0)),
        },
    ).scalar_one()
    return int(nid)


def _artifact_id_by_key(conn: sa.Connection, *, artifact_key: str) -> int | None:
    x = conn.execute(sa.text("SELECT id FROM artifacts WHERE artifact_key=:k"), {"k": artifact_key}).scalar()
    return int(x) if x is not None else None


def _insert_artifact(
    conn: sa.Connection,
    *,
    node_id: int,
    artifact_key: str,
    topic: str,
    domain: str,
    indicator_name: str,
    short_name: str,
    kb_level: str,
    title: str,
    description: str,
) -> int:
    existing = _artifact_id_by_key(conn, artifact_key=artifact_key)
    if existing:
        return existing
    aid = conn.execute(
        sa.text(
            """
            INSERT INTO artifacts
              (node_id, artifact_key, topic, domain, indicator_name, short_name, kb_level,
               achievement_text, achievement_item_no, achievement_item_text, title, description, created_at)
            VALUES
              (:nid, :akey, :topic, :domain, :ind, :sn, :kb,
               '', NULL, '', :title, :descr, :at)
            RETURNING id
            """
        ),
        {
            "nid": int(node_id),
            "akey": artifact_key,
            "topic": topic,
            "domain": domain,
            "ind": indicator_name,
            "sn": short_name,
            "kb": kb_level,
            "title": title,
            "descr": description,
            "at": _utc(datetime(2026, 2, 2, 11, 0, 0)),
        },
    ).scalar_one()
    return int(aid)


def _ensure_org_artifact(conn: sa.Connection, *, org_id: int, artifact_id: int) -> int:
    x = conn.execute(
        sa.text("SELECT id FROM org_artifacts WHERE org_id=:o AND artifact_id=:a"),
        {"o": int(org_id), "a": int(artifact_id)},
    ).scalar()
    if x is not None:
        return int(x)
    oa = conn.execute(
        sa.text(
            """
            INSERT INTO org_artifacts (org_id, artifact_id, status, review_status, created_at, updated_at)
            VALUES (:o,:a,'missing','pending',:at,:at)
            RETURNING id
            """
        ),
        {"o": int(org_id), "a": int(artifact_id), "at": _utc(datetime(2026, 2, 2, 11, 0, 0))},
    ).scalar_one()
    return int(oa)


def _next_version_no(conn: sa.Connection, *, org_artifact_id: int) -> int:
    v = conn.execute(
        sa.text("SELECT COALESCE(MAX(version_no),0) FROM file_versions WHERE org_artifact_id=:oa"),
        {"oa": int(org_artifact_id)},
    ).scalar()
    return int(v or 0) + 1


def _insert_fv(
    conn: sa.Connection,
    *,
    oa_id: int,
    filename: str,
    blob: bytes,
    created_at: datetime,
    created_by: int | None,
) -> int:
    ver = _next_version_no(conn, org_artifact_id=oa_id)
    fv_id = conn.execute(
        sa.text(
            """
            INSERT INTO file_versions
              (org_artifact_id, version_no, original_filename, content_type, size_bytes, sha256, storage_backend, storage_key, blob, created_at, created_by_user_id)
            VALUES
              (:oa, :ver, :fn, 'text/plain', :sz, :sha, 'postgres', NULL, :blob, :at, :by)
            RETURNING id
            """
        ),
        {
            "oa": int(oa_id),
            "ver": int(ver),
            "fn": filename,
            "sz": int(len(blob)),
            "sha": _sha256_hex(blob),
            "blob": blob,
            "at": _utc(created_at),
            "by": int(created_by) if created_by else None,
        },
    ).scalar_one()
    return int(fv_id)


def _set_oa_approved(
    conn: sa.Connection,
    *,
    oa_id: int,
    fv_id: int,
    upload_at: datetime,
    customer_id: int | None,
    auditor_id: int | None,
    org_id: int,
) -> None:
    # upload (pending)
    conn.execute(
        sa.text(
            """
            UPDATE org_artifacts
            SET status='uploaded',
                current_file_version_id=:cur,
                audited_file_version_id=NULL,
                audited_at=NULL,
                audited_by_user_id=NULL,
                review_status='pending',
                updated_at=:at,
                updated_by_user_id=:by
            WHERE id=:id
            """
        ),
        {"id": int(oa_id), "cur": int(fv_id), "at": _utc(upload_at), "by": int(customer_id) if customer_id else None},
    )
    conn.execute(
        sa.text(
            """
            INSERT INTO audit_log (at, actor_user_id, org_id, action, entity_type, entity_id, before_json, after_json, ip, user_agent)
            VALUES (:at, :actor, :org, 'upload', 'org_artifact', :eid,
                    CAST(NULL AS jsonb),
                    CAST(:after AS jsonb), '', '')
            """
        ),
        {
            "at": _utc(upload_at),
            "actor": int(customer_id) if customer_id else None,
            "org": int(org_id),
            "eid": str(int(oa_id)),
            "after": json.dumps(
                {
                    "status": "uploaded",
                    "current_file_version_id": int(fv_id),
                    "audited_file_version_id": None,
                    "audited_at": None,
                    "audited_by_user_id": None,
                    "review_status": "pending",
                },
                ensure_ascii=False,
            ),
        },
    )

    # audit (approved)
    aud_at = upload_at + datetime.resolution
    conn.execute(
        sa.text(
            """
            UPDATE org_artifacts
            SET status='uploaded',
                current_file_version_id=:cur,
                audited_file_version_id=:cur,
                audited_at=:aud_at,
                audited_by_user_id=:aud_by,
                review_status='approved',
                updated_at=:aud_at,
                updated_by_user_id=:aud_by
            WHERE id=:id
            """
        ),
        {
            "id": int(oa_id),
            "cur": int(fv_id),
            "aud_at": _utc(aud_at),
            "aud_by": int(auditor_id) if auditor_id else None,
        },
    )
    conn.execute(
        sa.text(
            """
            INSERT INTO audit_log (at, actor_user_id, org_id, action, entity_type, entity_id, before_json, after_json, ip, user_agent)
            VALUES (:at, :actor, :org, 'audit', 'org_artifact', :eid,
                    CAST(NULL AS jsonb),
                    CAST(:after AS jsonb), '', '')
            """
        ),
        {
            "at": _utc(aud_at),
            "actor": int(auditor_id) if auditor_id else None,
            "org": int(org_id),
            "eid": str(int(oa_id)),
            "after": json.dumps(
                {
                    "audited_file_version_id": int(fv_id),
                    "audited_at": aud_at.isoformat(),
                    "audited_by_user_id": int(auditor_id) if auditor_id else None,
                    "current_file_version_id": int(fv_id),
                    "review_status": "approved",
                },
                ensure_ascii=False,
            ),
        },
    )


def upgrade() -> None:
    conn = op.get_bind()

    org_id = _get_org_id(conn)
    if not org_id:
        return

    if (os.getenv("SEED_DEMO_DATA") or "").strip() not in ("1", "true", "True", "yes", "on"):
        return

    customer_id = _get_user_id(conn, login=CUSTOMER_LOGIN)
    auditor_id = _get_user_id(conn, login=AUDITOR_LOGIN)

    l1 = _level_id(conn, code="L1")
    l2 = _level_id(conn, code="L2")
    l3 = _level_id(conn, code="L3")

    root_id = _ensure_node(conn, parent_id=None, segment="Индекс КБ", full_path="Индекс КБ", sort_order=10_000)

    # Load template items and create artifacts for them if missing.
    items = conn.execute(
        sa.text(
            """
            SELECT t.sheet_name, t.group_code, t.row_key, t.title, t.short_name, t.sort_order
            FROM index_kb_template_rows t
            WHERE t.kind='item' AND t.short_name <> '' AND t.sheet_name IN ('Управление ИБ','СЗИ')
            ORDER BY t.sheet_name, t.sort_order, t.id
            """
        )
    ).all()
    if not items:
        return

    # Create sheet/group nodes
    sheet_node: dict[str, int] = {}
    group_node: dict[tuple[str, str], int] = {}

    def ensure_sheet(sheet: str) -> int:
        if sheet in sheet_node:
            return sheet_node[sheet]
        sid = _ensure_node(conn, parent_id=root_id, segment=sheet, full_path=f"Индекс КБ/{sheet}", sort_order=10_001 + len(sheet_node))
        sheet_node[sheet] = sid
        return sid

    def ensure_group(sheet: str, group: str) -> int:
        key = (sheet, group)
        if key in group_node:
            return group_node[key]
        pid = ensure_sheet(sheet)
        gid = _ensure_node(conn, parent_id=pid, segment=group, full_path=f"Индекс КБ/{sheet}/{group}", sort_order=20_000 + len(group_node))
        group_node[key] = gid
        return gid

    created_artifact_ids: list[int] = []
    # Deterministic kb_level mix to get non-trivial KB1/KB2/KB3 lines.
    kb_cycle = ["КБ1", "КБ2", "КБ3"]

    for idx, (sheet, group, row_key, title, short_name, sort_order) in enumerate(items):
        sheet = str(sheet or "")
        group = str(group or "").strip() or "—"
        row_key = str(row_key or "")
        title = str(title or "")
        short_name = str(short_name or "").strip()
        if not short_name:
            continue

        gnode = ensure_group(sheet, group)
        n_full = f"Индекс КБ/{sheet}/{group}/{short_name}"
        n_id = _ensure_node(conn, parent_id=gnode, segment=short_name, full_path=n_full, sort_order=int(sort_order or 0))

        # Use unique artifact_key to avoid collisions and support idempotency.
        akey = f"IKB:{sheet}:{row_key}"
        kb_level = kb_cycle[idx % len(kb_cycle)]
        topic = sheet  # shows up in topic filter and matches intent
        domain = group

        aid = _insert_artifact(
            conn,
            node_id=n_id,
            artifact_key=akey,
            topic=topic,
            domain=domain,
            indicator_name=title,
            short_name=short_name,
            kb_level=kb_level,
            title=title or short_name,
            description="DEMO: Index KB template artifact",
        )
        created_artifact_ids.append(int(aid))

        # Ensure artifact_level_items for new artifacts (0014 only seeded from existing artifacts).
        if kb_level == "КБ1" and l1:
            conn.execute(
                sa.text("INSERT INTO artifact_level_items (level_id, artifact_id) VALUES (:l,:a) ON CONFLICT DO NOTHING"),
                {"l": int(l1), "a": int(aid)},
            )
        elif kb_level == "КБ2" and l2:
            conn.execute(
                sa.text("INSERT INTO artifact_level_items (level_id, artifact_id) VALUES (:l,:a) ON CONFLICT DO NOTHING"),
                {"l": int(l2), "a": int(aid)},
            )
        elif l3:
            conn.execute(
                sa.text("INSERT INTO artifact_level_items (level_id, artifact_id) VALUES (:l,:a) ON CONFLICT DO NOTHING"),
                {"l": int(l3), "a": int(aid)},
            )

        _ensure_org_artifact(conn, org_id=int(org_id), artifact_id=int(aid))

    # Seed uploads/audits for a subset to make radar "full enough" but not all-5.
    # Score rule: 5 if audited==current and approved, else 0/None.
    # We'll approve ~65% deterministically.
    base = datetime(2026, 2, 10, 10, 0, 0)
    for j, aid in enumerate(created_artifact_ids):
        # Find org_artifact
        oa_id = conn.execute(
            sa.text("SELECT id FROM org_artifacts WHERE org_id=:o AND artifact_id=:a"),
            {"o": int(org_id), "a": int(aid)},
        ).scalar()
        if oa_id is None:
            continue

        if (j % 20) in (0, 1, 2):
            # leave some as missing to keep variety
            continue
        if (j % 20) in (3, 4, 5, 6):
            # upload but do not audit => pending
            sn = conn.execute(sa.text("SELECT short_name FROM artifacts WHERE id=:a"), {"a": int(aid)}).scalar() or ""
            at = base + timedelta(minutes=j)
            fv = _insert_fv(conn, oa_id=int(oa_id), filename=f"{sn}_pending.txt", blob=(f"DEMO pending\nsn={sn}\n").encode("utf-8"), created_at=at, created_by=customer_id)
            conn.execute(
                sa.text(
                    """
                    UPDATE org_artifacts
                    SET status='uploaded',
                        current_file_version_id=:cur,
                        audited_file_version_id=NULL,
                        audited_at=NULL,
                        audited_by_user_id=NULL,
                        review_status='pending',
                        updated_at=:at,
                        updated_by_user_id=:by
                    WHERE id=:id
                    """
                ),
                {"id": int(oa_id), "cur": int(fv), "at": _utc(at), "by": int(customer_id) if customer_id else None},
            )
            continue

        # approved
        sn = conn.execute(sa.text("SELECT short_name FROM artifacts WHERE id=:a"), {"a": int(aid)}).scalar() or ""
        at = base + timedelta(minutes=j)
        fv = _insert_fv(conn, oa_id=int(oa_id), filename=f"{sn}_approved.txt", blob=(f"DEMO approved\nsn={sn}\n").encode("utf-8"), created_at=at, created_by=customer_id)
        _set_oa_approved(conn, oa_id=int(oa_id), fv_id=int(fv), upload_at=at, customer_id=customer_id, auditor_id=auditor_id, org_id=int(org_id))


def downgrade() -> None:
    pass

