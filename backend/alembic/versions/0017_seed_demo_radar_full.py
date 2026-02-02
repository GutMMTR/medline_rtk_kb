"""seed demo (extend Index KB radar coverage)

Revision ID: 0017_seed_demo_radar_full
Revises: 0016_seed_demo_all
Create Date: 2026-02-02
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta

import sqlalchemy as sa
from alembic import op


revision = "0017_seed_demo_radar_full"
down_revision = "0016_seed_demo_all"
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
    content_type: str,
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
              (:oa, :ver, :fn, :ct, :sz, :sha, 'postgres', NULL, :blob, :at, :by)
            RETURNING id
            """
        ),
        {
            "oa": int(oa_id),
            "ver": int(ver),
            "fn": filename,
            "ct": content_type,
            "sz": int(len(blob)),
            "sha": _sha256_hex(blob),
            "blob": blob,
            "at": _utc(created_at),
            "by": int(created_by) if created_by else None,
        },
    ).scalar_one()
    return int(fv_id)


def _set_oa(
    conn: sa.Connection,
    *,
    oa_id: int,
    cur_fv: int,
    review_status: str,
    updated_at: datetime,
    updated_by: int | None,
    audited_fv: int | None = None,
    audited_at: datetime | None = None,
    audited_by: int | None = None,
) -> None:
    conn.execute(
        sa.text(
            """
            UPDATE org_artifacts
            SET status='uploaded',
                current_file_version_id=:cur,
                review_status=:rs,
                audited_file_version_id=:aud_fv,
                audited_at=:aud_at,
                audited_by_user_id=:aud_by,
                updated_at=:upd_at,
                updated_by_user_id=:upd_by
            WHERE id=:id
            """
        ),
        {
            "id": int(oa_id),
            "cur": int(cur_fv),
            "rs": review_status,
            "aud_fv": int(audited_fv) if audited_fv else None,
            "aud_at": _utc(audited_at) if audited_at else None,
            "aud_by": int(audited_by) if audited_by else None,
            "upd_at": _utc(updated_at),
            "upd_by": int(updated_by) if updated_by else None,
        },
    )


def _audit_log(
    conn: sa.Connection,
    *,
    at: datetime,
    actor_user_id: int | None,
    org_id: int | None,
    action: str,
    entity_type: str,
    entity_id: str,
    before: dict | None = None,
    after: dict | None = None,
) -> None:
    conn.execute(
        sa.text(
            """
            INSERT INTO audit_log (at, actor_user_id, org_id, action, entity_type, entity_id, before_json, after_json, ip, user_agent)
            VALUES (:at, :actor, :org, :action, :etype, :eid,
                    CAST(:before AS jsonb), CAST(:after AS jsonb), '', '')
            """
        ),
        {
            "at": _utc(at),
            "actor": int(actor_user_id) if actor_user_id else None,
            "org": int(org_id) if org_id else None,
            "action": action,
            "etype": entity_type,
            "eid": str(entity_id),
            "before": (json.dumps(before, ensure_ascii=False) if before is not None else None),
            "after": (json.dumps(after, ensure_ascii=False) if after is not None else None),
        },
    )


def _ensure_approved_for_short_name(
    conn: sa.Connection,
    *,
    org_id: int,
    short_name: str,
    upload_at: datetime,
    customer_id: int | None,
    auditor_id: int | None,
) -> int:
    """
    Делает (upload + audit) для ВСЕХ org_artifacts организации с данным short_name.
    Возвращает сколько org_artifacts обновили.
    """
    rows = conn.execute(
        sa.text(
            """
            SELECT oa.id, oa.current_file_version_id, oa.audited_file_version_id, oa.review_status
            FROM org_artifacts oa
            JOIN artifacts a ON a.id = oa.artifact_id
            WHERE oa.org_id = :o AND a.short_name = :sn
            ORDER BY oa.id
            """
        ),
        {"o": int(org_id), "sn": short_name},
    ).all()
    if not rows:
        return 0

    changed = 0
    for oa_id, cur_fv, aud_fv, rs in rows:
        if cur_fv and aud_fv and int(cur_fv) == int(aud_fv) and str(rs or "") == "approved":
            continue

        blob = (f"DEMO index_kb\nshort_name={short_name}\n").encode("utf-8")
        fv_id = _insert_fv(
            conn,
            oa_id=int(oa_id),
            filename=f"{short_name}_demo.txt",
            content_type="text/plain",
            blob=blob,
            created_at=upload_at,
            created_by=customer_id,
        )

        _audit_log(
            conn,
            at=upload_at,
            actor_user_id=customer_id,
            org_id=org_id,
            action="upload",
            entity_type="org_artifact",
            entity_id=str(int(oa_id)),
            before={
                "status": "missing",
                "current_file_version_id": None,
                "audited_file_version_id": None,
                "audited_at": None,
                "audited_by_user_id": None,
                "review_status": "pending",
            },
            after={
                "status": "uploaded",
                "current_file_version_id": fv_id,
                "audited_file_version_id": None,
                "audited_at": None,
                "audited_by_user_id": None,
                "review_status": "pending",
            },
        )

        aud_at = upload_at + datetime.resolution
        _set_oa(
            conn,
            oa_id=int(oa_id),
            cur_fv=fv_id,
            review_status="approved",
            updated_at=aud_at,
            updated_by=auditor_id,
            audited_fv=fv_id,
            audited_at=aud_at,
            audited_by=auditor_id,
        )
        _audit_log(
            conn,
            at=aud_at,
            actor_user_id=auditor_id,
            org_id=org_id,
            action="audit",
            entity_type="org_artifact",
            entity_id=str(int(oa_id)),
            before={
                "audited_file_version_id": None,
                "audited_at": None,
                "audited_by_user_id": None,
                "current_file_version_id": fv_id,
                "review_status": "pending",
            },
            after={
                "audited_file_version_id": fv_id,
                "audited_at": aud_at.isoformat(),
                "audited_by_user_id": auditor_id,
                "current_file_version_id": fv_id,
                "review_status": "approved",
            },
        )
        changed += 1

    return changed


def _ensure_second_approved_version_for_short_name(
    conn: sa.Connection,
    *,
    org_id: int,
    short_name: str,
    upload_at: datetime,
    customer_id: int | None,
    auditor_id: int | None,
) -> int:
    """
    Добавляет v2 (upload+audit) для org_artifacts с данным short_name, но только если версий < 2.
    В итоге current == audited и review_status=approved (радар не "ломается"), но история появляется.
    """
    rows = conn.execute(
        sa.text(
            """
            SELECT oa.id
            FROM org_artifacts oa
            JOIN artifacts a ON a.id = oa.artifact_id
            WHERE oa.org_id = :o AND a.short_name = :sn
            ORDER BY oa.id
            """
        ),
        {"o": int(org_id), "sn": short_name},
    ).all()
    if not rows:
        return 0

    touched = 0
    for (oa_id,) in rows:
        cnt = conn.execute(
            sa.text("SELECT count(*) FROM file_versions WHERE org_artifact_id=:oa"),
            {"oa": int(oa_id)},
        ).scalar()
        if int(cnt or 0) >= 2:
            continue

        before_row = conn.execute(
            sa.text(
                """
                SELECT status, current_file_version_id, audited_file_version_id, audited_at, audited_by_user_id, review_status
                FROM org_artifacts WHERE id=:id
                """
            ),
            {"id": int(oa_id)},
        ).first()
        before = {}
        if before_row:
            before = {
                "status": before_row[0],
                "current_file_version_id": before_row[1],
                "audited_file_version_id": before_row[2],
                "audited_at": (before_row[3].isoformat() if before_row[3] else None),
                "audited_by_user_id": before_row[4],
                "review_status": before_row[5],
            }

        blob = (f"DEMO index_kb v2\nshort_name={short_name}\n").encode("utf-8")
        fv2 = _insert_fv(
            conn,
            oa_id=int(oa_id),
            filename=f"{short_name}_demo_v2.txt",
            content_type="text/plain",
            blob=blob,
            created_at=upload_at,
            created_by=customer_id,
        )
        # upload
        _set_oa(conn, oa_id=int(oa_id), cur_fv=fv2, review_status="pending", updated_at=upload_at, updated_by=customer_id)
        _audit_log(
            conn,
            at=upload_at,
            actor_user_id=customer_id,
            org_id=org_id,
            action="upload",
            entity_type="org_artifact",
            entity_id=str(int(oa_id)),
            before=(before or None),
            after={
                "status": "uploaded",
                "current_file_version_id": fv2,
                "audited_file_version_id": None,
                "audited_at": None,
                "audited_by_user_id": None,
                "review_status": "pending",
            },
        )
        # audit (approved)
        aud_at = upload_at + datetime.resolution
        _set_oa(
            conn,
            oa_id=int(oa_id),
            cur_fv=fv2,
            review_status="approved",
            updated_at=aud_at,
            updated_by=auditor_id,
            audited_fv=fv2,
            audited_at=aud_at,
            audited_by=auditor_id,
        )
        _audit_log(
            conn,
            at=aud_at,
            actor_user_id=auditor_id,
            org_id=org_id,
            action="audit",
            entity_type="org_artifact",
            entity_id=str(int(oa_id)),
            before={
                "audited_file_version_id": None,
                "audited_at": None,
                "audited_by_user_id": None,
                "current_file_version_id": fv2,
                "review_status": "pending",
            },
            after={
                "audited_file_version_id": fv2,
                "audited_at": aud_at.isoformat(),
                "audited_by_user_id": auditor_id,
                "current_file_version_id": fv2,
                "review_status": "approved",
            },
        )
        touched += 1

    return touched


def _pick_short_names_for_radar(conn: sa.Connection, *, sheet_name: str) -> list[str]:
    # 1) Coverage per section (group_code): pick up to 2 items from each group
    groups = conn.execute(
        sa.text(
            """
            SELECT t.group_code, MIN(t.id) AS min_id
            FROM index_kb_template_rows t
            WHERE t.sheet_name=:s AND t.kind='item' AND t.short_name <> ''
            GROUP BY t.group_code
            ORDER BY min_id
            """
        ),
        {"s": sheet_name},
    ).all()

    picked: list[str] = []
    for g, _min_id in groups:
        if not g:
            continue
        rows = conn.execute(
            sa.text(
                """
                SELECT t.short_name
                FROM index_kb_template_rows t
                JOIN artifacts a ON upper(a.short_name)=upper(t.short_name)
                WHERE t.sheet_name=:s AND t.kind='item' AND t.short_name <> '' AND t.group_code=:g
                ORDER BY t.sort_order, t.id
                LIMIT 2
                """
            ),
            {"s": sheet_name, "g": str(g)},
        ).all()
        for (sn,) in rows:
            if sn:
                picked.append(str(sn))

    # 2) Coverage by "topics" that auditors expect to see in filters
    # (only those that exist in artifacts for this sheet)
    topics = [
        "Восстановление",
        "Мониторинг",
        "Реагирование",
        "Нормативное соответствие",
        "Управление ИБ",
        "СЗИ",
    ]
    for t in topics:
        rows = conn.execute(
            sa.text(
                """
                SELECT t.short_name, MIN(t.id) AS min_id
                FROM index_kb_template_rows t
                JOIN artifacts a ON upper(a.short_name)=upper(t.short_name)
                WHERE t.sheet_name=:s AND t.kind='item' AND t.short_name <> '' AND a.topic=:topic
                GROUP BY t.short_name
                ORDER BY min_id
                LIMIT 6
                """
            ),
            {"s": sheet_name, "topic": t},
        ).all()
        for (sn, _min_id) in rows:
            if sn:
                picked.append(str(sn))

    # dedupe preserving order
    seen: set[str] = set()
    out: list[str] = []
    for sn in picked:
        if sn in seen:
            continue
        seen.add(sn)
        out.append(sn)
    return out


def upgrade() -> None:
    conn = op.get_bind()

    org_id = _get_org_id(conn)
    if not org_id:
        return

    # Demo data toggle — same as 0016
    if (os.getenv("SEED_DEMO_DATA") or "").strip() not in ("1", "true", "True", "yes", "on"):
        return

    customer_id = _get_user_id(conn, login=CUSTOMER_LOGIN)
    auditor_id = _get_user_id(conn, login=AUDITOR_LOGIN)

    # Ensure we have org_artifacts materialized (should already be done by 0016, but keep safe)
    conn.execute(
        sa.text(
            """
            INSERT INTO org_artifacts (org_id, artifact_id, status, review_status, created_at, updated_at)
            SELECT :org_id, a.id, 'missing', 'pending', :now, :now
            FROM artifacts a
            WHERE NOT EXISTS (
              SELECT 1 FROM org_artifacts oa
              WHERE oa.org_id = :org_id AND oa.artifact_id = a.id
            )
            """
        ),
        {"org_id": int(org_id), "now": _utc(datetime(2026, 2, 2, 11, 0, 0))},
    )

    # Pick more points so radar becomes "full" across sections and topics.
    uib = _pick_short_names_for_radar(conn, sheet_name="Управление ИБ")
    szi = _pick_short_names_for_radar(conn, sheet_name="СЗИ")

    base_uib = datetime(2025, 11, 27, 10, 0, 0)
    base_szi = datetime(2025, 12, 3, 10, 0, 0)

    # Stage 1: ensure approved for all picked (idempotent)
    for i, sn in enumerate(uib):
        _ensure_approved_for_short_name(
            conn,
            org_id=int(org_id),
            short_name=sn,
            upload_at=base_uib + timedelta(minutes=i),
            customer_id=customer_id,
            auditor_id=auditor_id,
        )
    for i, sn in enumerate(szi):
        _ensure_approved_for_short_name(
            conn,
            org_id=int(org_id),
            short_name=sn,
            upload_at=base_szi + timedelta(minutes=i),
            customer_id=customer_id,
            auditor_id=auditor_id,
        )

    # Stage 2: add some history (v2 approved) for a subset, to make UI more "alive"
    for i, sn in enumerate(uib[:20]):
        _ensure_second_approved_version_for_short_name(
            conn,
            org_id=int(org_id),
            short_name=sn,
            upload_at=datetime(2026, 2, 12, 10, 0, 0) + timedelta(minutes=i),
            customer_id=customer_id,
            auditor_id=auditor_id,
        )
    for i, sn in enumerate(szi[:20]):
        _ensure_second_approved_version_for_short_name(
            conn,
            org_id=int(org_id),
            short_name=sn,
            upload_at=datetime(2026, 2, 13, 10, 0, 0) + timedelta(minutes=i),
            customer_id=customer_id,
            auditor_id=auditor_id,
        )


def downgrade() -> None:
    pass

