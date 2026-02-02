"""backfill realistic audit_log + comments for 0019 seed

We do NOT modify already-applied 0019 migration (bad practice).
Instead, we add a follow-up seed migration that:
- Adds missing audit_log rows with realistic before_json/after_json
- Adds org_artifact_comments + audit_log "comment" for needs_correction cases

Only affects demo data, guarded by SEED_DEMO_DATA=1.

Revision ID: 0020_seed_medline_full_log
Revises: 0019_seed_medline_folders
Create Date: 2026-02-02
"""

from __future__ import annotations

import json
import os
from datetime import datetime

import sqlalchemy as sa
from alembic import op


revision = "0020_seed_medline_full_log"
down_revision = "0019_seed_medline_folders"
branch_labels = None
depends_on = None


SYSTEM_ORG_NAME = "Default"
AUDITOR_LOGIN = "auditor"
CUSTOMER_LOGIN = "medline"

SEED_MARK = "0019_full"


def _utc(dt: datetime) -> datetime:
    return dt


def _get_demo_org_id(conn: sa.Connection) -> int | None:
    oid = conn.execute(sa.text("SELECT id FROM organizations WHERE name <> :n ORDER BY id ASC LIMIT 1"), {"n": SYSTEM_ORG_NAME}).scalar()
    return int(oid) if oid is not None else None


def _get_user_id(conn: sa.Connection, *, login: str) -> int | None:
    uid = conn.execute(sa.text("SELECT id FROM users WHERE login = :l"), {"l": login}).scalar()
    return int(uid) if uid is not None else None


def _audit_log(
    conn: sa.Connection,
    *,
    at: datetime,
    actor_user_id: int | None,
    org_id: int,
    action: str,
    oa_id: int,
    before: dict | None,
    after: dict | None,
) -> None:
    # Idempotency marker lives in after_json.seed
    if after is None:
        after = {}
    after = dict(after)
    after.setdefault("seed", SEED_MARK)

    conn.execute(
        sa.text(
            """
            INSERT INTO audit_log (at, actor_user_id, org_id, action, entity_type, entity_id, before_json, after_json, ip, user_agent)
            VALUES (:at, :actor, :org, :action, 'org_artifact', :eid,
                    CAST(:before AS jsonb), CAST(:after AS jsonb), '', '')
            """
        ),
        {
            "at": _utc(at),
            "actor": int(actor_user_id) if actor_user_id else None,
            "org": int(org_id),
            "action": action,
            "eid": str(int(oa_id)),
            "before": (json.dumps(before, ensure_ascii=False) if before is not None else None),
            "after": (json.dumps(after, ensure_ascii=False) if after is not None else None),
        },
    )


def _already_backfilled(conn: sa.Connection, *, oa_id: int) -> bool:
    x = conn.execute(
        sa.text(
            """
            SELECT 1
            FROM audit_log
            WHERE entity_type='org_artifact'
              AND entity_id=:eid
              AND (after_json->>'seed') = :seed
            LIMIT 1
            """
        ),
        {"eid": str(int(oa_id)), "seed": SEED_MARK},
    ).first()
    return bool(x)


def _ensure_comment(conn: sa.Connection, *, org_id: int, oa_id: int, author_id: int | None, text: str, at: datetime) -> None:
    exists = conn.execute(
        sa.text(
            """
            SELECT 1 FROM org_artifact_comments
            WHERE org_artifact_id=:oa AND comment_text LIKE :m
            LIMIT 1
            """
        ),
        {"oa": int(oa_id), "m": f"%{SEED_MARK}%"},
    ).first()
    if exists:
        return
    conn.execute(
        sa.text(
            """
            INSERT INTO org_artifact_comments (org_id, org_artifact_id, author_user_id, comment_text, created_at)
            VALUES (:o,:oa,:u,:t,:at)
            """
        ),
        {"o": int(org_id), "oa": int(oa_id), "u": int(author_id) if author_id else None, "t": text, "at": _utc(at)},
    )


def upgrade() -> None:
    conn = op.get_bind()

    if (os.getenv("SEED_DEMO_DATA") or "").strip() not in ("1", "true", "True", "yes", "on"):
        return

    org_id = _get_demo_org_id(conn)
    if not org_id:
        return

    customer_id = _get_user_id(conn, login=CUSTOMER_LOGIN)
    auditor_id = _get_user_id(conn, login=AUDITOR_LOGIN)

    prefixes = ("ВССТ", "МНТ", "НС", "РЕАГ")

    # Only touch org_artifacts that were seeded by 0019:
    # - current file name contains "_seed_" OR any version contains "_seed_"
    rows = conn.execute(
        sa.text(
            """
            SELECT oa.id,
                   oa.review_status,
                   oa.current_file_version_id,
                   oa.audited_file_version_id,
                   oa.audited_at,
                   oa.audited_by_user_id,
                   a.short_name,
                   cur.original_filename AS cur_fn,
                   cur.created_at AS cur_uploaded_at
            FROM org_artifacts oa
            JOIN artifacts a ON a.id=oa.artifact_id
            LEFT JOIN file_versions cur ON cur.id=oa.current_file_version_id
            WHERE oa.org_id=:o
              AND split_part(a.short_name,'.',1) = ANY(:prefs)
              AND (
                cur.original_filename LIKE '%_seed_%'
                OR EXISTS (
                  SELECT 1 FROM file_versions fv
                  WHERE fv.org_artifact_id=oa.id AND fv.original_filename LIKE '%_seed_%'
                )
              )
            ORDER BY oa.id
            """
        ),
        {"o": int(org_id), "prefs": list(prefixes)},
    ).all()

    for oa_id, rs, cur_fv_id, aud_fv_id, aud_at, aud_by, sn, cur_fn, cur_up_at in rows:
        oa_id = int(oa_id)
        if _already_backfilled(conn, oa_id=oa_id):
            continue

        # Identify seed versions (v1/v2) by version_no.
        v1 = conn.execute(
            sa.text(
                """
                SELECT id, version_no, original_filename, created_at
                FROM file_versions
                WHERE org_artifact_id=:oa AND original_filename LIKE '%_seed_%'
                ORDER BY version_no ASC, id ASC
                LIMIT 1
                """
            ),
            {"oa": int(oa_id)},
        ).first()
        if not v1:
            continue
        v1_id, v1_no, v1_fn, v1_at = int(v1[0]), int(v1[1]), str(v1[2] or ""), v1[3]

        v2 = conn.execute(
            sa.text(
                """
                SELECT id, version_no, original_filename, created_at
                FROM file_versions
                WHERE org_artifact_id=:oa AND version_no>:v1 AND original_filename LIKE '%_seed_v2_%'
                ORDER BY version_no ASC, id ASC
                LIMIT 1
                """
            ),
            {"oa": int(oa_id), "v1": int(v1_no)},
        ).first()
        v2_id = int(v2[0]) if v2 else None
        v2_fn = str(v2[2] or "") if v2 else ""
        v2_at = v2[3] if v2 else None

        # Reconstruct a realistic sequence.
        missing_before = {
            "status": "missing",
            "current_file_version_id": None,
            "audited_file_version_id": None,
            "audited_at": None,
            "audited_by_user_id": None,
            "review_status": "pending",
        }
        after_upload_v1 = {
            "status": "uploaded",
            "current_file_version_id": v1_id,
            "audited_file_version_id": None,
            "audited_at": None,
            "audited_by_user_id": None,
            "review_status": "pending",
            "filename": v1_fn,
            "short_name": str(sn or ""),
        }
        _audit_log(conn, at=v1_at, actor_user_id=customer_id, org_id=int(org_id), action="upload", oa_id=oa_id, before=missing_before, after=after_upload_v1)

        rs_s = str(rs or "")

        if rs_s == "approved" and aud_fv_id and cur_fv_id and int(aud_fv_id) == int(cur_fv_id):
            # approved current
            a_at = aud_at or (v1_at + datetime.resolution)
            _audit_log(
                conn,
                at=a_at,
                actor_user_id=(int(aud_by) if aud_by else auditor_id),
                org_id=int(org_id),
                action="audit",
                oa_id=oa_id,
                before={"review_status": "pending", "current_file_version_id": v1_id, "audited_file_version_id": None},
                after={
                    "review_status": "approved",
                    "current_file_version_id": int(cur_fv_id),
                    "audited_file_version_id": int(aud_fv_id),
                    "audited_at": (a_at.isoformat() if a_at else None),
                    "audited_by_user_id": (int(aud_by) if aud_by else auditor_id),
                    "filename": str(cur_fn or ""),
                },
            )
            continue

        if rs_s == "needs_correction":
            # comment + audit_needs_correction
            c_at = (v1_at + datetime.resolution)
            comment_text = f"Нужна корректировка: обновите документ и приложите подтверждение. ({SEED_MARK})"
            _ensure_comment(conn, org_id=int(org_id), oa_id=oa_id, author_id=(auditor_id or aud_by), text=comment_text, at=c_at)
            _audit_log(
                conn,
                at=c_at,
                actor_user_id=(auditor_id or aud_by),
                org_id=int(org_id),
                action="comment",
                oa_id=oa_id,
                before=None,
                after={"comment": comment_text},
            )
            _audit_log(
                conn,
                at=c_at,
                actor_user_id=(auditor_id or aud_by),
                org_id=int(org_id),
                action="audit_needs_correction",
                oa_id=oa_id,
                before={"review_status": "pending", "current_file_version_id": v1_id, "audited_file_version_id": None},
                after={
                    "review_status": "needs_correction",
                    "current_file_version_id": int(cur_fv_id) if cur_fv_id else v1_id,
                    "audited_file_version_id": None,
                    "comment": comment_text,
                },
            )
            continue

        # changed: has audited, but current differs (our 0019 logic keeps audited to show changed)
        if aud_fv_id and cur_fv_id and int(aud_fv_id) != int(cur_fv_id) and v2_id and int(cur_fv_id) == int(v2_id):
            a_at = aud_at or (v1_at + datetime.resolution)
            _audit_log(
                conn,
                at=a_at,
                actor_user_id=(int(aud_by) if aud_by else auditor_id),
                org_id=int(org_id),
                action="audit",
                oa_id=oa_id,
                before={"review_status": "pending", "current_file_version_id": v1_id, "audited_file_version_id": None},
                after={
                    "review_status": "approved",
                    "current_file_version_id": v1_id,
                    "audited_file_version_id": int(aud_fv_id),
                    "audited_at": (a_at.isoformat() if a_at else None),
                    "audited_by_user_id": (int(aud_by) if aud_by else auditor_id),
                    "filename": v1_fn,
                },
            )
            # upload v2 (audit reset in UI; in data we keep audited to show "changed")
            up2_at = v2_at or (cur_up_at or (v1_at))
            _audit_log(
                conn,
                at=up2_at,
                actor_user_id=customer_id,
                org_id=int(org_id),
                action="upload",
                oa_id=oa_id,
                before={
                    "review_status": "approved",
                    "current_file_version_id": v1_id,
                    "audited_file_version_id": int(aud_fv_id),
                    "audited_at": (a_at.isoformat() if a_at else None),
                },
                after={
                    "review_status": "pending",
                    "current_file_version_id": int(cur_fv_id),
                    "audited_file_version_id": int(aud_fv_id),
                    "filename": v2_fn or str(cur_fn or ""),
                    "note": "changed_after_audit",
                },
            )
            continue

        # default: pending (uploaded, no audit)
        # already have upload; nothing else to add.


def downgrade() -> None:
    # Seed-only: do not delete logs/comments on downgrade.
    pass

