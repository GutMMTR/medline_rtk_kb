"""seed demo org "Медлайн 2" for multi-org dashboards

Goal:
- Create an extra demo org so dashboards can be compared across multiple orgs.
- Seed a small but illustrative subset of uploads with mixed statuses:
  approved / pending / needs_correction / changed + some missing.
- Focus on Index KB artifacts (artifact_key LIKE 'IKB:%') so radar/status dashboards change clearly.

Guarded by SEED_DEMO_DATA=1 and idempotent.

Revision ID: 0021_seed_medline2
Revises: 0020_seed_medline_full_log
Create Date: 2026-02-02
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta

import sqlalchemy as sa
from alembic import op


revision = "0021_seed_medline2"
down_revision = "0020_seed_medline_full_log"
branch_labels = None
depends_on = None


ORG2_NAME = "Медлайн 2"
SYSTEM_ORG_NAME = "Default"
AUDITOR_LOGIN = "auditor"
CUSTOMER_LOGIN = "medline"
SEED_MARK = "0021_medline2"


def _utc(dt: datetime) -> datetime:
    return dt


def _sha256_hex(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def _get_user_id(conn: sa.Connection, *, login: str) -> int | None:
    uid = conn.execute(sa.text("SELECT id FROM users WHERE login = :l"), {"l": login}).scalar()
    return int(uid) if uid is not None else None


def _level_id(conn: sa.Connection, *, code: str) -> int | None:
    x = conn.execute(
        sa.text("SELECT id FROM artifact_levels WHERE code=:c ORDER BY id DESC LIMIT 1"),
        {"c": (code or "").strip().upper()},
    ).scalar()
    return int(x) if x is not None else None


def _ensure_org(conn: sa.Connection) -> int:
    oid = conn.execute(sa.text("SELECT id FROM organizations WHERE name = :n"), {"n": ORG2_NAME}).scalar()
    if oid is not None:
        return int(oid)
    oid = conn.execute(
        sa.text(
            """
            INSERT INTO organizations (name, created_at, created_by_user_id, created_via, artifact_level_id)
            VALUES (:name, :at, NULL, 'system', NULL)
            RETURNING id
            """
        ),
        {"name": ORG2_NAME, "at": _utc(datetime(2026, 2, 2, 12, 0, 0))},
    ).scalar_one()
    return int(oid)


def _materialize_org_artifacts(conn: sa.Connection, *, org_id: int, now: datetime) -> None:
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
        {"org_id": int(org_id), "now": _utc(now)},
    )


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
    org_id: int,
    action: str,
    oa_id: int,
    before: dict | None,
    after: dict | None,
) -> None:
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


PNG_1X1 = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\x0cIDATx\x9cc\xf8\xff\xff?\x03\x00\x08\xfc\x02\xfe\xa7@\x9aS\x00\x00\x00\x00IEND\xaeB`\x82"
)
PDF_MIN = b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\nxref\n0 2\n0000000000 65535 f \n0000000010 00000 n \ntrailer\n<< /Size 2 /Root 1 0 R >>\nstartxref\n20\n%%EOF\n"


def upgrade() -> None:
    conn = op.get_bind()

    if (os.getenv("SEED_DEMO_DATA") or "").strip() not in ("1", "true", "True", "yes", "on"):
        return

    org2_id = _ensure_org(conn)

    # Make org2 different from org1 in "current" indicator (L2 instead of L3).
    l2 = _level_id(conn, code="L2")
    if l2:
        conn.execute(sa.text("UPDATE organizations SET artifact_level_id=:l2 WHERE id=:o"), {"l2": int(l2), "o": int(org2_id)})

    _materialize_org_artifacts(conn, org_id=int(org2_id), now=datetime(2026, 2, 2, 12, 0, 0))

    # Idempotency: if already enough uploads for this org — do nothing.
    uploaded_cnt = conn.execute(
        sa.text("SELECT count(*) FROM org_artifacts WHERE org_id=:o AND current_file_version_id IS NOT NULL"),
        {"o": int(org2_id)},
    ).scalar()
    if int(uploaded_cnt or 0) >= 140:
        return

    customer_id = _get_user_id(conn, login=CUSTOMER_LOGIN)
    auditor_id = _get_user_id(conn, login=AUDITOR_LOGIN)

    # Pick artifacts for Index KB (IKB:...) so dashboards/radar change clearly.
    # Strategy: take some from each sheet, spread across groups via sort_order.
    ikb_rows = conn.execute(
        sa.text(
            """
            SELECT oa.id AS oa_id, a.short_name AS sn, a.topic AS topic, a.domain AS domain
            FROM org_artifacts oa
            JOIN artifacts a ON a.id=oa.artifact_id
            WHERE oa.org_id=:o
              AND a.artifact_key LIKE 'IKB:%'
            ORDER BY a.topic, a.domain, a.short_name, oa.id
            LIMIT 120
            """
        ),
        {"o": int(org2_id)},
    ).all()

    # Also add a small amount of "real" catalog artifacts (non-IKB) to diversify uploads timeline.
    extra_rows = conn.execute(
        sa.text(
            """
            SELECT oa.id AS oa_id, a.short_name AS sn, a.topic AS topic, a.domain AS domain
            FROM org_artifacts oa
            JOIN artifacts a ON a.id=oa.artifact_id
            WHERE oa.org_id=:o
              AND a.artifact_key IS NULL
              AND oa.current_file_version_id IS NULL
            ORDER BY a.short_name, oa.id
            LIMIT 30
            """
        ),
        {"o": int(org2_id)},
    ).all()

    rows = list(ikb_rows) + list(extra_rows)

    # Deterministic status mix: make org2 visibly different from org1.
    # approved: 40%, pending: 35%, needs_correction: 15%, changed: 10%
    base = datetime(2026, 1, 15, 10, 0, 0)
    for i, (oa_id, sn, _topic, _domain) in enumerate(rows, start=1):
        oa_id = int(oa_id)
        sn = str(sn or "artifact")
        bucket = i % 20

        ext_cycle = [
            ("txt", "text/plain", (f"DEMO {SEED_MARK}\nshort_name={sn}\n").encode("utf-8")),
            ("png", "image/png", PNG_1X1),
            ("pdf", "application/pdf", PDF_MIN),
        ]
        ext, ct, blob = ext_cycle[i % len(ext_cycle)]
        up_at = base + timedelta(hours=i)
        fn = f"{sn}_{SEED_MARK}.{ext}"

        fv1 = _insert_fv(conn, oa_id=oa_id, filename=fn, content_type=ct, blob=blob, created_at=up_at, created_by=customer_id)

        missing_before = {
            "status": "missing",
            "current_file_version_id": None,
            "audited_file_version_id": None,
            "audited_at": None,
            "audited_by_user_id": None,
            "review_status": "pending",
        }
        after_upload = {
            "status": "uploaded",
            "current_file_version_id": fv1,
            "audited_file_version_id": None,
            "audited_at": None,
            "audited_by_user_id": None,
            "review_status": "pending",
            "filename": fn,
            "short_name": sn,
        }

        # changed (10%)
        if bucket in (0, 1):
            # v1 approved
            aud_at = up_at + datetime.resolution
            _set_oa(conn, oa_id=oa_id, cur_fv=fv1, review_status="approved", updated_at=aud_at, updated_by=auditor_id, audited_fv=fv1, audited_at=aud_at, audited_by=auditor_id)
            _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org2_id), action="upload", oa_id=oa_id, before=missing_before, after=after_upload)
            _audit_log(conn, at=aud_at, actor_user_id=auditor_id, org_id=int(org2_id), action="audit", oa_id=oa_id, before={"review_status": "pending"}, after={"review_status": "approved", "audited_file_version_id": fv1})

            # v2 upload after some days => becomes "changed"
            up2 = up_at + timedelta(days=10)
            fv2 = _insert_fv(
                conn,
                oa_id=oa_id,
                filename=f"{sn}_{SEED_MARK}_v2.txt",
                content_type="text/plain",
                blob=(f"DEMO {SEED_MARK} v2\nshort_name={sn}\n").encode("utf-8"),
                created_at=up2,
                created_by=customer_id,
            )
            conn.execute(
                sa.text(
                    """
                    UPDATE org_artifacts
                    SET status='uploaded',
                        current_file_version_id=:cur,
                        review_status='pending',
                        updated_at=:at,
                        updated_by_user_id=:by
                    WHERE id=:id
                    """
                ),
                {"id": int(oa_id), "cur": int(fv2), "at": _utc(up2), "by": int(customer_id) if customer_id else None},
            )
            _audit_log(
                conn,
                at=up2,
                actor_user_id=customer_id,
                org_id=int(org2_id),
                action="upload",
                oa_id=oa_id,
                before={"review_status": "approved", "current_file_version_id": fv1, "audited_file_version_id": fv1},
                after={"review_status": "pending", "current_file_version_id": fv2, "audited_file_version_id": fv1, "filename": f"{sn}_{SEED_MARK}_v2.txt"},
            )
            continue

        # needs correction (15%)
        if bucket in (2, 3, 4):
            c_at = up_at + datetime.resolution
            _set_oa(conn, oa_id=oa_id, cur_fv=fv1, review_status="needs_correction", updated_at=c_at, updated_by=auditor_id)
            _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org2_id), action="upload", oa_id=oa_id, before=missing_before, after=after_upload)
            comment = f"Нужна корректировка: обновите документ и приложите подтверждение. ({SEED_MARK})"
            _ensure_comment(conn, org_id=int(org2_id), oa_id=oa_id, author_id=auditor_id, text=comment, at=c_at)
            _audit_log(conn, at=c_at, actor_user_id=auditor_id, org_id=int(org2_id), action="comment", oa_id=oa_id, before=None, after={"comment": comment})
            _audit_log(conn, at=c_at, actor_user_id=auditor_id, org_id=int(org2_id), action="audit_needs_correction", oa_id=oa_id, before={"review_status": "pending"}, after={"review_status": "needs_correction"})
            continue

        # pending (35%)
        if bucket in (5, 6, 7, 8, 9, 10, 11):
            _set_oa(conn, oa_id=oa_id, cur_fv=fv1, review_status="pending", updated_at=up_at, updated_by=customer_id)
            _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org2_id), action="upload", oa_id=oa_id, before=missing_before, after=after_upload)
            continue

        # approved (rest)
        aud_at = up_at + datetime.resolution
        _set_oa(conn, oa_id=oa_id, cur_fv=fv1, review_status="approved", updated_at=aud_at, updated_by=auditor_id, audited_fv=fv1, audited_at=aud_at, audited_by=auditor_id)
        _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org2_id), action="upload", oa_id=oa_id, before=missing_before, after=after_upload)
        _audit_log(conn, at=aud_at, actor_user_id=auditor_id, org_id=int(org2_id), action="audit", oa_id=oa_id, before={"review_status": "pending"}, after={"review_status": "approved", "audited_file_version_id": fv1})


def downgrade() -> None:
    # Seed-only migration: do not delete data on downgrade.
    pass

