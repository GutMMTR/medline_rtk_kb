"""seed more demo organizations + artifact activity

Adds:
- Rename demo org "Медлайн 2" -> "Макомнет" (if it exists and the new name is free)
- Create additional demo organizations:
  Мортон, Твинго, Глобус, Джиэнси, Центральный телеграф, Розничные системы,
  Цифровое телевидение, Центр хранения данных
- For each org: materialize org_artifacts for all artifacts and seed a small,
  illustrative subset of uploads with mixed statuses (approved/pending/needs_correction/changed)

Guarded by SEED_DEMO_DATA=1 and idempotent.

Revision ID: 0023_seed_more_orgs_artifacts
Revises: 0022_chat_threads_messages
Create Date: 2026-02-04
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta

import sqlalchemy as sa
from alembic import op


revision = "0023_seed_more_orgs_artifacts"
down_revision = "0022_chat_threads_messages"
branch_labels = None
depends_on = None


SYSTEM_ORG_NAME = "Default"
ORG_OLD_NAME = "Медлайн 2"
ORG2_NAME = "Макомнет"
AUDITOR_LOGIN = "auditor"
CUSTOMER_LOGIN = "medline"

SEED_MARK = "0023_more_orgs"


NEW_ORG_NAMES: list[str] = [
    ORG2_NAME,  # via rename OR create
    "Мортон",
    "Твинго",
    "Глобус",
    "Джиэнси",
    "Центральный телеграф",
    "Розничные системы",
    "Цифровое телевидение",
    "Центр хранения данных",
]

# Slightly different levels to make dashboards more interesting (if levels exist)
ORG_LEVEL_CODE: dict[str, str] = {
    ORG2_NAME: "L2",
    "Мортон": "L3",
    "Твинго": "L1",
    "Глобус": "L2",
    "Джиэнси": "L3",
    "Центральный телеграф": "L2",
    "Розничные системы": "L3",
    "Цифровое телевидение": "L1",
    "Центр хранения данных": "L3",
}


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


def _seed_enabled() -> bool:
    return (os.getenv("SEED_DEMO_DATA") or "").strip() in ("1", "true", "True", "yes", "on")


def _ensure_org(conn: sa.Connection, *, name: str, created_at: datetime) -> int:
    oid = conn.execute(sa.text("SELECT id FROM organizations WHERE name = :n"), {"n": name}).scalar()
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
        {"name": name, "at": _utc(created_at)},
    ).scalar_one()
    return int(oid)


def _maybe_rename_org(conn: sa.Connection) -> int | None:
    """
    Rename "Медлайн 2" -> "Макомнет" if:
    - old exists
    - new does not exist

    Returns resulting org id if either exists, else None.
    """
    new_id = conn.execute(sa.text("SELECT id FROM organizations WHERE name = :n"), {"n": ORG2_NAME}).scalar()
    if new_id is not None:
        return int(new_id)
    old_id = conn.execute(sa.text("SELECT id FROM organizations WHERE name = :n"), {"n": ORG_OLD_NAME}).scalar()
    if old_id is not None:
        conn.execute(sa.text("UPDATE organizations SET name=:new WHERE id=:id"), {"new": ORG2_NAME, "id": int(old_id)})
        return int(old_id)
    return None


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


def _seed_activity_for_org(conn: sa.Connection, *, org_id: int, org_name: str, customer_id: int | None, auditor_id: int | None) -> None:
    """
    Seed a small, varied subset (default ~60) so UI/dashboards look alive,
    without creating an enormous DB.
    """
    uploaded_cnt = conn.execute(
        sa.text("SELECT count(*) FROM org_artifacts WHERE org_id=:o AND current_file_version_id IS NOT NULL"),
        {"o": int(org_id)},
    ).scalar()
    if int(uploaded_cnt or 0) >= 60:
        return

    # Pick a mix:
    # - 40 Index KB artifacts (IKB:%) for radar/status dashboards
    # - 20 "real" catalog artifacts from folder-friendly prefixes (for explorer)
    # Use a deterministic offset per org so orgs differ.
    # NOTE: built-in hash() is randomized by PYTHONHASHSEED and is NOT stable across runs.
    # Use sha256-based integer instead for deterministic seeding.
    org_ord = int(hashlib.sha256((org_name or "").encode("utf-8")).hexdigest()[:8], 16) % 7
    ikb_rows = conn.execute(
        sa.text(
            """
            SELECT oa.id AS oa_id, a.short_name AS sn
            FROM org_artifacts oa
            JOIN artifacts a ON a.id=oa.artifact_id
            WHERE oa.org_id=:o
              AND a.artifact_key LIKE 'IKB:%'
              AND oa.current_file_version_id IS NULL
            ORDER BY a.short_name, oa.id
            OFFSET :off
            LIMIT 40
            """
        ),
        {"o": int(org_id), "off": int(org_ord * 5)},
    ).all()

    extra_rows = conn.execute(
        sa.text(
            """
            SELECT oa.id AS oa_id, a.short_name AS sn
            FROM org_artifacts oa
            JOIN artifacts a ON a.id=oa.artifact_id
            WHERE oa.org_id=:o
              AND a.artifact_key IS NULL
              AND oa.current_file_version_id IS NULL
              AND split_part(a.short_name,'.',1) = ANY(:prefs)
            ORDER BY a.short_name, oa.id
            OFFSET :off
            LIMIT 20
            """
        ),
        {"o": int(org_id), "prefs": ["ВССТ", "МНТ", "НС", "РЕАГ"], "off": int(org_ord * 3)},
    ).all()

    rows = list(ikb_rows) + list(extra_rows)
    if not rows:
        return

    base = datetime(2026, 2, 1, 9, 0, 0) + timedelta(hours=org_ord * 6)
    fmt_cycle = [
        ("txt", "text/plain", None),
        ("png", "image/png", PNG_1X1),
        ("pdf", "application/pdf", PDF_MIN),
    ]

    for i, (oa_id, sn) in enumerate(rows, start=1):
        oa_id = int(oa_id)
        sn = str(sn or "artifact")
        bucket = i % 20

        ext, ct, blob0 = fmt_cycle[i % len(fmt_cycle)]
        blob = blob0 if blob0 is not None else (f"DEMO {SEED_MARK}\norg={org_name}\nshort_name={sn}\n").encode("utf-8")
        up_at = base + timedelta(minutes=i * 7)
        fn = f"{sn}_{SEED_MARK}_{ext}.{ext}"

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
            "review_status": "pending",
            "filename": fn,
            "short_name": sn,
        }

        # changed (~10%)
        if bucket in (0, 1):
            aud_at = up_at + datetime.resolution
            _set_oa(conn, oa_id=oa_id, cur_fv=fv1, review_status="approved", updated_at=aud_at, updated_by=auditor_id, audited_fv=fv1, audited_at=aud_at, audited_by=auditor_id)
            _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org_id), action="upload", oa_id=oa_id, before=missing_before, after=after_upload)
            _audit_log(conn, at=aud_at, actor_user_id=auditor_id, org_id=int(org_id), action="audit", oa_id=oa_id, before={"review_status": "pending"}, after={"review_status": "approved", "audited_file_version_id": fv1})

            up2 = up_at + timedelta(days=7)
            fv2 = _insert_fv(
                conn,
                oa_id=oa_id,
                filename=f"{sn}_{SEED_MARK}_v2.txt",
                content_type="text/plain",
                blob=(f"DEMO {SEED_MARK} v2\norg={org_name}\nshort_name={sn}\n").encode("utf-8"),
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
                org_id=int(org_id),
                action="upload",
                oa_id=oa_id,
                before={"review_status": "approved", "current_file_version_id": fv1, "audited_file_version_id": fv1},
                after={"review_status": "pending", "current_file_version_id": fv2, "audited_file_version_id": fv1, "filename": f"{sn}_{SEED_MARK}_v2.txt"},
            )
            continue

        # needs correction (~15%)
        if bucket in (2, 3, 4):
            c_at = up_at + datetime.resolution
            _set_oa(conn, oa_id=oa_id, cur_fv=fv1, review_status="needs_correction", updated_at=c_at, updated_by=auditor_id)
            _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org_id), action="upload", oa_id=oa_id, before=missing_before, after=after_upload)
            comment = f"Нужна корректировка: обновите документ и приложите подтверждение. ({SEED_MARK})"
            _ensure_comment(conn, org_id=int(org_id), oa_id=oa_id, author_id=auditor_id, text=comment, at=c_at)
            _audit_log(conn, at=c_at, actor_user_id=auditor_id, org_id=int(org_id), action="comment", oa_id=oa_id, before=None, after={"comment": comment})
            _audit_log(conn, at=c_at, actor_user_id=auditor_id, org_id=int(org_id), action="audit_needs_correction", oa_id=oa_id, before={"review_status": "pending"}, after={"review_status": "needs_correction"})
            continue

        # pending (~35%)
        if bucket in (5, 6, 7, 8, 9, 10, 11):
            _set_oa(conn, oa_id=oa_id, cur_fv=fv1, review_status="pending", updated_at=up_at, updated_by=customer_id)
            _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org_id), action="upload", oa_id=oa_id, before=missing_before, after=after_upload)
            continue

        # approved (rest)
        aud_at = up_at + datetime.resolution
        _set_oa(conn, oa_id=oa_id, cur_fv=fv1, review_status="approved", updated_at=aud_at, updated_by=auditor_id, audited_fv=fv1, audited_at=aud_at, audited_by=auditor_id)
        _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org_id), action="upload", oa_id=oa_id, before=missing_before, after=after_upload)
        _audit_log(conn, at=aud_at, actor_user_id=auditor_id, org_id=int(org_id), action="audit", oa_id=oa_id, before={"review_status": "pending"}, after={"review_status": "approved", "audited_file_version_id": fv1})


def upgrade() -> None:
    conn = op.get_bind()

    if not _seed_enabled():
        return

    customer_id = _get_user_id(conn, login=CUSTOMER_LOGIN)
    auditor_id = _get_user_id(conn, login=AUDITOR_LOGIN)

    # 1) Rename "Медлайн 2" -> "Макомнет" if applicable
    org2_id = _maybe_rename_org(conn)
    if org2_id is None:
        org2_id = _ensure_org(conn, name=ORG2_NAME, created_at=datetime(2026, 2, 4, 10, 0, 0))

    # 2) Ensure all orgs exist
    org_ids: dict[str, int] = {ORG2_NAME: int(org2_id)}
    base_at = datetime(2026, 2, 4, 10, 10, 0)
    for idx, name in enumerate(NEW_ORG_NAMES):
        if name == ORG2_NAME:
            continue
        org_ids[name] = _ensure_org(conn, name=name, created_at=base_at + timedelta(minutes=idx))

    # 3) Assign levels (if exist) and materialize org_artifacts
    for idx, (name, oid) in enumerate(org_ids.items()):
        lvl_code = ORG_LEVEL_CODE.get(name, "L3")
        lvl_id = _level_id(conn, code=lvl_code)
        if lvl_id:
            conn.execute(sa.text("UPDATE organizations SET artifact_level_id=:l WHERE id=:o"), {"l": int(lvl_id), "o": int(oid)})
        _materialize_org_artifacts(conn, org_id=int(oid), now=datetime(2026, 2, 4, 11, 0, 0) + timedelta(minutes=idx))

    # 4) Seed a small activity subset per org
    for name, oid in org_ids.items():
        _seed_activity_for_org(conn, org_id=int(oid), org_name=name, customer_id=customer_id, auditor_id=auditor_id)


def downgrade() -> None:
    # Seed-only migration: do not delete data on downgrade.
    pass

