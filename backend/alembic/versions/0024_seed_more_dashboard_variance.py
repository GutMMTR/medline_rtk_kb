"""seed more artifact activity for clearer dashboards

Goal:
- Make dashboards clearly different when selecting multiple organizations:
  add more uploads with mixed statuses for the organizations added in 0023.

Strategy:
- For each target org, ensure a larger number of uploaded org_artifacts,
  with deterministic but org-specific status mix (approved/pending/needs_correction/changed).
- Prefer Index KB artifacts (artifact_key LIKE 'IKB:%') so Index/Radar dashboards change.
- Add a smaller portion of non-IKB artifacts from explorer-friendly prefixes.
- Idempotent: only adds more if this seed mark is not present enough for the org.

Guarded by SEED_DEMO_DATA=1.

Revision ID: 0024_seed_dash_variance
Revises: 0023_seed_more_orgs_artifacts
Create Date: 2026-02-04
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta

import sqlalchemy as sa
from alembic import op


revision = "0024_seed_dash_variance"
down_revision = "0023_seed_more_orgs_artifacts"
branch_labels = None
depends_on = None


SEED_MARK = "0024_dash"
AUDITOR_LOGIN = "auditor"
CUSTOMER_LOGIN = "medline"

# Organizations added/renamed for multi-org demos
TARGET_ORG_NAMES: list[str] = [
    "Макомнет",
    "Мортон",
    "Твинго",
    "Глобус",
    "Джиэнси",
    "Центральный телеграф",
    "Розничные системы",
    "Цифровое телевидение",
    "Центр хранения данных",
]


def _seed_enabled() -> bool:
    return (os.getenv("SEED_DEMO_DATA") or "").strip() in ("1", "true", "True", "yes", "on")


def _utc(dt: datetime) -> datetime:
    return dt


def _sha256_hex(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def _stable_mod(org_name: str, mod: int) -> int:
    h = hashlib.sha256((org_name or "").encode("utf-8")).hexdigest()
    return int(h[:8], 16) % max(1, int(mod))


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


def _already_seeded_enough(conn: sa.Connection, *, org_id: int, want: int) -> bool:
    x = conn.execute(
        sa.text(
            """
            SELECT count(*)
            FROM file_versions fv
            JOIN org_artifacts oa ON oa.id=fv.org_artifact_id
            WHERE oa.org_id=:o AND fv.original_filename LIKE :m
            """
        ),
        {"o": int(org_id), "m": f"%{SEED_MARK}%"},
    ).scalar()
    return int(x or 0) >= int(want)


def _seed_more_for_org(conn: sa.Connection, *, org_id: int, org_name: str, customer_id: int | None, auditor_id: int | None) -> None:
    # Target number of *new* seeded versions for this migration.
    # Keep modest but noticeable across many orgs.
    want_new_versions = 220
    if _already_seeded_enough(conn, org_id=org_id, want=want_new_versions):
        return

    # Org-specific mix (percentages over 100), deterministic per org
    mix_shift = _stable_mod(org_name, 17)  # 0..16
    approved_pct = 40 + (mix_shift % 21)  # 40..60
    needs_pct = 10 + ((mix_shift * 3) % 16)  # 10..25
    changed_pct = 6 + ((mix_shift * 5) % 7)  # 6..12
    pending_pct = max(5, 100 - approved_pct - needs_pct - changed_pct)

    # Pick candidates with no current file.
    # Prefer IKB artifacts to affect Index KB dashboards.
    ord_off = _stable_mod(org_name, 11)
    ikb = conn.execute(
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
            LIMIT 170
            """
        ),
        {"o": int(org_id), "off": int(ord_off * 7)},
    ).all()

    extra = conn.execute(
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
            LIMIT 80
            """
        ),
        {"o": int(org_id), "prefs": ["ВССТ", "МНТ", "НС", "РЕАГ"], "off": int(ord_off * 3)},
    ).all()

    rows = list(ikb) + list(extra)
    if not rows:
        return

    base = datetime(2026, 2, 3, 9, 0, 0) + timedelta(hours=_stable_mod(org_name, 9) * 3)
    fmt_cycle = [
        ("txt", "text/plain", None),
        ("png", "image/png", PNG_1X1),
        ("pdf", "application/pdf", PDF_MIN),
    ]

    for i, (oa_id, sn) in enumerate(rows, start=1):
        oa_id = int(oa_id)
        sn = str(sn or "artifact")

        ext, ct, blob0 = fmt_cycle[i % len(fmt_cycle)]
        blob = blob0 if blob0 is not None else (f"DEMO {SEED_MARK}\norg={org_name}\nshort_name={sn}\n").encode("utf-8")
        up_at = base + timedelta(minutes=i * 5)
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

        # Deterministic bucket in [0..99] per row, per org
        b = (i * 7 + mix_shift * 11) % 100

        # changed
        if b < changed_pct:
            aud_at = up_at + datetime.resolution
            _set_oa(
                conn,
                oa_id=oa_id,
                cur_fv=fv1,
                review_status="approved",
                updated_at=aud_at,
                updated_by=auditor_id,
                audited_fv=fv1,
                audited_at=aud_at,
                audited_by=auditor_id,
            )
            _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org_id), action="upload", oa_id=oa_id, before=missing_before, after=after_upload)
            _audit_log(conn, at=aud_at, actor_user_id=auditor_id, org_id=int(org_id), action="audit", oa_id=oa_id, before={"review_status": "pending"}, after={"review_status": "approved", "audited_file_version_id": fv1})

            up2 = up_at + timedelta(days=9)
            fv2 = _insert_fv(
                conn,
                oa_id=oa_id,
                filename=f"{sn}_{SEED_MARK}_v2.txt",
                content_type="text/plain",
                blob=(f"DEMO {SEED_MARK} v2\norg={org_name}\nshort_name={sn}\n").encode("utf-8"),
                created_at=up2,
                created_by=customer_id,
            )
            # Keep audited_* to render "Изменён" in UI
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

        # needs correction
        if b < changed_pct + needs_pct:
            c_at = up_at + datetime.resolution
            _set_oa(conn, oa_id=oa_id, cur_fv=fv1, review_status="needs_correction", updated_at=c_at, updated_by=auditor_id)
            _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org_id), action="upload", oa_id=oa_id, before=missing_before, after=after_upload)
            comment = f"Нужна корректировка: обновите документ и приложите подтверждение. ({SEED_MARK})"
            _ensure_comment(conn, org_id=int(org_id), oa_id=oa_id, author_id=auditor_id, text=comment, at=c_at)
            _audit_log(conn, at=c_at, actor_user_id=auditor_id, org_id=int(org_id), action="comment", oa_id=oa_id, before=None, after={"comment": comment})
            _audit_log(conn, at=c_at, actor_user_id=auditor_id, org_id=int(org_id), action="audit_needs_correction", oa_id=oa_id, before={"review_status": "pending"}, after={"review_status": "needs_correction"})
            continue

        # approved
        if b < changed_pct + needs_pct + approved_pct:
            aud_at = up_at + datetime.resolution
            _set_oa(
                conn,
                oa_id=oa_id,
                cur_fv=fv1,
                review_status="approved",
                updated_at=aud_at,
                updated_by=auditor_id,
                audited_fv=fv1,
                audited_at=aud_at,
                audited_by=auditor_id,
            )
            _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org_id), action="upload", oa_id=oa_id, before=missing_before, after=after_upload)
            _audit_log(conn, at=aud_at, actor_user_id=auditor_id, org_id=int(org_id), action="audit", oa_id=oa_id, before={"review_status": "pending"}, after={"review_status": "approved", "audited_file_version_id": fv1})
            continue

        # pending
        _set_oa(conn, oa_id=oa_id, cur_fv=fv1, review_status="pending", updated_at=up_at, updated_by=customer_id)
        _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org_id), action="upload", oa_id=oa_id, before=missing_before, after=after_upload)


def upgrade() -> None:
    conn = op.get_bind()
    if not _seed_enabled():
        return

    customer_id = _get_user_id(conn, login=CUSTOMER_LOGIN)
    auditor_id = _get_user_id(conn, login=AUDITOR_LOGIN)

    org_rows = conn.execute(
        sa.text("SELECT id, name FROM organizations WHERE name = ANY(:names) ORDER BY id"),
        {"names": TARGET_ORG_NAMES},
    ).all()
    for oid, name in org_rows:
        _seed_more_for_org(conn, org_id=int(oid), org_name=str(name or ""), customer_id=customer_id, auditor_id=auditor_id)


def downgrade() -> None:
    # Seed-only migration: do not delete data on downgrade.
    pass

