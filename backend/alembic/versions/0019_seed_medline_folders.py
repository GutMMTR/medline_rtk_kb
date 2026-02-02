"""seed demo files for Medline folders (VSST/MNT/NS/REAG)

Goal:
- Ensure file explorer has realistic demo files (txt/jpg/png/pdf/svg) for Medline
  specifically for prefixes: ВССТ, МНТ, НС, РЕАГ.
- Keep migration idempotent (skip if already seeded).

Revision ID: 0019_seed_medline_folders
Revises: 0018_seed_kb_artifacts
Create Date: 2026-02-02
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta

import sqlalchemy as sa
from alembic import op


revision = "0019_seed_medline_folders"
down_revision = "0018_seed_kb_artifacts"
branch_labels = None
depends_on = None


SYSTEM_ORG_NAME = "Default"
AUDITOR_LOGIN = "auditor"
CUSTOMER_LOGIN = "medline"


def _utc(dt: datetime) -> datetime:
    return dt


def _sha256_hex(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def _get_demo_org_id(conn: sa.Connection) -> int | None:
    # Avoid fragile Cyrillic equality checks from outside environments.
    # In demo DB there are two orgs: "Default" and the real demo org (e.g. "Медлайн").
    oid = conn.execute(sa.text("SELECT id FROM organizations WHERE name <> :n ORDER BY id ASC LIMIT 1"), {"n": SYSTEM_ORG_NAME}).scalar()
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
    entity_id: int,
    after: dict,
) -> None:
    conn.execute(
        sa.text(
            """
            INSERT INTO audit_log (at, actor_user_id, org_id, action, entity_type, entity_id, before_json, after_json, ip, user_agent)
            VALUES (:at, :actor, :org, :action, 'org_artifact', :eid,
                    CAST(NULL AS jsonb), CAST(:after AS jsonb), '', '')
            """
        ),
        {
            "at": _utc(at),
            "actor": int(actor_user_id) if actor_user_id else None,
            "org": int(org_id) if org_id else None,
            "action": action,
            "eid": str(int(entity_id)),
            "after": json.dumps(after, ensure_ascii=False),
        },
    )


# Small binary samples to make downloads "real enough"
PNG_1X1 = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\x0cIDATx\x9cc\xf8\xff\xff?\x03\x00\x08\xfc\x02\xfe\xa7@\x9aS\x00\x00\x00\x00IEND\xaeB`\x82"
)

# Minimal valid JPEG (1x1) - small demo blob
JPG_1X1 = (
    b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x01\x00H\x00H\x00\x00"
    b"\xff\xdb\x00C\x00" + b"\x08" * 0x3B
    + b"\xff\xc0\x00\x11\x08\x00\x01\x00\x01\x03\x01\x11\x00\x02\x11\x01\x03\x11\x01"
    + b"\xff\xc4\x00\x14\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    + b"\xff\xc4\x00\x14\x10\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    + b"\xff\xda\x00\x0c\x03\x01\x00\x02\x11\x03\x11\x00?\x00"
    + b"\x00"
    + b"\xff\xd9"
)

PDF_MIN = b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\nxref\n0 2\n0000000000 65535 f \n0000000010 00000 n \ntrailer\n<< /Size 2 /Root 1 0 R >>\nstartxref\n20\n%%EOF\n"


def _svg(label: str) -> bytes:
    txt = (label or "DEMO").strip()[:12]
    svg = f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="420" height="220" viewBox="0 0 420 220">
  <rect width="420" height="220" rx="16" fill="#111827"/>
  <text x="210" y="120" text-anchor="middle" font-family="ui-sans-serif,Segoe UI,Arial" font-size="34" font-weight="800" fill="#6B2DFF">{txt}</text>
  <text x="210" y="160" text-anchor="middle" font-family="ui-monospace,Consolas,Menlo,monospace" font-size="14" fill="#E5E7EB">seed</text>
</svg>
"""
    return svg.encode("utf-8")


def upgrade() -> None:
    conn = op.get_bind()

    # Feature flag: same behavior as other demo seed migrations.
    if (os.getenv("SEED_DEMO_DATA") or "").strip() not in ("1", "true", "True", "yes", "on"):
        return

    org_id = _get_demo_org_id(conn)
    if not org_id:
        return

    customer_id = _get_user_id(conn, login=CUSTOMER_LOGIN)
    auditor_id = _get_user_id(conn, login=AUDITOR_LOGIN)

    # Select org_artifacts for target prefixes by checking the first segment of short_name.
    # Counts from catalog:
    # - ВССТ: 24, МНТ: 41, НС: 82, РЕАГ: 45
    targets = [("ВССТ", 24), ("МНТ", 41), ("НС", 82), ("РЕАГ", 45)]

    base = datetime(2026, 2, 11, 10, 0, 0)
    fmt_cycle = [
        ("txt", "text/plain"),
        ("jpg", "image/jpeg"),
        ("png", "image/png"),
        ("pdf", "application/pdf"),
        ("svg", "image/svg+xml"),
    ]

    for pref, lim in targets:
        rows = conn.execute(
            sa.text(
                """
                SELECT oa.id, a.short_name,
                       oa.current_file_version_id,
                       (SELECT fv.original_filename FROM file_versions fv WHERE fv.id=oa.current_file_version_id) AS cur_fn
                FROM org_artifacts oa
                JOIN artifacts a ON a.id=oa.artifact_id
                WHERE oa.org_id=:o
                  AND split_part(a.short_name,'.',1)=:p
                ORDER BY a.short_name, oa.id
                LIMIT :lim
                """
            ),
            {"o": int(org_id), "p": pref, "lim": int(lim)},
        ).all()

        for i, (oa_id, sn, _cur_fv, cur_fn) in enumerate(rows):
            sn = str(sn or "")
            cur_fn = str(cur_fn or "")

            # Idempotency: if already seeded for this prefix -> skip.
            if "_seed_" in cur_fn:
                continue

            ext, ct = fmt_cycle[(i % len(fmt_cycle))]
            # Keep variety in names for the explorer
            filename = f"{sn}_seed_{ext}.{ext}"
            if ext == "txt":
                blob = (f"DEMO seed\nprefix={pref}\nshort_name={sn}\n").encode("utf-8")
            elif ext == "jpg":
                blob = JPG_1X1
            elif ext == "png":
                blob = PNG_1X1
            elif ext == "pdf":
                blob = PDF_MIN
            else:
                blob = _svg(pref)

            up_at = base + timedelta(minutes=(targets.index((pref, lim)) * 300 + i))
            fv = _insert_fv(conn, oa_id=int(oa_id), filename=filename, content_type=ct, blob=blob, created_at=up_at, created_by=customer_id)

            # Status mix per prefix (deterministic):
            # - ~55% approved
            # - ~25% pending
            # - ~15% needs_correction
            # - ~5% changed (approved v1 -> upload v2 without audit)
            bucket = i % 20
            if bucket == 0:
                # changed: create v1 approved and then v2 current (pending) with audited pointing to v1
                aud_at = up_at + datetime.resolution
                _set_oa(
                    conn,
                    oa_id=int(oa_id),
                    cur_fv=int(fv),
                    review_status="approved",
                    updated_at=aud_at,
                    updated_by=auditor_id,
                    audited_fv=int(fv),
                    audited_at=aud_at,
                    audited_by=auditor_id,
                )
                _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org_id), action="upload", entity_id=int(oa_id), after={"current_file_version_id": fv, "review_status": "pending"})
                _audit_log(conn, at=aud_at, actor_user_id=auditor_id, org_id=int(org_id), action="audit", entity_id=int(oa_id), after={"audited_file_version_id": fv, "review_status": "approved"})

                # v2 (different format)
                ext2, ct2 = fmt_cycle[((i + 1) % len(fmt_cycle))]
                fn2 = f"{sn}_seed_v2_{ext2}.{ext2}"
                if ext2 == "txt":
                    blob2 = (f"DEMO seed v2\nprefix={pref}\nshort_name={sn}\n").encode("utf-8")
                elif ext2 == "jpg":
                    blob2 = JPG_1X1
                elif ext2 == "png":
                    blob2 = PNG_1X1
                elif ext2 == "pdf":
                    blob2 = PDF_MIN
                else:
                    blob2 = _svg(pref + " v2")
                up2 = up_at + timedelta(days=2)
                fv2 = _insert_fv(conn, oa_id=int(oa_id), filename=fn2, content_type=ct2, blob=blob2, created_at=up2, created_by=customer_id)
                # New version => audit reset but keep audited_file_version_id to show "Изменён"
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
                _audit_log(conn, at=up2, actor_user_id=customer_id, org_id=int(org_id), action="upload", entity_id=int(oa_id), after={"current_file_version_id": fv2, "review_status": "pending"})
                continue

            if bucket in (1, 2, 3):
                # needs correction
                _set_oa(conn, oa_id=int(oa_id), cur_fv=int(fv), review_status="needs_correction", updated_at=up_at, updated_by=auditor_id)
                _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org_id), action="upload", entity_id=int(oa_id), after={"current_file_version_id": fv, "review_status": "pending"})
                _audit_log(conn, at=up_at + datetime.resolution, actor_user_id=auditor_id, org_id=int(org_id), action="audit_needs_correction", entity_id=int(oa_id), after={"current_file_version_id": fv, "review_status": "needs_correction"})
                continue

            if bucket in (4, 5, 6, 7, 8):
                # pending (uploaded, no audit yet)
                _set_oa(conn, oa_id=int(oa_id), cur_fv=int(fv), review_status="pending", updated_at=up_at, updated_by=customer_id)
                _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org_id), action="upload", entity_id=int(oa_id), after={"current_file_version_id": fv, "review_status": "pending"})
                continue

            # approved
            aud_at = up_at + datetime.resolution
            _set_oa(
                conn,
                oa_id=int(oa_id),
                cur_fv=int(fv),
                review_status="approved",
                updated_at=aud_at,
                updated_by=auditor_id,
                audited_fv=int(fv),
                audited_at=aud_at,
                audited_by=auditor_id,
            )
            _audit_log(conn, at=up_at, actor_user_id=customer_id, org_id=int(org_id), action="upload", entity_id=int(oa_id), after={"current_file_version_id": fv, "review_status": "pending"})
            _audit_log(conn, at=aud_at, actor_user_id=auditor_id, org_id=int(org_id), action="audit", entity_id=int(oa_id), after={"audited_file_version_id": fv, "review_status": "approved"})


def downgrade() -> None:
    # Seed-only migration: do not delete data on downgrade.
    pass

