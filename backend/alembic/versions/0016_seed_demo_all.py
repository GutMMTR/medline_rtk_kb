"""seed demo (Medline org) by file dates (no audit periods)

IMPORTANT: This repository is intended to be поднят "с нуля" (fresh DB).
The demo seed is designed to be deterministic and idempotent for a fresh database.

Revision ID: 0016_seed_demo_all
Revises: 0014_periods_levels_review
Create Date: 2026-02-02
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta

import sqlalchemy as sa
from alembic import op


revision = "0016_seed_demo_all"
down_revision = "0014_periods_levels_review"
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


def _l1_id(conn: sa.Connection) -> int | None:
    x = conn.execute(sa.text("SELECT id FROM artifact_levels WHERE code='L1' ORDER BY id DESC LIMIT 1")).scalar()
    return int(x) if x is not None else None


def _level_id(conn: sa.Connection, *, code: str) -> int | None:
    x = conn.execute(
        sa.text("SELECT id FROM artifact_levels WHERE code=:c ORDER BY id DESC LIMIT 1"),
        {"c": (code or "").strip().upper()},
    ).scalar()
    return int(x) if x is not None else None


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


def _pick_l1_org_artifacts(conn: sa.Connection, *, org_id: int, limit: int) -> list[tuple[int, str]]:
    l1 = _l1_id(conn)
    if not l1:
        rows = conn.execute(
            sa.text(
                """
                SELECT oa.id, COALESCE(a.short_name,'') sn
                FROM org_artifacts oa JOIN artifacts a ON a.id=oa.artifact_id
                WHERE oa.org_id=:o
                ORDER BY a.short_name, oa.id
                LIMIT :lim
                """
            ),
            {"o": int(org_id), "lim": int(limit)},
        ).all()
        return [(int(r[0]), str(r[1] or "")) for r in rows]

    rows = conn.execute(
        sa.text(
            """
            SELECT oa.id, COALESCE(a.short_name,'') sn
            FROM org_artifacts oa
            JOIN artifacts a ON a.id=oa.artifact_id
            JOIN artifact_level_items ali ON ali.artifact_id=a.id
            WHERE oa.org_id=:o AND ali.level_id=:l1
            ORDER BY a.short_name, oa.id
            LIMIT :lim
            """
        ),
        {"o": int(org_id), "l1": int(l1), "lim": int(limit)},
    ).all()
    return [(int(r[0]), str(r[1] or "")) for r in rows]


def _next_version_no(conn: sa.Connection, *, org_artifact_id: int) -> int:
    v = conn.execute(sa.text("SELECT COALESCE(MAX(version_no),0) FROM file_versions WHERE org_artifact_id=:oa"), {"oa": int(org_artifact_id)}).scalar()
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


def _add_comment(conn: sa.Connection, *, org_id: int, oa_id: int, author_id: int | None, text: str, at: datetime) -> None:
    conn.execute(
        sa.text(
            """
            INSERT INTO org_artifact_comments (org_id, org_artifact_id, author_user_id, comment_text, created_at)
            VALUES (:o,:oa,:u,:t,:at)
            """
        ),
        {"o": int(org_id), "oa": int(oa_id), "u": int(author_id) if author_id else None, "t": text, "at": _utc(at)},
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


def _fv_count(conn: sa.Connection, *, oa_id: int) -> int:
    x = conn.execute(sa.text("SELECT count(*) FROM file_versions WHERE org_artifact_id=:oa"), {"oa": int(oa_id)}).scalar()
    return int(x or 0)


def _upload_new_version_reset_audit(
    conn: sa.Connection,
    *,
    org_id: int,
    oa_id: int,
    sn: str,
    at: datetime,
    customer_id: int | None,
    kind: str,
) -> int:
    """
    Загружает новую версию и сбрасывает аудитные поля (как обычный upload в UI).
    Возвращает id новой file_version.
    """
    blob = (f"DEMO {kind}\nshort_name={sn}\n").encode("utf-8")
    fv2 = _insert_fv(
        conn,
        oa_id=int(oa_id),
        filename=f"{sn or 'artifact'}_{kind}_v2.txt",
        content_type="text/plain",
        blob=blob,
        created_at=at,
        created_by=customer_id,
    )
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

    # New version => audit reset
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
        {"id": int(oa_id), "cur": int(fv2), "at": _utc(at), "by": int(customer_id) if customer_id else None},
    )
    _audit_log(
        conn,
        at=at,
        actor_user_id=customer_id,
        org_id=org_id,
        action="upload",
        entity_type="org_artifact",
        entity_id=str(int(oa_id)),
        before=before or None,
        after={
            "status": "uploaded",
            "current_file_version_id": fv2,
            "audited_file_version_id": None,
            "audited_at": None,
            "audited_by_user_id": None,
            "review_status": "pending",
        },
    )
    return int(fv2)


def _svg(label: str) -> bytes:
    txt = (label or "ML").strip()[:3].upper()
    svg = f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="512" height="320" viewBox="0 0 512 320">
  <rect width="512" height="320" rx="28" fill="#111827"/>
  <rect x="18" y="18" width="476" height="284" rx="20" fill="#0B1220"/>
  <text x="256" y="150" text-anchor="middle" font-family="ui-sans-serif,Segoe UI,Arial" font-size="64" font-weight="800" fill="#6B2DFF">{txt}</text>
  <text x="256" y="210" text-anchor="middle" font-family="ui-monospace,Consolas,Menlo,monospace" font-size="22" fill="#E5E7EB">DEMO</text>
</svg>
"""
    return svg.encode("utf-8")


PNG_1X1 = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\x0cIDATx\x9cc\xf8\xff\xff?\x03\x00\x08\xfc\x02\xfe\xa7@\x9aS\x00\x00\x00\x00IEND\xaeB`\x82"
)


def upgrade() -> None:
    conn = op.get_bind()

    org_id = _get_org_id(conn)
    if not org_id:
        return

    # org defaults
    # Для демо ставим L3, чтобы было что считать для КБ1/КБ2/КБ3.
    l3 = _level_id(conn, code="L3")
    if l3:
        conn.execute(sa.text("UPDATE organizations SET artifact_level_id=:l3 WHERE id=:o"), {"l3": int(l3), "o": int(org_id)})

    # Always ensure org_artifacts exist (file explorer expects structure from artifacts).
    _materialize_org_artifacts(conn, org_id=org_id, now=datetime(2026, 2, 2, 11, 0, 0))

    # Demo data toggle (only affects files/statuses, not periods).
    if (os.getenv("SEED_DEMO_DATA") or "").strip() not in ("1", "true", "True", "yes", "on"):
        return

    # Idempotency: if we already have enough uploads, do nothing.
    uploaded_cnt = conn.execute(sa.text("SELECT count(*) FROM org_artifacts WHERE org_id=:o AND status='uploaded'"), {"o": int(org_id)}).scalar()
    if int(uploaded_cnt or 0) >= 150:
        return

    customer_id = _get_user_id(conn, login=CUSTOMER_LOGIN)
    auditor_id = _get_user_id(conn, login=AUDITOR_LOGIN)

    rows = _pick_l1_org_artifacts(conn, org_id=org_id, limit=179)
    # seed only those without any file versions
    candidates: list[tuple[int, str]] = []
    for oa_id, sn in rows:
        has = conn.execute(sa.text("SELECT 1 FROM file_versions WHERE org_artifact_id=:oa LIMIT 1"), {"oa": int(oa_id)}).first()
        if not has:
            candidates.append((oa_id, sn))

    need = 150 - int(uploaded_cnt or 0)
    candidates = candidates[: max(0, need)]

    n = len(candidates)
    if n <= 0:
        return

    # Небольшая доля должна попадать в "последний месяц" (Февраль 2026),
    # остальное — в Q4 2025 (чтобы демо не смешивалось).
    keep_feb = min(20, n)
    feb_set = {oa_id for (oa_id, _) in candidates[-keep_feb:]} if keep_feb else set()

    # status mix
    n_pending = max(1, int(round(n * 0.40)))
    n_approved = max(1, int(round(n * 0.35)))
    n_needs = max(1, int(round(n * 0.15)))
    pending = candidates[:n_pending]
    approved = candidates[n_pending : n_pending + n_approved]
    needs = candidates[n_pending + n_approved : n_pending + n_approved + n_needs]
    changed = candidates[n_pending + n_approved + n_needs :]

    bulk_at = datetime(2025, 11, 22, 10, 0, 0)  # Q4 2025
    aud_at = datetime(2025, 12, 6, 12, 0, 0)
    corr_at = datetime(2025, 12, 12, 15, 0, 0)
    feb_upload_at = datetime(2026, 2, 10, 10, 0, 0)
    feb_aud_at = datetime(2026, 2, 15, 12, 0, 0)
    feb_corr_at = datetime(2026, 2, 18, 15, 0, 0)

    # pending
    for idx, (oa_id, sn) in enumerate(pending, start=1):
        up_at = feb_upload_at if oa_id in feb_set else bulk_at
        if idx % 3 == 0:
            blob = (f"DEMO pending\nshort_name={sn}\n").encode("utf-8")
            ct = "text/plain"
            fn = f"{sn or 'artifact'}_pending_{idx}.txt"
        else:
            blob = PNG_1X1
            ct = "image/png"
            fn = f"{sn or 'artifact'}_pending_{idx}.png"
        fv = _insert_fv(conn, oa_id=oa_id, filename=fn, content_type=ct, blob=blob, created_at=up_at, created_by=customer_id)
        before = {
            "status": "missing",
            "current_file_version_id": None,
            "audited_file_version_id": None,
            "audited_at": None,
            "audited_by_user_id": None,
            "review_status": "pending",
        }
        _set_oa(conn, oa_id=oa_id, cur_fv=fv, review_status="pending", updated_at=up_at, updated_by=customer_id)
        after = {
            "status": "uploaded",
            "current_file_version_id": fv,
            "audited_file_version_id": None,
            "audited_at": None,
            "audited_by_user_id": None,
            "review_status": "pending",
        }
        _audit_log(
            conn,
            at=up_at,
            actor_user_id=customer_id,
            org_id=org_id,
            action="upload",
            entity_type="org_artifact",
            entity_id=str(oa_id),
            before=before,
            after=after,
        )

    # approved
    for idx, (oa_id, sn) in enumerate(approved, start=1):
        up_at = feb_upload_at if oa_id in feb_set else bulk_at
        a_at = feb_aud_at if oa_id in feb_set else aud_at
        if idx % 2 == 0:
            blob = _svg("OK")
            ct = "image/svg+xml"
            fn = f"{sn or 'artifact'}_approved_{idx}.svg"
        else:
            blob = (f"DEMO approved\nshort_name={sn}\n").encode("utf-8")
            ct = "text/plain"
            fn = f"{sn or 'artifact'}_approved_{idx}.txt"
        fv = _insert_fv(conn, oa_id=oa_id, filename=fn, content_type=ct, blob=blob, created_at=up_at, created_by=customer_id)
        # upload (customer)
        _set_oa(conn, oa_id=oa_id, cur_fv=fv, review_status="pending", updated_at=up_at, updated_by=customer_id)
        _audit_log(
            conn,
            at=up_at,
            actor_user_id=customer_id,
            org_id=org_id,
            action="upload",
            entity_type="org_artifact",
            entity_id=str(oa_id),
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
                "current_file_version_id": fv,
                "audited_file_version_id": None,
                "audited_at": None,
                "audited_by_user_id": None,
                "review_status": "pending",
            },
        )
        # audit (auditor)
        _set_oa(conn, oa_id=oa_id, cur_fv=fv, review_status="approved", updated_at=a_at, updated_by=auditor_id, audited_fv=fv, audited_at=a_at, audited_by=auditor_id)
        _audit_log(
            conn,
            at=a_at,
            actor_user_id=auditor_id,
            org_id=org_id,
            action="audit",
            entity_type="org_artifact",
            entity_id=str(oa_id),
            before={
                "audited_file_version_id": None,
                "audited_at": None,
                "audited_by_user_id": None,
                "current_file_version_id": fv,
                "review_status": "pending",
            },
            after={
                "audited_file_version_id": fv,
                "audited_at": a_at.isoformat(),
                "audited_by_user_id": auditor_id,
                "current_file_version_id": fv,
                "review_status": "approved",
            },
        )

    # needs correction
    for idx, (oa_id, sn) in enumerate(needs, start=1):
        up_at = feb_upload_at if oa_id in feb_set else bulk_at
        c_at = feb_corr_at if oa_id in feb_set else corr_at
        blob = (f"DEMO needs_correction\nshort_name={sn}\n").encode("utf-8")
        fv = _insert_fv(conn, oa_id=oa_id, filename=f"{sn or 'artifact'}_needs_{idx}.txt", content_type="text/plain", blob=blob, created_at=up_at, created_by=customer_id)
        # upload (customer)
        _set_oa(conn, oa_id=oa_id, cur_fv=fv, review_status="pending", updated_at=up_at, updated_by=customer_id)
        _audit_log(
            conn,
            at=up_at,
            actor_user_id=customer_id,
            org_id=org_id,
            action="upload",
            entity_type="org_artifact",
            entity_id=str(oa_id),
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
                "current_file_version_id": fv,
                "audited_file_version_id": None,
                "audited_at": None,
                "audited_by_user_id": None,
                "review_status": "pending",
            },
        )
        # auditor returns to correction + comment
        _add_comment(conn, org_id=org_id, oa_id=oa_id, author_id=auditor_id, text="Нужна корректировка: обновите документ и приложите подтверждение.", at=c_at)
        _audit_log(
            conn,
            at=c_at,
            actor_user_id=auditor_id,
            org_id=org_id,
            action="comment",
            entity_type="org_artifact",
            entity_id=str(oa_id),
            before=None,
            after={"comment": "Нужна корректировка: обновите документ и приложите подтверждение."},
        )
        _set_oa(conn, oa_id=oa_id, cur_fv=fv, review_status="needs_correction", updated_at=c_at, updated_by=auditor_id)
        _audit_log(
            conn,
            at=c_at,
            actor_user_id=auditor_id,
            org_id=org_id,
            action="audit_needs_correction",
            entity_type="org_artifact",
            entity_id=str(oa_id),
            before={
                "review_status": "pending",
                "audited_file_version_id": None,
                "audited_at": None,
                "audited_by_user_id": None,
                "current_file_version_id": fv,
            },
            after={
                "review_status": "needs_correction",
                "audited_file_version_id": None,
                "audited_at": None,
                "audited_by_user_id": None,
                "current_file_version_id": fv,
                "comment": "Нужна корректировка: обновите документ и приложите подтверждение.",
            },
        )

    # changed (2 versions)
    for idx, (oa_id, sn) in enumerate(changed, start=1):
        up1_at = feb_upload_at if oa_id in feb_set else bulk_at
        a_at = feb_aud_at if oa_id in feb_set else aud_at
        up2_at = datetime(2026, 2, 20, 10, 0, 0) if oa_id in feb_set else datetime(2025, 12, 20, 10, 0, 0)

        fv1 = _insert_fv(
            conn,
            oa_id=oa_id,
            filename=f"{sn or 'artifact'}_changed_{idx}_v1.txt",
            content_type="text/plain",
            blob=(f"DEMO changed v1\nshort_name={sn}\n").encode("utf-8"),
            created_at=up1_at,
            created_by=customer_id,
        )
        _set_oa(conn, oa_id=oa_id, cur_fv=fv1, review_status="pending", updated_at=up1_at, updated_by=customer_id)
        _audit_log(
            conn,
            at=up1_at,
            actor_user_id=customer_id,
            org_id=org_id,
            action="upload",
            entity_type="org_artifact",
            entity_id=str(oa_id),
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
                "current_file_version_id": fv1,
                "audited_file_version_id": None,
                "audited_at": None,
                "audited_by_user_id": None,
                "review_status": "pending",
            },
        )
        _set_oa(conn, oa_id=oa_id, cur_fv=fv1, review_status="approved", updated_at=a_at, updated_by=auditor_id, audited_fv=fv1, audited_at=a_at, audited_by=auditor_id)
        _audit_log(
            conn,
            at=a_at,
            actor_user_id=auditor_id,
            org_id=org_id,
            action="audit",
            entity_type="org_artifact",
            entity_id=str(oa_id),
            before={
                "audited_file_version_id": None,
                "audited_at": None,
                "audited_by_user_id": None,
                "current_file_version_id": fv1,
                "review_status": "pending",
            },
            after={
                "audited_file_version_id": fv1,
                "audited_at": a_at.isoformat(),
                "audited_by_user_id": auditor_id,
                "current_file_version_id": fv1,
                "review_status": "approved",
            },
        )

        fv2 = _insert_fv(
            conn,
            oa_id=oa_id,
            filename=f"{sn or 'artifact'}_changed_{idx}_v2.svg",
            content_type="image/svg+xml",
            blob=_svg("CHG"),
            created_at=up2_at,
            created_by=customer_id,
        )
        # New version => audit reset (как в /my/artifacts/{id}/upload)
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
            {"id": int(oa_id), "cur": int(fv2), "at": _utc(up2_at), "by": int(customer_id) if customer_id else None},
        )
        _audit_log(
            conn,
            at=up2_at,
            actor_user_id=customer_id,
            org_id=org_id,
            action="upload",
            entity_type="org_artifact",
            entity_id=str(oa_id),
            before={
                "status": "uploaded",
                "current_file_version_id": fv1,
                "audited_file_version_id": fv1,
                "audited_at": a_at.isoformat(),
                "audited_by_user_id": auditor_id,
                "review_status": "approved",
            },
            after={
                "status": "uploaded",
                "current_file_version_id": fv2,
                "audited_file_version_id": None,
                "audited_at": None,
                "audited_by_user_id": None,
                "review_status": "pending",
            },
        )

    # Ещё немного истории версий, чтобы в UI было наглядно по статусам:
    # - pending: v1 -> v2 (остаётся pending)
    # - approved: v1 approved -> v2 (сброс аудита => pending)
    # - needs_correction: после возврата заказчик загружает v2 (pending)
    for i, (oa_id, sn) in enumerate(pending[:12], start=1):
        if _fv_count(conn, oa_id=int(oa_id)) >= 2:
            continue
        at2 = (feb_upload_at if oa_id in feb_set else bulk_at) + timedelta(days=3)
        _upload_new_version_reset_audit(conn, org_id=org_id, oa_id=int(oa_id), sn=sn, at=at2, customer_id=customer_id, kind="pending")

    for i, (oa_id, sn) in enumerate(approved[:12], start=1):
        if _fv_count(conn, oa_id=int(oa_id)) >= 2:
            continue
        at2 = (feb_aud_at if oa_id in feb_set else aud_at) + timedelta(days=10)
        _upload_new_version_reset_audit(conn, org_id=org_id, oa_id=int(oa_id), sn=sn, at=at2, customer_id=customer_id, kind="after_approved")

    for i, (oa_id, sn) in enumerate(needs[:6], start=1):
        if _fv_count(conn, oa_id=int(oa_id)) >= 2:
            continue
        at2 = (feb_corr_at if oa_id in feb_set else corr_at) + timedelta(days=4)
        _upload_new_version_reset_audit(conn, org_id=org_id, oa_id=int(oa_id), sn=sn, at=at2, customer_id=customer_id, kind="after_correction")

    # Дополнительно: чтобы в УИБ/СЗИ был заметный расчёт,
    # делаем batch "upload+audit" для части строк из шаблонов Индекса КБ.
    # Это создаёт больше file_versions и даёт ненулевые значения в таблицах.
    uib_names = conn.execute(
        sa.text(
            """
            SELECT t.short_name, MIN(t.id) AS min_id
            FROM index_kb_template_rows t
            JOIN artifacts a ON upper(a.short_name)=upper(t.short_name)
            WHERE t.sheet_name='Управление ИБ' AND t.kind='item' AND t.short_name <> ''
            GROUP BY t.short_name
            ORDER BY min_id
            LIMIT 25
            """
        )
    ).all()
    for (sn2, _min_id) in uib_names:
        if sn2:
            _ensure_approved_for_short_name(
                conn,
                org_id=org_id,
                short_name=str(sn2),
                upload_at=datetime(2025, 11, 25, 10, 0, 0),
                customer_id=customer_id,
                auditor_id=auditor_id,
            )

    szi_names = conn.execute(
        sa.text(
            """
            SELECT t.short_name, MIN(t.id) AS min_id
            FROM index_kb_template_rows t
            JOIN artifacts a ON upper(a.short_name)=upper(t.short_name)
            WHERE t.sheet_name='СЗИ' AND t.kind='item' AND t.short_name <> ''
            GROUP BY t.short_name
            ORDER BY min_id
            LIMIT 25
            """
        )
    ).all()
    for (sn2, _min_id) in szi_names:
        if sn2:
            _ensure_approved_for_short_name(
                conn,
                org_id=org_id,
                short_name=str(sn2),
                upload_at=datetime(2025, 12, 2, 10, 0, 0),
                customer_id=customer_id,
                auditor_id=auditor_id,
            )


def downgrade() -> None:
    pass

