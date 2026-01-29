"""seed demo users (auditor + customer)

Revision ID: 0013_seed_demo_users
Revises: 0012_seed_artifacts
Create Date: 2026-01-29
"""

from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa
from alembic import op


revision = "0013_seed_demo_users"
down_revision = "0012_seed_artifacts"
branch_labels = None
depends_on = None


SYSTEM_ORG_NAME = "Default"
DEMO_ORG_NAME = "Демо организация"


def _utcnow() -> datetime:
    return datetime.utcnow()


def _ensure_org(conn: sa.Connection, *, name: str) -> int:
    org_id = conn.execute(sa.text("SELECT id FROM organizations WHERE name = :n"), {"n": name}).scalar()
    if org_id is not None:
        return int(org_id)

    # created_via/created_by_user_id columns exist by this migration point (0006+).
    org_id = conn.execute(
        sa.text(
            "INSERT INTO organizations (name, created_at, created_by_user_id, created_via) "
            "VALUES (:name, :created_at, NULL, 'system') "
            "RETURNING id"
        ),
        {"name": name, "created_at": _utcnow()},
    ).scalar_one()
    return int(org_id)


def _ensure_user(conn: sa.Connection, *, login: str, password: str, full_name: str, is_admin: bool) -> int:
    user_id = conn.execute(sa.text("SELECT id FROM users WHERE login = :l"), {"l": login}).scalar()
    if user_id is not None:
        # Do not overwrite password_hash; only keep safety flags consistent for demo users.
        conn.execute(
            sa.text("UPDATE users SET is_active = TRUE, is_admin = :is_admin WHERE id = :id"),
            {"id": int(user_id), "is_admin": bool(is_admin)},
        )
        return int(user_id)

    # Use the same hashing routine as the app.
    from app.auth.security import hash_password  # noqa: WPS433

    user_id = conn.execute(
        sa.text(
            "INSERT INTO users (login, password_hash, full_name, is_active, is_admin, created_at) "
            "VALUES (:login, :password_hash, :full_name, TRUE, :is_admin, :created_at) "
            "RETURNING id"
        ),
        {
            "login": login,
            "password_hash": hash_password(password),
            "full_name": full_name,
            "is_admin": bool(is_admin),
            "created_at": _utcnow(),
        },
    ).scalar_one()
    return int(user_id)


def _ensure_membership(conn: sa.Connection, *, user_id: int, org_id: int, role: str) -> None:
    exists = conn.execute(
        sa.text("SELECT 1 FROM user_org_memberships WHERE user_id = :u AND org_id = :o"),
        {"u": int(user_id), "o": int(org_id)},
    ).first()
    if exists:
        return
    conn.execute(
        sa.text(
            "INSERT INTO user_org_memberships (user_id, org_id, role, created_at) "
            "VALUES (:u, :o, :role, :created_at)"
        ),
        {"u": int(user_id), "o": int(org_id), "role": role, "created_at": _utcnow()},
    )


def upgrade() -> None:
    conn = op.get_bind()
    system_org_id = _ensure_org(conn, name=SYSTEM_ORG_NAME)
    demo_org_id = _ensure_org(conn, name=DEMO_ORG_NAME)

    auditor_id = _ensure_user(conn, login="auditor", password="auditor12345", full_name="Default Auditor", is_admin=False)
    customer_id = _ensure_user(conn, login="customer", password="customer12345", full_name="Demo Customer", is_admin=False)

    # Auditor is treated as "global" in MVP if has at least one auditor membership.
    _ensure_membership(conn, user_id=auditor_id, org_id=system_org_id, role="auditor")
    # Customer must have a non-Default org, because UI filters out Default.
    _ensure_membership(conn, user_id=customer_id, org_id=demo_org_id, role="customer")


def downgrade() -> None:
    # Safety: do not delete users on downgrade.
    pass

