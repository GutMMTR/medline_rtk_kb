"""Rename system organization Default -> РТК (demo/dev).

Why:
- In UI and demo data we want the system organization to be named "РТК" instead of "Default".

Safety / idempotency:
- If "Default" does not exist -> do nothing.
- If "РТК" already exists -> do not rename to avoid name collision.
- Also adjusts demo user full_name strings that contain "Default" (only if they match old defaults).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0029_rename_default_org_to_rtk"
down_revision = "0028_audit_period_feb1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    has_default = conn.execute(sa.text("SELECT 1 FROM organizations WHERE name='Default' LIMIT 1")).first()
    if has_default:
        has_rtk = conn.execute(sa.text("SELECT 1 FROM organizations WHERE name='РТК' LIMIT 1")).first()
        if not has_rtk:
            conn.execute(sa.text("UPDATE organizations SET name='РТК' WHERE name='Default'"))

    # Demo user names: keep it non-destructive (only if they match old defaults).
    conn.execute(
        sa.text(
            """
            UPDATE users
            SET full_name = 'РТК Аудитор'
            WHERE lower(login)='auditor' AND full_name='Default Auditor'
            """
        )
    )
    conn.execute(
        sa.text(
            """
            UPDATE users
            SET full_name = 'РТК Администратор'
            WHERE lower(login)='admin' AND full_name='Default Admin'
            """
        )
    )


def downgrade() -> None:
    conn = op.get_bind()

    # Best-effort rollback: rename back only if "Default" is free.
    has_rtk = conn.execute(sa.text("SELECT 1 FROM organizations WHERE name='РТК' LIMIT 1")).first()
    if has_rtk:
        has_default = conn.execute(sa.text("SELECT 1 FROM organizations WHERE name='Default' LIMIT 1")).first()
        if not has_default:
            conn.execute(sa.text("UPDATE organizations SET name='Default' WHERE name='РТК'"))

    conn.execute(
        sa.text(
            """
            UPDATE users
            SET full_name = 'Default Auditor'
            WHERE lower(login)='auditor' AND full_name='РТК Аудитор'
            """
        )
    )
    conn.execute(
        sa.text(
            """
            UPDATE users
            SET full_name = 'Default Admin'
            WHERE lower(login)='admin' AND full_name='РТК Администратор'
            """
        )
    )

