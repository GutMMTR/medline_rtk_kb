"""org created_by + created_via

Revision ID: 0006_org_created_by_and_via
Revises: 0005_nextcloud_integration
Create Date: 2026-01-26
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0006_org_created_by_and_via"
down_revision = "0005_nextcloud_integration"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("organizations", sa.Column("created_by_user_id", sa.Integer(), nullable=True))
    op.add_column(
        "organizations",
        sa.Column("created_via", sa.String(length=32), nullable=False, server_default="manual"),
    )
    op.create_foreign_key(
        "fk_organizations_created_by_user_id_users",
        "organizations",
        "users",
        ["created_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(op.f("ix_organizations_created_by_user_id"), "organizations", ["created_by_user_id"], unique=False)

    # Backfill:
    # - Default org created by bootstrap => system
    op.execute("UPDATE organizations SET created_via='system' WHERE name='Default'")
    # - Orgs that already have imported nextcloud files => nextcloud
    op.execute(
        """
        UPDATE organizations o
        SET created_via='nextcloud'
        WHERE EXISTS (
          SELECT 1
          FROM nextcloud_remote_file_state s
          WHERE s.org_id = o.id
        )
        """
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_organizations_created_by_user_id"), table_name="organizations")
    op.drop_constraint("fk_organizations_created_by_user_id_users", "organizations", type_="foreignkey")
    op.drop_column("organizations", "created_via")
    op.drop_column("organizations", "created_by_user_id")

