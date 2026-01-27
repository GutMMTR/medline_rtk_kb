"""org artifacts audit fields

Revision ID: 0008_org_artifacts_audit_fields
Revises: 0007_index_kb_manual_values
Create Date: 2026-01-27
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0008_org_artifacts_audit_fields"
down_revision = "0007_index_kb_manual_values"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("org_artifacts", sa.Column("audited_file_version_id", sa.Integer(), nullable=True))
    op.add_column("org_artifacts", sa.Column("audited_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("org_artifacts", sa.Column("audited_by_user_id", sa.Integer(), nullable=True))

    op.create_foreign_key(
        "fk_org_artifacts_audited_file_version_id",
        "org_artifacts",
        "file_versions",
        ["audited_file_version_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_org_artifacts_audited_by_user_id",
        "org_artifacts",
        "users",
        ["audited_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.create_index("ix_org_artifacts_audited_file_version_id", "org_artifacts", ["audited_file_version_id"], unique=False)
    op.create_index("ix_org_artifacts_audited_by_user_id", "org_artifacts", ["audited_by_user_id"], unique=False)
    op.create_index("ix_org_artifacts_audited_at", "org_artifacts", ["audited_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_org_artifacts_audited_at", table_name="org_artifacts")
    op.drop_index("ix_org_artifacts_audited_by_user_id", table_name="org_artifacts")
    op.drop_index("ix_org_artifacts_audited_file_version_id", table_name="org_artifacts")

    op.drop_constraint("fk_org_artifacts_audited_by_user_id", "org_artifacts", type_="foreignkey")
    op.drop_constraint("fk_org_artifacts_audited_file_version_id", "org_artifacts", type_="foreignkey")

    op.drop_column("org_artifacts", "audited_by_user_id")
    op.drop_column("org_artifacts", "audited_at")
    op.drop_column("org_artifacts", "audited_file_version_id")

