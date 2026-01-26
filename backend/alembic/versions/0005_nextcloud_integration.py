"""nextcloud integration

Revision ID: 0005_nextcloud_integration
Revises: 0004_org_artifact_comments
Create Date: 2026-01-23
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0005_nextcloud_integration"
down_revision = "0004_org_artifact_comments"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "nextcloud_integration_settings",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("base_url", sa.String(length=1024), nullable=False, server_default=""),
        sa.Column("username", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("password", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("root_folder", sa.String(length=1024), nullable=False, server_default=""),
        sa.Column("create_orgs", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("last_sync_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=False, server_default=""),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "nextcloud_remote_file_state",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("org_artifact_id", sa.Integer(), nullable=False),
        sa.Column("remote_path", sa.String(length=2048), nullable=False),
        sa.Column("etag", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("size_bytes", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("imported_file_version_id", sa.Integer(), nullable=True),
        sa.Column("imported_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["imported_file_version_id"], ["file_versions.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["org_artifact_id"], ["org_artifacts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "remote_path", name="uq_nextcloud_org_remote_path"),
    )
    op.create_index(op.f("ix_nextcloud_remote_file_state_org_artifact_id"), "nextcloud_remote_file_state", ["org_artifact_id"], unique=False)
    op.create_index(op.f("ix_nextcloud_remote_file_state_org_id"), "nextcloud_remote_file_state", ["org_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_nextcloud_remote_file_state_org_id"), table_name="nextcloud_remote_file_state")
    op.drop_index(op.f("ix_nextcloud_remote_file_state_org_artifact_id"), table_name="nextcloud_remote_file_state")
    op.drop_table("nextcloud_remote_file_state")
    op.drop_table("nextcloud_integration_settings")

