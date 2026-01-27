"""file previews cache (office -> pdf)

Revision ID: 0009_file_previews
Revises: 0008_org_artifacts_audit_fields
Create Date: 2026-01-27
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0009_file_previews"
down_revision = "0008_org_artifacts_audit_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "file_previews",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("file_version_id", sa.Integer(), nullable=False),
        sa.Column("preview_mime", sa.String(length=255), nullable=False, server_default="application/pdf"),
        sa.Column("preview_size_bytes", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("preview_sha256", sa.String(length=64), nullable=False, server_default=""),
        sa.Column("preview_blob", sa.LargeBinary(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("last_error", sa.Text(), nullable=False, server_default=""),
        sa.Column("last_error_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["file_version_id"], ["file_versions.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("file_version_id", name="uq_file_previews_file_version_id"),
    )
    op.create_index("ix_file_previews_file_version_id", "file_previews", ["file_version_id"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_file_previews_file_version_id", table_name="file_previews")
    op.drop_table("file_previews")

