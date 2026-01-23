"""org artifact comments

Revision ID: 0004_org_artifact_comments
Revises: 0003_artifacts_program_fields
Create Date: 2026-01-22
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0004_org_artifact_comments"
down_revision = "0003_artifacts_program_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "org_artifact_comments",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("org_artifact_id", sa.Integer(), sa.ForeignKey("org_artifacts.id", ondelete="CASCADE"), nullable=False),
        sa.Column("author_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("comment_text", sa.Text(), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_org_artifact_comments_org_id", "org_artifact_comments", ["org_id"])
    op.create_index("ix_org_artifact_comments_org_artifact_id", "org_artifact_comments", ["org_artifact_id"])
    op.create_index("ix_org_artifact_comments_author_user_id", "org_artifact_comments", ["author_user_id"])
    op.create_index("ix_org_artifact_comments_created_at", "org_artifact_comments", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_org_artifact_comments_created_at", table_name="org_artifact_comments")
    op.drop_index("ix_org_artifact_comments_author_user_id", table_name="org_artifact_comments")
    op.drop_index("ix_org_artifact_comments_org_artifact_id", table_name="org_artifact_comments")
    op.drop_index("ix_org_artifact_comments_org_id", table_name="org_artifact_comments")
    op.drop_table("org_artifact_comments")

