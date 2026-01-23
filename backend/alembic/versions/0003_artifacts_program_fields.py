"""add artifacts program fields

Revision ID: 0003_artifacts_program_fields
Revises: 0002_artifacts_and_audit
Create Date: 2026-01-21
"""

from alembic import op
import sqlalchemy as sa


revision = "0003_artifacts_program_fields"
down_revision = "0002_artifacts_and_audit"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("artifacts", sa.Column("topic", sa.String(length=255), nullable=False, server_default=""))
    op.add_column("artifacts", sa.Column("domain", sa.String(length=255), nullable=False, server_default=""))
    op.add_column("artifacts", sa.Column("indicator_name", sa.Text(), nullable=False, server_default=""))
    op.add_column("artifacts", sa.Column("short_name", sa.String(length=255), nullable=False, server_default=""))
    op.add_column("artifacts", sa.Column("kb_level", sa.String(length=64), nullable=False, server_default=""))
    op.add_column("artifacts", sa.Column("achievement_text", sa.Text(), nullable=False, server_default=""))
    op.add_column("artifacts", sa.Column("achievement_item_no", sa.Integer(), nullable=True))
    op.add_column("artifacts", sa.Column("achievement_item_text", sa.Text(), nullable=False, server_default=""))

    op.create_index("ix_artifacts_topic", "artifacts", ["topic"], unique=False)
    op.create_index("ix_artifacts_domain", "artifacts", ["domain"], unique=False)
    op.create_index("ix_artifacts_short_name", "artifacts", ["short_name"], unique=False)
    op.create_index("ix_artifacts_kb_level", "artifacts", ["kb_level"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_artifacts_kb_level", table_name="artifacts")
    op.drop_index("ix_artifacts_short_name", table_name="artifacts")
    op.drop_index("ix_artifacts_domain", table_name="artifacts")
    op.drop_index("ix_artifacts_topic", table_name="artifacts")

    op.drop_column("artifacts", "achievement_item_text")
    op.drop_column("artifacts", "achievement_item_no")
    op.drop_column("artifacts", "achievement_text")
    op.drop_column("artifacts", "kb_level")
    op.drop_column("artifacts", "short_name")
    op.drop_column("artifacts", "indicator_name")
    op.drop_column("artifacts", "domain")
    op.drop_column("artifacts", "topic")

