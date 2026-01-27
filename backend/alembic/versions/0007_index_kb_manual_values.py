"""index kb manual values

Revision ID: 0007_index_kb_manual_values
Revises: 0006_org_created_by_and_via
Create Date: 2026-01-26
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0007_index_kb_manual_values"
down_revision = "0006_org_created_by_and_via"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "index_kb_manual_values",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("sheet_name", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("row_key", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("value", sa.Float(), nullable=False, server_default=sa.text("0")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["updated_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "sheet_name", "row_key", name="uq_index_kb_manual_org_sheet_row"),
    )
    op.create_index(op.f("ix_index_kb_manual_values_org_id"), "index_kb_manual_values", ["org_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_index_kb_manual_values_org_id"), table_name="index_kb_manual_values")
    op.drop_table("index_kb_manual_values")

