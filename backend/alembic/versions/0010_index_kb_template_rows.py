"""index kb template rows (store sheet structure in DB)

Revision ID: 0010_index_kb_template_rows
Revises: 0009_file_previews
Create Date: 2026-01-28
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0010_index_kb_template_rows"
down_revision = "0009_file_previews"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "index_kb_template_rows",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("sheet_name", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("kind", sa.String(length=32), nullable=False, server_default="item"),
        sa.Column("row_key", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("title", sa.Text(), nullable=False, server_default=""),
        sa.Column("short_name", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("group_code", sa.String(length=255), nullable=False, server_default=""),
        sa.UniqueConstraint("sheet_name", "row_key", name="uq_index_kb_template_sheet_row"),
    )
    op.create_index("ix_index_kb_template_rows_sheet_name", "index_kb_template_rows", ["sheet_name"], unique=False)
    op.create_index(
        "ix_index_kb_template_rows_sheet_name_sort_order",
        "index_kb_template_rows",
        ["sheet_name", "sort_order"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_index_kb_template_rows_sheet_name_sort_order", table_name="index_kb_template_rows")
    op.drop_index("ix_index_kb_template_rows_sheet_name", table_name="index_kb_template_rows")
    op.drop_table("index_kb_template_rows")

