"""chat threads/messages for auditor<->customer (MVP polling)

Revision ID: 0022_chat_threads_messages
Revises: 0021_seed_medline2
Create Date: 2026-02-03
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0022_chat_threads_messages"
down_revision = "0021_seed_medline2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "chat_threads",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("org_artifact_id", sa.Integer(), sa.ForeignKey("org_artifacts.id", ondelete="CASCADE"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.UniqueConstraint("org_id", "org_artifact_id", name="uq_chat_threads_org_artifact"),
    )
    op.create_index("ix_chat_threads_org_id", "chat_threads", ["org_id"], unique=False)
    op.create_index("ix_chat_threads_org_artifact_id", "chat_threads", ["org_artifact_id"], unique=False)

    # One "org chat" per organization (where org_artifact_id IS NULL).
    op.create_index(
        "ux_chat_threads_org_only",
        "chat_threads",
        ["org_id"],
        unique=True,
        postgresql_where=sa.text("org_artifact_id IS NULL"),
    )

    op.create_table(
        "chat_messages",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("thread_id", sa.Integer(), sa.ForeignKey("chat_threads.id", ondelete="CASCADE"), nullable=False),
        sa.Column("author_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("body", sa.Text(), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    )
    op.create_index("ix_chat_messages_thread_id", "chat_messages", ["thread_id"], unique=False)
    op.create_index("ix_chat_messages_thread_created", "chat_messages", ["thread_id", "created_at"], unique=False)

    op.create_table(
        "chat_thread_reads",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("thread_id", sa.Integer(), sa.ForeignKey("chat_threads.id", ondelete="CASCADE"), nullable=False),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("last_read_message_id", sa.Integer(), sa.ForeignKey("chat_messages.id", ondelete="SET NULL"), nullable=True),
        sa.Column("last_read_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.UniqueConstraint("thread_id", "user_id", name="uq_chat_thread_reads_thread_user"),
    )
    op.create_index("ix_chat_thread_reads_user_id", "chat_thread_reads", ["user_id"], unique=False)
    op.create_index("ix_chat_thread_reads_thread_id", "chat_thread_reads", ["thread_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_chat_thread_reads_thread_id", table_name="chat_thread_reads")
    op.drop_index("ix_chat_thread_reads_user_id", table_name="chat_thread_reads")
    op.drop_table("chat_thread_reads")

    op.drop_index("ix_chat_messages_thread_created", table_name="chat_messages")
    op.drop_index("ix_chat_messages_thread_id", table_name="chat_messages")
    op.drop_table("chat_messages")

    op.drop_index("ux_chat_threads_org_only", table_name="chat_threads")
    op.drop_index("ix_chat_threads_org_artifact_id", table_name="chat_threads")
    op.drop_index("ix_chat_threads_org_id", table_name="chat_threads")
    op.drop_table("chat_threads")

