"""initial schema

Revision ID: 0001_initial
Revises: 
Create Date: 2026-01-21
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "organizations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("login", sa.String(length=255), nullable=False),
        sa.Column("password_hash", sa.String(length=255), nullable=False),
        sa.Column("full_name", sa.String(length=255), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("is_admin", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_users_login", "users", ["login"], unique=True)

    # Важно: для Postgres enum нужно использовать postgresql.ENUM(create_type=False),
    # иначе при create_table SQLAlchemy попытается создать тип повторно и упадёт.
    role_enum = postgresql.ENUM("customer", "auditor", "admin", name="role", create_type=False)
    role_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "user_org_memberships",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("role", role_enum, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("user_id", "org_id", name="uq_user_org"),
    )

    op.create_table(
        "stored_files",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("original_filename", sa.String(length=1024), nullable=False),
        sa.Column("content_type", sa.String(length=255), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("blob", sa.LargeBinary(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("note", sa.Text(), nullable=False),
    )
    op.create_index("ix_stored_files_org_id", "stored_files", ["org_id"], unique=False)
    op.create_index("ix_stored_files_sha256", "stored_files", ["sha256"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_stored_files_sha256", table_name="stored_files")
    op.drop_index("ix_stored_files_org_id", table_name="stored_files")
    op.drop_table("stored_files")

    op.drop_table("user_org_memberships")

    op.drop_index("ix_users_login", table_name="users")
    op.drop_table("users")

    op.drop_table("organizations")

    role_enum = sa.Enum(name="role")
    role_enum.drop(op.get_bind(), checkfirst=True)
