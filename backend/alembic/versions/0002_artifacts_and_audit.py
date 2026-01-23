"""artifacts + file versions + audit log

Revision ID: 0002_artifacts_and_audit
Revises: 0001_initial
Create Date: 2026-01-21
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0002_artifacts_and_audit"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Важно: используем postgresql.ENUM(create_type=False), иначе create_table
    # может попытаться создать тип повторно и упасть на DuplicateObject.
    org_artifact_status = postgresql.ENUM("missing", "uploaded", name="org_artifact_status", create_type=False)
    org_artifact_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "artifact_nodes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("parent_id", sa.Integer(), sa.ForeignKey("artifact_nodes.id", ondelete="CASCADE"), nullable=True),
        sa.Column("segment", sa.String(length=255), nullable=False),
        sa.Column("full_path", sa.String(length=2048), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_artifact_nodes_parent_id", "artifact_nodes", ["parent_id"], unique=False)
    op.create_index("ix_artifact_nodes_full_path", "artifact_nodes", ["full_path"], unique=True)

    op.create_table(
        "artifacts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("node_id", sa.Integer(), sa.ForeignKey("artifact_nodes.id", ondelete="CASCADE"), nullable=False),
        sa.Column("artifact_key", sa.String(length=255), nullable=True),
        sa.Column("title", sa.String(length=1024), nullable=False, server_default=""),
        sa.Column("description", sa.Text(), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_artifacts_node_id", "artifacts", ["node_id"], unique=True)
    op.create_index("ix_artifacts_artifact_key", "artifacts", ["artifact_key"], unique=True)

    op.create_table(
        "org_artifacts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("artifact_id", sa.Integer(), sa.ForeignKey("artifacts.id", ondelete="CASCADE"), nullable=False),
        sa.Column("status", org_artifact_status, nullable=False, server_default="missing"),
        sa.Column("current_file_version_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.UniqueConstraint("org_id", "artifact_id", name="uq_org_artifact"),
    )
    op.create_index("ix_org_artifacts_org_id", "org_artifacts", ["org_id"], unique=False)
    op.create_index("ix_org_artifacts_artifact_id", "org_artifacts", ["artifact_id"], unique=False)

    op.create_table(
        "file_versions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_artifact_id", sa.Integer(), sa.ForeignKey("org_artifacts.id", ondelete="CASCADE"), nullable=False),
        sa.Column("version_no", sa.Integer(), nullable=False),
        sa.Column("original_filename", sa.String(length=1024), nullable=False),
        sa.Column("content_type", sa.String(length=255), nullable=False, server_default="application/octet-stream"),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("storage_backend", sa.String(length=64), nullable=False, server_default="postgres"),
        sa.Column("storage_key", sa.String(length=1024), nullable=True),
        sa.Column("blob", sa.LargeBinary(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.UniqueConstraint("org_artifact_id", "version_no", name="uq_org_artifact_version"),
    )
    op.create_index("ix_file_versions_org_artifact_id", "file_versions", ["org_artifact_id"], unique=False)
    op.create_index("ix_file_versions_sha256", "file_versions", ["sha256"], unique=False)

    op.create_foreign_key(
        "fk_org_artifacts_current_file_version_id",
        "org_artifacts",
        "file_versions",
        ["current_file_version_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.create_table(
        "audit_log",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("actor_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id", ondelete="SET NULL"), nullable=True),
        sa.Column("action", sa.String(length=255), nullable=False),
        sa.Column("entity_type", sa.String(length=255), nullable=False),
        sa.Column("entity_id", sa.String(length=255), nullable=False),
        sa.Column("before_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("after_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("ip", sa.String(length=64), nullable=False, server_default=""),
        sa.Column("user_agent", sa.String(length=1024), nullable=False, server_default=""),
    )
    op.create_index("ix_audit_log_at", "audit_log", ["at"], unique=False)
    op.create_index("ix_audit_log_actor_user_id", "audit_log", ["actor_user_id"], unique=False)
    op.create_index("ix_audit_log_org_id", "audit_log", ["org_id"], unique=False)
    op.create_index("ix_audit_log_action", "audit_log", ["action"], unique=False)
    op.create_index("ix_audit_log_entity_type", "audit_log", ["entity_type"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_audit_log_entity_type", table_name="audit_log")
    op.drop_index("ix_audit_log_action", table_name="audit_log")
    op.drop_index("ix_audit_log_org_id", table_name="audit_log")
    op.drop_index("ix_audit_log_actor_user_id", table_name="audit_log")
    op.drop_index("ix_audit_log_at", table_name="audit_log")
    op.drop_table("audit_log")

    op.drop_constraint("fk_org_artifacts_current_file_version_id", "org_artifacts", type_="foreignkey")

    op.drop_index("ix_file_versions_sha256", table_name="file_versions")
    op.drop_index("ix_file_versions_org_artifact_id", table_name="file_versions")
    op.drop_table("file_versions")

    op.drop_index("ix_org_artifacts_artifact_id", table_name="org_artifacts")
    op.drop_index("ix_org_artifacts_org_id", table_name="org_artifacts")
    op.drop_table("org_artifacts")

    op.drop_index("ix_artifacts_artifact_key", table_name="artifacts")
    op.drop_index("ix_artifacts_node_id", table_name="artifacts")
    op.drop_table("artifacts")

    op.drop_index("ix_artifact_nodes_full_path", table_name="artifact_nodes")
    op.drop_index("ix_artifact_nodes_parent_id", table_name="artifact_nodes")
    op.drop_table("artifact_nodes")

    org_artifact_status = sa.Enum(name="org_artifact_status")
    org_artifact_status.drop(op.get_bind(), checkfirst=True)

