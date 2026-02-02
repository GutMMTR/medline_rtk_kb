"""artifact levels + review status (no audit periods)

Revision ID: 0014_periods_levels_review
Revises: 0013_seed_demo_users
Create Date: 2026-01-30
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = "0014_periods_levels_review"
down_revision = "0013_seed_demo_users"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- org_artifact review status enum ---
    org_artifact_review_status = postgresql.ENUM(
        "pending",
        "approved",
        "needs_correction",
        name="org_artifact_review_status",
        create_type=False,
    )
    org_artifact_review_status.create(op.get_bind(), checkfirst=True)

    # --- artifact levels ---
    op.create_table(
        "artifact_levels",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("color", sa.String(length=32), nullable=False, server_default="#64748b"),  # slate-500
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("TRUE")),
        sa.UniqueConstraint("code", name="uq_artifact_levels_code"),
    )
    op.create_index("ix_artifact_levels_sort_order", "artifact_levels", ["sort_order"], unique=False)
    op.create_index("ix_artifact_levels_is_active", "artifact_levels", ["is_active"], unique=False)

    op.create_table(
        "artifact_level_items",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("level_id", sa.Integer(), sa.ForeignKey("artifact_levels.id", ondelete="CASCADE"), nullable=False),
        sa.Column("artifact_id", sa.Integer(), sa.ForeignKey("artifacts.id", ondelete="CASCADE"), nullable=False),
        sa.UniqueConstraint("level_id", "artifact_id", name="uq_artifact_level_item"),
    )
    op.create_index("ix_artifact_level_items_level_id", "artifact_level_items", ["level_id"], unique=False)
    op.create_index("ix_artifact_level_items_artifact_id", "artifact_level_items", ["artifact_id"], unique=False)

    # --- organizations: add artifact_level_id ---
    op.add_column("organizations", sa.Column("artifact_level_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_organizations_artifact_level_id",
        "organizations",
        "artifact_levels",
        ["artifact_level_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_organizations_artifact_level_id", "organizations", ["artifact_level_id"], unique=False)

    # --- org_artifacts: add review_status ---
    op.add_column(
        "org_artifacts",
        sa.Column(
            "review_status",
            org_artifact_review_status,
            nullable=False,
            server_default="pending",
        ),
    )
    op.create_index("ix_org_artifacts_review_status", "org_artifacts", ["review_status"], unique=False)

    # --- seed default levels (idempotent) ---
    op.execute(
        """
        INSERT INTO artifact_levels (code, name, sort_order, color, is_active)
        VALUES
          ('L1', 'Уровень 1', 1, '#22c55e', TRUE),  -- green-500
          ('L2', 'Уровень 2', 2, '#f59e0b', TRUE),  -- amber-500
          ('L3', 'Уровень 3', 3, '#ef4444', TRUE)   -- red-500
        ON CONFLICT (code) DO UPDATE
          SET name = EXCLUDED.name,
              sort_order = EXCLUDED.sort_order,
              color = EXCLUDED.color,
              is_active = EXCLUDED.is_active
        """
    )

    # --- populate level items from Artifact.kb_level as a sane default ---
    # Strategy: store "delta" per level, so effective set is union of levels <= selected sort_order:
    # - L1: kb_level == 'КБ1'
    # - L2: kb_level == 'КБ2'
    # - L3: kb_level == 'КБ3' OR anything else (including empty)
    op.execute(
        """
        WITH lvl AS (
          SELECT
            max(id) FILTER (WHERE code='L1') AS l1,
            max(id) FILTER (WHERE code='L2') AS l2,
            max(id) FILTER (WHERE code='L3') AS l3
          FROM artifact_levels
        )
        INSERT INTO artifact_level_items (level_id, artifact_id)
        SELECT (SELECT l1 FROM lvl), a.id
        FROM artifacts a
        WHERE COALESCE(a.kb_level, '') = 'КБ1'
        ON CONFLICT (level_id, artifact_id) DO NOTHING
        """
    )
    op.execute(
        """
        WITH lvl AS (
          SELECT
            max(id) FILTER (WHERE code='L2') AS l2
          FROM artifact_levels
        )
        INSERT INTO artifact_level_items (level_id, artifact_id)
        SELECT (SELECT l2 FROM lvl), a.id
        FROM artifacts a
        WHERE COALESCE(a.kb_level, '') = 'КБ2'
        ON CONFLICT (level_id, artifact_id) DO NOTHING
        """
    )
    op.execute(
        """
        WITH lvl AS (
          SELECT
            max(id) FILTER (WHERE code='L3') AS l3
          FROM artifact_levels
        )
        INSERT INTO artifact_level_items (level_id, artifact_id)
        SELECT (SELECT l3 FROM lvl), a.id
        FROM artifacts a
        WHERE COALESCE(a.kb_level, '') NOT IN ('КБ1','КБ2')
        ON CONFLICT (level_id, artifact_id) DO NOTHING
        """
    )

    # --- set defaults for existing orgs (safe) ---
    op.execute(
        """
        UPDATE organizations
        SET artifact_level_id = (SELECT id FROM artifact_levels WHERE code='L3' LIMIT 1)
        WHERE artifact_level_id IS NULL
        """
    )

    # --- backfill review_status from existing audit fields ---
    op.execute(
        """
        UPDATE org_artifacts
        SET review_status = 'approved'
        WHERE current_file_version_id IS NOT NULL
          AND audited_file_version_id IS NOT NULL
          AND audited_file_version_id = current_file_version_id
        """
    )
    op.execute(
        """
        UPDATE org_artifacts
        SET review_status = 'pending'
        WHERE review_status IS NULL
           OR review_status NOT IN ('pending','approved','needs_correction')
        """
    )

    # Drop server default to keep future behavior explicit in app code.
    op.alter_column("org_artifacts", "review_status", server_default=None)


def downgrade() -> None:
    # org_artifacts.review_status
    op.drop_index("ix_org_artifacts_review_status", table_name="org_artifacts")
    op.drop_column("org_artifacts", "review_status")

    # organizations links
    op.drop_index("ix_organizations_artifact_level_id", table_name="organizations")
    op.drop_constraint("fk_organizations_artifact_level_id", "organizations", type_="foreignkey")
    op.drop_column("organizations", "artifact_level_id")

    # level tables
    op.drop_index("ix_artifact_level_items_artifact_id", table_name="artifact_level_items")
    op.drop_index("ix_artifact_level_items_level_id", table_name="artifact_level_items")
    op.drop_table("artifact_level_items")

    op.drop_index("ix_artifact_levels_is_active", table_name="artifact_levels")
    op.drop_index("ix_artifact_levels_sort_order", table_name="artifact_levels")
    op.drop_table("artifact_levels")

    # drop enum type
    org_artifact_review_status = sa.Enum(name="org_artifact_review_status")
    org_artifact_review_status.drop(op.get_bind(), checkfirst=True)

