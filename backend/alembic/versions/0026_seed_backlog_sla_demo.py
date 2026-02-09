"""Seed demo backlog SLA aging for dashboards.

Revision ID must be <= 32 chars (fits alembic_version.version_num).
"""

from __future__ import annotations

from alembic import op


# revision identifiers, used by Alembic.
revision = "0026_seed_backlog_sla_demo"
down_revision = "0025_org_kb_targets"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Goal: make "Бэклог · В работе" visually meaningful in demo by ensuring
    # there are actionable items older than 3 days / 2 weeks / 1 month.
    #
    # Scope: only Медлайн (org_id=2), sheet "Управление ИБ".
    #
    # Idempotency: only move timestamps backwards (never forward).
    op.execute(
        """
        WITH sheet_ids AS (
          SELECT DISTINCT a.id
          FROM artifacts a
          JOIN index_kb_template_rows r
            ON upper(r.short_name) = upper(a.short_name)
          WHERE r.sheet_name = 'Управление ИБ'
            AND r.kind = 'item'
            AND coalesce(r.short_name,'') <> ''
        ),
        oa_needs_audit AS (
          SELECT oa.id AS oa_id, oa.current_file_version_id AS fv_id
          FROM org_artifacts oa
          WHERE oa.org_id = 2
            AND oa.artifact_id IN (SELECT id FROM sheet_ids)
            AND oa.current_file_version_id IS NOT NULL
            AND oa.audited_file_version_id IS NULL
            AND oa.review_status = 'pending'
          ORDER BY oa.id
        ),
        fv_5d AS (SELECT fv_id FROM oa_needs_audit OFFSET 0 LIMIT 10),
        fv_20d AS (SELECT fv_id FROM oa_needs_audit OFFSET 10 LIMIT 10),
        fv_45d AS (SELECT fv_id FROM oa_needs_audit OFFSET 20 LIMIT 15)
        UPDATE file_versions fv
        SET created_at = CASE
          WHEN fv.id IN (SELECT fv_id FROM fv_45d) AND fv.created_at > (now() - interval '45 days') THEN (now() - interval '45 days')
          WHEN fv.id IN (SELECT fv_id FROM fv_20d) AND fv.created_at > (now() - interval '20 days') THEN (now() - interval '20 days')
          WHEN fv.id IN (SELECT fv_id FROM fv_5d)  AND fv.created_at > (now() - interval '5 days')  THEN (now() - interval '5 days')
          ELSE fv.created_at
        END
        WHERE fv.id IN (SELECT fv_id FROM oa_needs_audit);
        """
    )

    # For needs_correction, we measure age by audited_at; backdate it.
    op.execute(
        """
        WITH sheet_ids AS (
          SELECT DISTINCT a.id
          FROM artifacts a
          JOIN index_kb_template_rows r
            ON upper(r.short_name) = upper(a.short_name)
          WHERE r.sheet_name = 'Управление ИБ'
            AND r.kind = 'item'
            AND coalesce(r.short_name,'') <> ''
        ),
        oa_nc AS (
          SELECT oa.id AS oa_id
          FROM org_artifacts oa
          WHERE oa.org_id = 2
            AND oa.artifact_id IN (SELECT id FROM sheet_ids)
            AND oa.review_status = 'needs_correction'
          ORDER BY oa.id
        ),
        oa_nc_10d AS (SELECT oa_id FROM oa_nc OFFSET 0 LIMIT 2),
        oa_nc_35d AS (SELECT oa_id FROM oa_nc OFFSET 2 LIMIT 2)
        UPDATE org_artifacts oa
        SET audited_at = CASE
          WHEN oa.id IN (SELECT oa_id FROM oa_nc_35d) AND (oa.audited_at IS NULL OR oa.audited_at > (now() - interval '35 days')) THEN (now() - interval '35 days')
          WHEN oa.id IN (SELECT oa_id FROM oa_nc_10d) AND (oa.audited_at IS NULL OR oa.audited_at > (now() - interval '10 days')) THEN (now() - interval '10 days')
          ELSE oa.audited_at
        END
        WHERE oa.id IN (SELECT oa_id FROM oa_nc);
        """
    )


def downgrade() -> None:
    # Non-reversible seed.
    pass

