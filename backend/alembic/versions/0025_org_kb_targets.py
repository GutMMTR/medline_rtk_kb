"""org index KB targets (per sheet)

Adds per-organization target values for Index KB sheets (УИБ / СЗИ).
Used in:
- Organization settings (admin org edit)
- Dashboards (visual target line/marker)

Seed (idempotent): set the same target for both sheets for selected demo orgs.

Revision ID: 0025_org_kb_targets
Revises: 0024_seed_dash_variance
Create Date: 2026-02-04
"""

from __future__ import annotations

import os

import sqlalchemy as sa
from alembic import op


revision = "0025_org_kb_targets"
down_revision = "0024_seed_dash_variance"
branch_labels = None
depends_on = None


UIB_SHEET_NAME = "Управление ИБ"
SZI_SHEET_NAME = "СЗИ"


def upgrade() -> None:
    op.create_table(
        "org_index_kb_targets",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("sheet_name", sa.String(length=255), nullable=False),
        sa.Column("target_value", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.UniqueConstraint("org_id", "sheet_name", name="uq_org_index_kb_targets_org_sheet"),
    )
    op.create_index("ix_org_index_kb_targets_org_id", "org_index_kb_targets", ["org_id"], unique=False)
    op.create_index("ix_org_index_kb_targets_sheet_name", "org_index_kb_targets", ["sheet_name"], unique=False)

    # Seed demo targets (optional; safe for prod too — only affects orgs by exact name).
    if (os.getenv("SEED_DEMO_DATA") or "").strip() not in ("1", "true", "True", "yes", "on"):
        return

    conn = op.get_bind()

    # User-provided targets:
    # NOTE: "Медлайн 2" here means target=2 for org "Медлайн", same for "Макомнет 2".
    org_targets: dict[str, float] = {
        "Медлайн": 2.0,
        "Макомнет": 2.0,
        "Мортон": 1.0,
        "Твинго": 1.0,
        "Глобус": 1.5,
        "Джиэнси": 2.0,
        "Центральный телеграф": 3.0,
        "Розничные системы": 2.0,
        "Цифровое телевидение": 2.5,
        "Центр хранения данных": 3.0,
    }

    # Upsert for both sheets.
    for org_name, target in org_targets.items():
        oid = conn.execute(sa.text("SELECT id FROM organizations WHERE name=:n"), {"n": org_name}).scalar()
        if oid is None:
            continue
        for sheet in (UIB_SHEET_NAME, SZI_SHEET_NAME):
            conn.execute(
                sa.text(
                    """
                    INSERT INTO org_index_kb_targets (org_id, sheet_name, target_value)
                    VALUES (:o, :s, :v)
                    ON CONFLICT (org_id, sheet_name)
                    DO UPDATE SET target_value=EXCLUDED.target_value
                    """
                ),
                {"o": int(oid), "s": sheet, "v": float(target)},
            )


def downgrade() -> None:
    op.drop_index("ix_org_index_kb_targets_sheet_name", table_name="org_index_kb_targets")
    op.drop_index("ix_org_index_kb_targets_org_id", table_name="org_index_kb_targets")
    op.drop_table("org_index_kb_targets")

