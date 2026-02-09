"""Add audit period settings to organizations and seed demo values."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0027_org_audit_period"
down_revision = "0026_seed_backlog_sla_demo"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("organizations", sa.Column("audit_period_start", sa.Date(), nullable=True))
    op.add_column("organizations", sa.Column("audit_period_weeks", sa.Integer(), nullable=True))

    # Seed demo values (idempotent: set only if NULL).
    # org ids: 2..11 are demo orgs (Default=1).
    rows = [
        (2, "2026-02-09", 2),  # Медлайн
        (3, "2026-02-09", 2),  # Макомнет
        (4, "2026-03-02", 1),  # Мортон
        (5, "2026-03-02", 1),  # Твинго
        (6, "2026-04-06", 2),  # Глобус
        (7, "2026-04-06", 2),  # Джиэнси
        (8, "2026-06-01", 4),  # Центральный телеграф
        (9, "2026-05-18", 2),  # Розничные системы
        (10, "2026-05-18", 2),  # Цифровое телевидение
        (11, "2026-06-01", 4),  # Центр хранения данных
    ]
    for org_id, start, weeks in rows:
        op.execute(
            sa.text(
                """
                UPDATE organizations
                SET audit_period_start = COALESCE(audit_period_start, CAST(:start AS date)),
                    audit_period_weeks = COALESCE(audit_period_weeks, :weeks)
                WHERE id = :org_id
                """
            ).bindparams(org_id=int(org_id), start=str(start), weeks=int(weeks))
        )


def downgrade() -> None:
    op.drop_column("organizations", "audit_period_weeks")
    op.drop_column("organizations", "audit_period_start")

