"""audit periods: calendar date_from/date_to (instead of days)

Revision ID: 0015_audit_period_calendar_dates
Revises: 0014_periods_levels_review
Create Date: 2026-01-30
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0015_audit_period_calendar_dates"
down_revision = "0014_periods_levels_review"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add calendar boundaries (temporary nullable for backfill).
    op.add_column("audit_periods", sa.Column("date_from", sa.Date(), nullable=True))
    op.add_column("audit_periods", sa.Column("date_to", sa.Date(), nullable=True))
    op.create_index("ix_audit_periods_date_from", "audit_periods", ["date_from"], unique=False)
    op.create_index("ix_audit_periods_date_to", "audit_periods", ["date_to"], unique=False)

    # Backfill existing rows from previous "days" meaning:
    # date_to = current_date; date_from = current_date - (days-1).
    op.execute(
        """
        UPDATE audit_periods
        SET
          date_to = CURRENT_DATE,
          date_from = (CURRENT_DATE - ((GREATEST(days, 1) - 1) * INTERVAL '1 day'))::date
        WHERE date_from IS NULL OR date_to IS NULL
        """
    )

    # Make boundaries required going forward.
    op.alter_column("audit_periods", "date_from", nullable=False)
    op.alter_column("audit_periods", "date_to", nullable=False)

    # Drop old "days" column.
    op.drop_column("audit_periods", "days")


def downgrade() -> None:
    # Recreate "days" column (fallback to computed difference).
    op.add_column("audit_periods", sa.Column("days", sa.Integer(), nullable=False, server_default="365"))
    op.execute(
        """
        UPDATE audit_periods
        SET days = GREATEST(1, (date_to - date_from) + 1)
        """
    )
    op.alter_column("audit_periods", "days", server_default=None)

    op.drop_index("ix_audit_periods_date_to", table_name="audit_periods")
    op.drop_index("ix_audit_periods_date_from", table_name="audit_periods")
    op.drop_column("audit_periods", "date_to")
    op.drop_column("audit_periods", "date_from")

