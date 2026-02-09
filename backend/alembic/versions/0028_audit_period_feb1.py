"""Update demo audit periods to start from Feb 1, 2026.

Scope: demo org ids 2..11 (Default=1).
Idempotency/safety: update only when value is NULL or matches the previous seeded dates,
so manual edits in admin UI are not overwritten.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0028_audit_period_feb1"
down_revision = "0027_org_audit_period"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        sa.text(
            """
            UPDATE organizations
            SET audit_period_start = CAST('2026-02-01' AS date)
            WHERE id BETWEEN 2 AND 11
              AND (
                audit_period_start IS NULL
                OR audit_period_start IN (
                  CAST('2026-02-09' AS date),
                  CAST('2026-03-02' AS date),
                  CAST('2026-04-06' AS date),
                  CAST('2026-05-18' AS date),
                  CAST('2026-06-01' AS date)
                )
              )
            """
        )
    )


def downgrade() -> None:
    # Best-effort rollback to the original per-org demo dates (only if still equals Feb 1).
    rows = [
        (2, "2026-02-09"),
        (3, "2026-02-09"),
        (4, "2026-03-02"),
        (5, "2026-03-02"),
        (6, "2026-04-06"),
        (7, "2026-04-06"),
        (8, "2026-06-01"),
        (9, "2026-05-18"),
        (10, "2026-05-18"),
        (11, "2026-06-01"),
    ]
    for org_id, start in rows:
        op.execute(
            sa.text(
                """
                UPDATE organizations
                SET audit_period_start = CAST(:start AS date)
                WHERE id = :org_id
                  AND audit_period_start = CAST('2026-02-01' AS date)
                """
            ).bindparams(org_id=int(org_id), start=str(start))
        )

