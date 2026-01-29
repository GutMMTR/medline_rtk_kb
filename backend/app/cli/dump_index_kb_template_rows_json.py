from __future__ import annotations

import json

from sqlalchemy.orm import Session

from app.db.models import IndexKbTemplateRow
from app.db.session import engine


def main() -> None:
    with Session(engine) as db:
        rows = (
            db.query(IndexKbTemplateRow)
            .order_by(IndexKbTemplateRow.sheet_name.asc(), IndexKbTemplateRow.sort_order.asc(), IndexKbTemplateRow.id.asc())
            .all()
        )

    out = []
    for r in rows:
        out.append(
            {
                "sheet_name": r.sheet_name,
                "sort_order": int(r.sort_order or 0),
                "kind": r.kind,
                "row_key": r.row_key,
                "title": r.title,
                "short_name": r.short_name,
                "group_code": r.group_code,
            }
        )

    # Keep it compact (one line) to reduce risk of truncation in some shells/tools.
    print(json.dumps(out, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()

