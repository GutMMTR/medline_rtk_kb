from __future__ import annotations

import argparse
import os

from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import engine
from app.index_kb.szi_sheet import ensure_szi_template_loaded
from app.index_kb.uib_sheet import ensure_uib_template_loaded


def main() -> None:
    ap = argparse.ArgumentParser(description="Load Index KB sheet structure into DB (one-time).")
    ap.add_argument("--sheet", default="szi", choices=["szi", "uib"], help="Which sheet to load")
    ap.add_argument("--template-path", default="", help="Path to xlsx template (optional; uses settings/env by default)")
    ap.add_argument("--force", action="store_true", help="Replace existing rows in DB")
    args = ap.parse_args()

    template_path = (args.template_path or "").strip()
    if not template_path:
        template_path = os.environ.get("INDEX_KB_TEMPLATE_PATH", "") or (settings.index_kb_template_path or "")

    with Session(engine) as db:
        if args.sheet == "szi":
            cnt = ensure_szi_template_loaded(db, template_path=template_path, force=bool(args.force))
            print(f"OK: loaded SZI template rows: {cnt}")
        if args.sheet == "uib":
            cnt = ensure_uib_template_loaded(db, template_path=template_path, force=bool(args.force))
            print(f"OK: loaded UIB template rows: {cnt}")


if __name__ == "__main__":
    main()

