from __future__ import annotations

import argparse
import hashlib
import os
from datetime import datetime
from pathlib import Path

from sqlalchemy import and_, func, insert, select
from sqlalchemy.orm import Session

from app.audit.service import write_audit_log
from app.auth.security import hash_password
from app.db.models import Artifact, FileVersion, OrgArtifact, OrgArtifactStatus, Role, User
from app.db.session import SessionLocal


def _ensure_migration_user(db: Session) -> User:
    u = db.query(User).filter(User.login == "migration_service").one_or_none()
    if not u:
        u = User(
            login="migration_service",
            password_hash=hash_password("migration_service_disabled"),
            full_name="Migration Service",
            is_active=False,
            is_admin=True,
        )
        db.add(u)
        db.flush()
    return u


def _ensure_org_artifacts_materialized(db: Session, org_id: int) -> None:
    now = datetime.utcnow()
    stmt = insert(OrgArtifact).from_select(
        ["org_id", "artifact_id", "status", "created_at", "updated_at"],
        select(
            func.cast(org_id, OrgArtifact.org_id.type),
            Artifact.id,
            func.cast(OrgArtifactStatus.missing.value, OrgArtifact.status.type),
            func.cast(now, OrgArtifact.created_at.type),
            func.cast(now, OrgArtifact.updated_at.type),
        ).where(
            ~select(1)
            .where(and_(OrgArtifact.org_id == org_id, OrgArtifact.artifact_id == Artifact.id))
            .exists()
        ),
    )
    db.execute(stmt)


def _sha256_file(path: Path) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            size += len(chunk)
            h.update(chunk)
    return h.hexdigest(), size


def main() -> int:
    p = argparse.ArgumentParser(description="Импорт файлов с шары в Postgres blob storage (MVP).")
    p.add_argument("--base-path", required=True, help="Путь к смонтированной шаре (или папке).")
    p.add_argument("--org-id", type=int, required=True, help="org_id, в которую импортируем.")
    p.add_argument("--dry-run", action="store_true", help="Только отчёт, без записи в БД.")
    p.add_argument("--apply", action="store_true", help="Записать версии файлов в БД.")
    args = p.parse_args()

    if args.apply and args.dry_run:
        raise SystemExit("Нельзя одновременно --dry-run и --apply")
    if not args.apply and not args.dry_run:
        raise SystemExit("Укажите один режим: --dry-run или --apply")

    base = Path(args.base_path)
    if not base.exists() or not base.is_dir():
        raise SystemExit(f"Путь не найден или не директория: {base}")

    db = SessionLocal()
    try:
        actor = _ensure_migration_user(db)
        _ensure_org_artifacts_materialized(db, args.org_id)
        if args.apply:
            db.commit()

        found = 0
        imported = 0
        skipped = 0
        errors = 0

        for path in base.rglob("*"):
            if not path.is_file():
                continue
            found += 1
            stem = path.stem.strip()
            if not stem:
                skipped += 1
                continue

            # MVP-маппинг: имя файла без расширения == artifact_key
            artifact_key = stem
            a = db.query(Artifact).filter(Artifact.artifact_key == artifact_key).one_or_none()
            if not a:
                # эвристика: иногда на шаре точки заменены на _
                a = db.query(Artifact).filter(Artifact.artifact_key == artifact_key.replace("_", ".")).one_or_none()
            if not a:
                skipped += 1
                continue

            oa = db.query(OrgArtifact).filter(OrgArtifact.org_id == args.org_id, OrgArtifact.artifact_id == a.id).one_or_none()
            if not oa:
                skipped += 1
                continue

            try:
                sha, size = _sha256_file(path)
                current = None
                if oa.current_file_version_id:
                    current = db.get(FileVersion, oa.current_file_version_id)
                if current and current.sha256 == sha:
                    skipped += 1
                    continue

                if args.dry_run:
                    imported += 1
                    continue

                content = path.read_bytes()
                current_max = db.query(func.max(FileVersion.version_no)).filter(FileVersion.org_artifact_id == oa.id).scalar() or 0
                fv = FileVersion(
                    org_artifact_id=oa.id,
                    version_no=int(current_max) + 1,
                    original_filename=path.name,
                    content_type="application/octet-stream",
                    size_bytes=size,
                    sha256=sha,
                    storage_backend="postgres",
                    storage_key=str(path),
                    blob=content,
                    created_at=datetime.utcnow(),
                    created_by_user_id=actor.id,
                )
                db.add(fv)
                db.flush()

                before = {"status": oa.status.value, "current_file_version_id": oa.current_file_version_id}
                oa.status = OrgArtifactStatus.uploaded
                oa.current_file_version_id = fv.id
                oa.updated_at = datetime.utcnow()
                oa.updated_by_user_id = actor.id
                after = {"status": oa.status.value, "current_file_version_id": oa.current_file_version_id}

                write_audit_log(
                    db,
                    actor=actor,
                    org_id=args.org_id,
                    action="migration_import",
                    entity_type="org_artifact",
                    entity_id=str(oa.id),
                    before=before,
                    after=after,
                    request=None,
                )
                imported += 1
            except Exception:
                errors += 1

        if args.apply:
            write_audit_log(
                db,
                actor=actor,
                org_id=args.org_id,
                action="migration_summary",
                entity_type="migration",
                entity_id=str(base),
                after={"found": found, "imported": imported, "skipped": skipped, "errors": errors},
                request=None,
            )
            db.commit()

        print(f"found={found} imported={imported} skipped={skipped} errors={errors} mode={'apply' if args.apply else 'dry-run'}")
        return 0 if errors == 0 else 2
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())

