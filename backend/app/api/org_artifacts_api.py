from __future__ import annotations

import hashlib
from datetime import datetime

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile, status
from fastapi.responses import Response
from sqlalchemy import and_, func, insert, select
from sqlalchemy.orm import Session

from app.audit.service import write_audit_log
from app.auth.dependencies import get_current_user_bearer, get_user_role_for_org
from app.core.config import settings
from app.db.models import Artifact, FileVersion, OrgArtifact, OrgArtifactStatus, Role, User
from app.db.session import get_db
from app.obs.metrics import metrics


router = APIRouter(prefix="/api", tags=["org-artifacts"])


def _require_org_access(db: Session, user: User, org_id: int) -> Role:
    role = get_user_role_for_org(db, user, org_id)
    if not role:
        raise HTTPException(status_code=403, detail="Нет доступа к организации")
    return role


def _ensure_org_artifacts_materialized(db: Session, org_id: int) -> None:
    # INSERT INTO org_artifacts (...) SELECT ... WHERE NOT EXISTS ...
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


class ArtifactRowOut(dict):
    pass


@router.get("/orgs/{org_id}/artifacts")
def list_artifacts(
    org_id: int,
    request: Request,
    topic: str | None = Query(default=None),
    domain: str | None = Query(default=None),
    kb_level: str | None = Query(default=None),
    short_name: str | None = Query(default=None),
    q: str | None = Query(default=None, description="Поиск по indicator_name/title/achievement"),
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_bearer),
):
    _require_org_access(db, user, org_id)
    _ensure_org_artifacts_materialized(db, org_id)
    db.commit()

    query = (
        db.query(OrgArtifact, Artifact)
        .join(Artifact, Artifact.id == OrgArtifact.artifact_id)
        .filter(OrgArtifact.org_id == org_id)
        .order_by(Artifact.topic.asc(), Artifact.domain.asc(), Artifact.short_name.asc(), Artifact.achievement_item_no.asc().nullsfirst())
    )
    if topic:
        query = query.filter(Artifact.topic == topic)
    if domain:
        query = query.filter(Artifact.domain == domain)
    if kb_level:
        query = query.filter(Artifact.kb_level == kb_level)
    if short_name:
        query = query.filter(Artifact.short_name == short_name)
    if q:
        like = f"%{q.strip()}%"
        query = query.filter(
            (Artifact.indicator_name.ilike(like))
            | (Artifact.title.ilike(like))
            | (Artifact.achievement_text.ilike(like))
            | (Artifact.achievement_item_text.ilike(like))
        )

    total = query.count()
    rows = query.offset(offset).limit(limit).all()

    def to_row(oa: OrgArtifact, a: Artifact) -> dict:
        return {
            "org_artifact_id": oa.id,
            "artifact_id": a.id,
            "artifact_key": a.artifact_key,
            "topic": a.topic,
            "domain": a.domain,
            "indicator_name": a.indicator_name,
            "short_name": a.short_name,
            "kb_level": a.kb_level,
            "achievement_text": a.achievement_text,
            "achievement_item_no": a.achievement_item_no,
            "achievement_item_text": a.achievement_item_text,
            "title": a.title,
            "status": oa.status.value,
            "current_file_version_id": oa.current_file_version_id,
            "updated_at": oa.updated_at.isoformat() if oa.updated_at else None,
            "updated_by_user_id": oa.updated_by_user_id,
        }

    return {"total": total, "items": [to_row(oa, a) for (oa, a) in rows]}


@router.post("/orgs/{org_id}/artifacts/{org_artifact_id}/upload")
def upload_artifact_file(
    org_id: int,
    org_artifact_id: int,
    request: Request,
    upload: UploadFile = File(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_bearer),
):
    role = _require_org_access(db, user, org_id)
    if role not in (Role.customer, Role.auditor, Role.admin):
        raise HTTPException(status_code=403, detail="Недостаточно прав")

    oa = db.get(OrgArtifact, org_artifact_id)
    if not oa or oa.org_id != org_id:
        raise HTTPException(status_code=404, detail="Артефакт организации не найден")

    content = upload.file.read()
    size_bytes = len(content)
    if size_bytes > settings.max_upload_mb * 1024 * 1024:
        metrics.inc_upload_error()
        raise HTTPException(status_code=413, detail=f"Файл слишком большой. Лимит {settings.max_upload_mb} МБ")

    sha256 = hashlib.sha256(content).hexdigest()
    current_max = db.query(func.max(FileVersion.version_no)).filter(FileVersion.org_artifact_id == oa.id).scalar() or 0
    next_version = int(current_max) + 1

    fv = FileVersion(
        org_artifact_id=oa.id,
        version_no=next_version,
        original_filename=upload.filename or "file",
        content_type=upload.content_type or "application/octet-stream",
        size_bytes=size_bytes,
        sha256=sha256,
        storage_backend="postgres",
        storage_key=None,
        blob=content,
        created_at=datetime.utcnow(),
        created_by_user_id=user.id,
    )
    db.add(fv)
    db.flush()

    before = {
        "status": oa.status.value,
        "current_file_version_id": oa.current_file_version_id,
        "audited_file_version_id": oa.audited_file_version_id,
        "audited_at": oa.audited_at.isoformat() if oa.audited_at else None,
        "audited_by_user_id": oa.audited_by_user_id,
    }
    oa.status = OrgArtifactStatus.uploaded
    oa.current_file_version_id = fv.id
    oa.updated_at = datetime.utcnow()
    oa.updated_by_user_id = user.id
    # New version => audit reset
    oa.audited_file_version_id = None
    oa.audited_at = None
    oa.audited_by_user_id = None
    after = {
        "status": oa.status.value,
        "current_file_version_id": oa.current_file_version_id,
        "audited_file_version_id": oa.audited_file_version_id,
        "audited_at": oa.audited_at,
        "audited_by_user_id": oa.audited_by_user_id,
    }

    write_audit_log(
        db,
        actor=user,
        org_id=org_id,
        action="upload",
        entity_type="org_artifact",
        entity_id=str(oa.id),
        before=before,
        after=after,
        request=request,
    )
    db.commit()
    metrics.inc_upload(size_bytes=size_bytes)
    return {"ok": True, "file_version_id": fv.id, "version_no": fv.version_no}


@router.get("/orgs/{org_id}/artifacts/{org_artifact_id}/download")
def download_artifact_file(
    org_id: int,
    org_artifact_id: int,
    version: int | None = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_bearer),
) -> Response:
    _require_org_access(db, user, org_id)

    oa = db.get(OrgArtifact, org_artifact_id)
    if not oa or oa.org_id != org_id:
        raise HTTPException(status_code=404, detail="Артефакт организации не найден")

    q = db.query(FileVersion).filter(FileVersion.org_artifact_id == oa.id)
    if version is not None:
        fv = q.filter(FileVersion.version_no == version).one_or_none()
    else:
        if oa.current_file_version_id:
            fv = db.get(FileVersion, oa.current_file_version_id)
        else:
            fv = q.order_by(FileVersion.version_no.desc()).first()
    if not fv or not fv.blob:
        metrics.inc_download_error()
        raise HTTPException(status_code=404, detail="Файл не найден")

    headers = {"Content-Disposition": f'attachment; filename="{fv.original_filename}"'}
    metrics.inc_download()
    return Response(content=fv.blob, media_type=fv.content_type, headers=headers)


@router.patch("/orgs/{org_id}/artifacts/{org_artifact_id}")
def patch_org_artifact(
    org_id: int,
    org_artifact_id: int,
    request: Request,
    status_value: str | None = Query(default=None, alias="status"),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user_bearer),
):
    role = _require_org_access(db, user, org_id)
    if role not in (Role.auditor, Role.admin):
        raise HTTPException(status_code=403, detail="Только auditor/admin могут менять статус вручную")

    oa = db.get(OrgArtifact, org_artifact_id)
    if not oa or oa.org_id != org_id:
        raise HTTPException(status_code=404, detail="Артефакт организации не найден")

    before = {"status": oa.status.value}
    if status_value is not None:
        try:
            oa.status = OrgArtifactStatus(status_value)
        except ValueError:
            raise HTTPException(status_code=400, detail="Некорректный статус")
    oa.updated_at = datetime.utcnow()
    oa.updated_by_user_id = user.id

    write_audit_log(
        db,
        actor=user,
        org_id=org_id,
        action="patch",
        entity_type="org_artifact",
        entity_id=str(oa.id),
        before=before,
        after={"status": oa.status.value},
        request=request,
    )
    db.commit()
    return {"ok": True}

