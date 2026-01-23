from __future__ import annotations

import csv
import hashlib
from datetime import datetime
from io import StringIO
from typing import Dict, Tuple

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile, status
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.audit.service import write_audit_log
from app.auth.dependencies import require_admin_bearer
from app.auth.security import hash_password
from app.db.models import Artifact, ArtifactNode, Organization, Role, User, UserOrgMembership
from app.db.session import get_db
from app.importers.program_excel import ProgramArtifactRow, parse_program_xlsx
from app.obs.metrics import metrics


router = APIRouter(prefix="/api/admin", tags=["admin"])


class OrgOut(BaseModel):
    id: int
    name: str


class UserOut(BaseModel):
    id: int
    login: str
    full_name: str
    is_active: bool
    is_admin: bool


class MembershipOut(BaseModel):
    id: int
    user_id: int
    org_id: int
    role: str


@router.get("/orgs", response_model=list[OrgOut])
def admin_list_orgs(db: Session = Depends(get_db), _: User = Depends(require_admin_bearer)):
    orgs = db.query(Organization).order_by(Organization.name.asc()).all()
    return [OrgOut(id=o.id, name=o.name) for o in orgs]


class OrgCreate(BaseModel):
    name: str


@router.post("/orgs", response_model=OrgOut)
def admin_create_org(req: OrgCreate, db: Session = Depends(get_db), user: User = Depends(require_admin_bearer)):
    name = req.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Имя организации обязательно")
    exists = db.query(Organization).filter(Organization.name == name).one_or_none()
    if exists:
        raise HTTPException(status_code=400, detail="Организация уже существует")
    org = Organization(name=name)
    db.add(org)
    db.flush()
    write_audit_log(db, actor=user, org_id=None, action="create", entity_type="organization", entity_id=str(org.id), after={"name": name})
    db.commit()
    return OrgOut(id=org.id, name=org.name)


@router.get("/users", response_model=list[UserOut])
def admin_list_users(db: Session = Depends(get_db), _: User = Depends(require_admin_bearer)):
    users = db.query(User).order_by(User.login.asc()).all()
    return [UserOut(id=u.id, login=u.login, full_name=u.full_name, is_active=u.is_active, is_admin=u.is_admin) for u in users]


class UserCreate(BaseModel):
    login: str
    password: str
    full_name: str = ""
    is_admin: bool = False


@router.post("/users", response_model=UserOut)
def admin_create_user(req: UserCreate, db: Session = Depends(get_db), user: User = Depends(require_admin_bearer)):
    login = req.login.strip()
    if not login:
        raise HTTPException(status_code=400, detail="Логин обязателен")
    exists = db.query(User).filter(User.login == login).one_or_none()
    if exists:
        raise HTTPException(status_code=400, detail="Логин уже используется")
    new_user = User(
        login=login,
        password_hash=hash_password(req.password),
        full_name=req.full_name.strip(),
        is_active=True,
        is_admin=bool(req.is_admin),
    )
    db.add(new_user)
    db.flush()
    write_audit_log(db, actor=user, org_id=None, action="create", entity_type="user", entity_id=str(new_user.id), after={"login": login})
    db.commit()
    return UserOut(id=new_user.id, login=new_user.login, full_name=new_user.full_name, is_active=new_user.is_active, is_admin=new_user.is_admin)


@router.get("/memberships", response_model=list[MembershipOut])
def admin_list_memberships(db: Session = Depends(get_db), _: User = Depends(require_admin_bearer)):
    ms = db.query(UserOrgMembership).order_by(UserOrgMembership.created_at.desc()).all()
    return [MembershipOut(id=m.id, user_id=m.user_id, org_id=m.org_id, role=m.role.value) for m in ms]


class MembershipUpsert(BaseModel):
    user_id: int
    org_id: int
    role: str


@router.post("/memberships", response_model=MembershipOut)
def admin_upsert_membership(req: MembershipUpsert, db: Session = Depends(get_db), user: User = Depends(require_admin_bearer)):
    try:
        role_enum = Role(req.role)
    except ValueError:
        raise HTTPException(status_code=400, detail="Некорректная роль")
    m = (
        db.query(UserOrgMembership)
        .filter(UserOrgMembership.user_id == req.user_id, UserOrgMembership.org_id == req.org_id)
        .one_or_none()
    )
    if m:
        before = {"role": m.role.value}
        m.role = role_enum
        after = {"role": m.role.value}
        write_audit_log(db, actor=user, org_id=req.org_id, action="update", entity_type="membership", entity_id=str(m.id), before=before, after=after)
    else:
        m = UserOrgMembership(user_id=req.user_id, org_id=req.org_id, role=role_enum)
        db.add(m)
        db.flush()
        write_audit_log(db, actor=user, org_id=req.org_id, action="create", entity_type="membership", entity_id=str(m.id), after={"role": m.role.value})
    db.commit()
    return MembershipOut(id=m.id, user_id=m.user_id, org_id=m.org_id, role=m.role.value)


def _artifact_key(short_name: str, item_no: int | None) -> str:
    return f"{short_name}#{item_no}" if item_no is not None else short_name


def _resolve_artifact_key_and_segment(
    db: Session,
    *,
    seen_base_keys: Dict[str, int],
    short_name: str,
    item_no: int | None,
    indicator_name: str,
    achievement_item_text: str,
    topic: str,
    domain: str,
    kb_level: str,
) -> Tuple[str, str | None]:
    short_name = (short_name or "").strip()
    base_key = _artifact_key(short_name, item_no)
    if item_no is not None:
        return base_key, str(item_no)

    seen_base_keys[base_key] = seen_base_keys.get(base_key, 0) + 1
    if seen_base_keys[base_key] > 1:
        stable = f"{topic}|{domain}|{short_name}|{kb_level}|{indicator_name}|{achievement_item_text}"
        h = hashlib.sha256(stable.encode("utf-8")).hexdigest()[:8]
        return f"{short_name}~{h}", h

    existing = db.query(Artifact).filter(Artifact.artifact_key == base_key).one_or_none()
    if not existing:
        return base_key, None
    if (existing.indicator_name or "") == (indicator_name or "") and (existing.achievement_item_text or "") == (achievement_item_text or ""):
        return base_key, None

    stable = f"{topic}|{domain}|{short_name}|{kb_level}|{indicator_name}|{achievement_item_text}"
    h = hashlib.sha256(stable.encode("utf-8")).hexdigest()[:8]
    return f"{short_name}~{h}", h


def _ensure_node_path(db: Session, segments: list[str]) -> ArtifactNode:
    parent_id: int | None = None
    full_path_parts: list[str] = []
    node: ArtifactNode | None = None
    for seg in segments:
        seg = (seg or "").strip()
        if not seg:
            continue
        full_path_parts.append(seg)
        full_path = ".".join(full_path_parts)
        node = db.query(ArtifactNode).filter(ArtifactNode.full_path == full_path).one_or_none()
        if not node:
            node = ArtifactNode(parent_id=parent_id, segment=seg, full_path=full_path, sort_order=0, created_at=datetime.utcnow())
            db.add(node)
            db.flush()
        parent_id = node.id
    if not node:
        raise ValueError("Пустой путь узлов")
    return node


def _parse_csv(content: bytes) -> list[ProgramArtifactRow]:
    # Минимальный CSV: full_path, artifact_key (опц), title (опц)
    # Для текущего шаблона "Программа" CSV пока не основной, поддержим позже.
    raise ValueError("CSV для листа 'Программа' пока не поддержан, используйте .xlsx")


def _load_program_rows(file: UploadFile) -> tuple[list[ProgramArtifactRow], str]:
    content = file.file.read()
    sha = hashlib.sha256(content).hexdigest()
    filename = (file.filename or "").lower()
    if filename.endswith(".xlsx"):
        rows = parse_program_xlsx(content)
    elif filename.endswith(".csv"):
        rows = _parse_csv(content)
    else:
        raise ValueError("Поддерживаем только .xlsx (и опционально .csv)")
    return rows, sha


class ImportApplyOut(BaseModel):
    created: int
    updated: int
    sha256: str


@router.post("/artifacts/import", response_model=ImportApplyOut)
def import_artifacts_apply(
    request: Request,
    upload: UploadFile = File(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin_bearer),
):
    try:
        rows, sha = _load_program_rows(upload)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    created = 0
    updated = 0
    seen_base_keys: Dict[str, int] = {}
    try:
        for r in rows:
            key, extra_segment = _resolve_artifact_key_and_segment(
                db,
                seen_base_keys=seen_base_keys,
                short_name=r.short_name,
                item_no=r.achievement_item_no,
                indicator_name=r.indicator_name,
                achievement_item_text=r.achievement_item_text,
                topic=r.topic,
                domain=r.domain,
                kb_level=r.kb_level,
            )
            short_parts = [p.strip() for p in (r.short_name or "").split(".") if p.strip()]
            segments = [r.topic or "", r.domain or "", *short_parts]
            if extra_segment:
                segments.append(extra_segment)
            leaf = _ensure_node_path(db, segments)

            a = db.query(Artifact).filter(Artifact.artifact_key == key).one_or_none()
            payload = {
                "node_id": leaf.id,
                "artifact_key": key,
                "topic": r.topic,
                "domain": r.domain,
                "indicator_name": r.indicator_name,
                "short_name": r.short_name,
                "kb_level": r.kb_level,
                "achievement_text": r.achievement_text,
                "achievement_item_no": r.achievement_item_no,
                "achievement_item_text": r.achievement_item_text,
                "title": r.achievement_item_text,
            }
            if not a:
                a = Artifact(**payload, description="", created_at=datetime.utcnow())
                db.add(a)
                db.flush()
                created += 1
            else:
                before = {
                    "topic": a.topic,
                    "domain": a.domain,
                    "indicator_name": a.indicator_name,
                    "kb_level": a.kb_level,
                    "achievement_item_text": a.achievement_item_text,
                }
                for k, v in payload.items():
                    setattr(a, k, v)
                db.flush()
                updated += 1
                write_audit_log(
                    db,
                    actor=user,
                    org_id=None,
                    action="update",
                    entity_type="artifact",
                    entity_id=str(a.id),
                    before=before,
                    after={k: payload[k] for k in before.keys()},
                    request=request,
                )
    except IntegrityError as e:
        db.rollback()
        msg = str(e.orig) if getattr(e, "orig", None) else str(e)
        raise HTTPException(status_code=400, detail=f"Ошибка целостности БД при импорте: {msg}")

    write_audit_log(
        db,
        actor=user,
        org_id=None,
        action="import_apply",
        entity_type="artifacts",
        entity_id=sha,
        after={"created": created, "updated": updated, "filename": upload.filename or ""},
        request=request,
    )
    db.commit()
    metrics.inc_import()
    return ImportApplyOut(created=created, updated=updated, sha256=sha)

