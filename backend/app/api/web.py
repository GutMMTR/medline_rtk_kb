from __future__ import annotations

import hashlib
import io
import os
from datetime import datetime, timezone
from typing import Dict, Tuple
from urllib.parse import quote, urlencode
from datetime import timedelta

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from openpyxl import Workbook
from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_, func, insert, select
from sqlalchemy.orm import Session, aliased

from app.audit.service import write_audit_log
from app.auth.dependencies import get_current_user, get_user_role_for_org, require_admin
from app.auth.security import JWT_COOKIE_NAME, create_access_token, hash_password, verify_password
from app.core.config import settings
from app.db.models import (
    Artifact,
    ArtifactNode,
    FileVersion,
    NextcloudIntegrationSettings,
    NextcloudRemoteFileState,
    OrgArtifact,
    OrgArtifactComment,
    OrgArtifactStatus,
    Organization,
    Role,
    StoredFile,
    User,
    UserOrgMembership,
)
from app.db.session import get_db
from app.importers.program_excel import parse_program_xlsx
from app.index_kb.excel_fill import fill_workbook_for_org
from app.index_kb.formula_eval import build_evaluator_from_openpyxl_workbook
from app.index_kb.sheet_render import iter_render_rows
from app.index_kb.template_loader import get_index_kb_template
from app.index_kb.uib_sheet import UIB_SHEET_NAME, build_uib_view, upsert_manual_value
from app.integrations.nextcloud_dav import NextcloudDavClient, build_webdav_base_url
from app.integrations.nextcloud_sync import sync_from_nextcloud


router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _fmt_dt(value: object) -> str:
    """
    Форматируем datetime для UI: делаем стабильный UTC+3 (MSK), т.к. в контейнере/браузере
    на практике бывают проблемы с tzdata/кешем. Для MVP считаем, что все даты в БД в UTC.
    """
    if value is None:
        return ""
    if isinstance(value, datetime):
        dt = value
        # Treat naive as UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc).replace(microsecond=0, tzinfo=None)
        dt = dt + timedelta(hours=3)  # MSK
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


templates.env.filters["dt"] = _fmt_dt


def _validate_password(password: str) -> str | None:
    """Минимальная политика пароля для админки."""
    p = password or ""
    if len(p) < 8:
        return "Пароль должен быть не короче 8 символов"
    if any(ch.isspace() for ch in p):
        return "Пароль не должен содержать пробелы"
    has_lower = any("a" <= ch.lower() <= "z" and ch.islower() for ch in p)
    has_upper = any("a" <= ch.lower() <= "z" and ch.isupper() for ch in p)
    has_digit = any(ch.isdigit() for ch in p)
    has_special = any(not ch.isalnum() for ch in p)
    if not (has_lower and has_upper and has_digit and has_special):
        return "Пароль должен содержать: строчные и заглавные буквы, цифры и спецсимвол"
    return None


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=status.HTTP_303_SEE_OTHER)


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request) -> HTMLResponse:
    resp = templates.TemplateResponse("login.html", {"request": request, "error": None, "container_class": "container-wide"})
    # Важно: страница логина часто кешируется браузером (особенно при back/forward).
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@router.post("/login")
def login_action(
    request: Request,
    login: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
) -> Response:
    user = db.query(User).filter(User.login == login).one_or_none()
    if user and not user.is_active:
        resp = templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Пользователь заблокирован", "container_class": "container-wide"},
            status_code=403,
        )
        resp.headers["Cache-Control"] = "no-store, max-age=0"
        return resp

    if not user or not verify_password(password, user.password_hash):
        resp = templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Неверный логин или пароль", "container_class": "container-wide"},
            status_code=400,
        )
        resp.headers["Cache-Control"] = "no-store, max-age=0"
        return resp

    token = create_access_token(user.id)
    resp = _redirect("/")
    resp.set_cookie(
        key=JWT_COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        secure=False,  # в проде включим за reverse-proxy с HTTPS
    )
    return resp


@router.post("/logout")
def logout_action() -> Response:
    resp = _redirect("/login")
    resp.delete_cookie(JWT_COOKIE_NAME)
    return resp


@router.get("/", response_class=HTMLResponse)
def index(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    org_id: int | None = None,
) -> Response:
    # Admin UI: админку открываем сразу (это основной кабинет администратора).
    if user.is_admin:
        return _redirect("/admin")

    # Доступные организации
    # MVP-допущение: аудитор глобальный, если есть хотя бы один membership auditor
    is_global_auditor = (
        db.query(UserOrgMembership)
        .filter(UserOrgMembership.user_id == user.id, UserOrgMembership.role == Role.auditor)
        .first()
        is not None
    )
    if user.is_admin or is_global_auditor:
        orgs = db.query(Organization).order_by(Organization.name.asc()).all()
    else:
        orgs = (
            db.query(Organization)
            .join(UserOrgMembership, UserOrgMembership.org_id == Organization.id)
            .filter(UserOrgMembership.user_id == user.id)
            .order_by(Organization.name.asc())
            .all()
        )
    if not orgs:
        return templates.TemplateResponse(
            "empty.html",
            {"request": request, "user": user, "message": "Нет доступных организаций. Обратитесь к администратору."},
        )

    selected_org_id = org_id or orgs[0].id
    role = get_user_role_for_org(db, user, selected_org_id)
    if not role:
        selected_org_id = orgs[0].id
        role = get_user_role_for_org(db, user, selected_org_id)

    # Customer UI: по умолчанию ведём пользователя в таблицу артефактов по организации.
    if role == Role.customer:
        return _redirect(f"/my/artifacts?org_id={selected_org_id}")

    # Auditor UI: отдельный экран (выбор организации + таблица артефактов, только чтение + комментарии).
    if role == Role.auditor and not user.is_admin:
        return _redirect(f"/auditor/artifacts?org_id={selected_org_id}")

    files = db.query(StoredFile).filter(StoredFile.org_id == selected_org_id).order_by(StoredFile.created_at.desc()).all()
    return templates.TemplateResponse(
        "files.html",
        {
            "request": request,
            "user": user,
            "orgs": orgs,
            "selected_org_id": selected_org_id,
            "role": role.value if role else None,
            "files": files,
            "max_upload_mb": settings.max_upload_mb,
        },
    )


def _ensure_org_artifacts_materialized_simple(db: Session, org_id: int) -> None:
    artifact_ids = [a_id for (a_id,) in db.query(Artifact.id).all()]
    if not artifact_ids:
        return
    existing_ids = set(
        a_id for (a_id,) in db.query(OrgArtifact.artifact_id).filter(OrgArtifact.org_id == org_id).all()
    )
    now = datetime.utcnow()
    for a_id in artifact_ids:
        if a_id in existing_ids:
            continue
        db.add(OrgArtifact(org_id=org_id, artifact_id=a_id, status=OrgArtifactStatus.missing, created_at=now, updated_at=now))
    db.flush()

def _ensure_org_artifacts_materialized(db: Session, org_id: int) -> None:
    # Быстрое заполнение org_artifacts для организации (если импортировали новые artifacts).
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


@router.get("/my/artifacts", response_class=HTMLResponse)
def my_artifacts_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    org_id: int | None = None,
    topic: str | None = None,
    domain: str | None = None,
    kb_level: str | None = None,
    short_name: str | None = None,
    q: str | None = None,
    status: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> HTMLResponse:
    # Доступные организации для пользователя (customer видит только свои).
    is_global_auditor = (
        db.query(UserOrgMembership)
        .filter(UserOrgMembership.user_id == user.id, UserOrgMembership.role == Role.auditor)
        .first()
        is not None
    )
    if user.is_admin or is_global_auditor:
        orgs = db.query(Organization).order_by(Organization.name.asc()).all()
    else:
        orgs = (
            db.query(Organization)
            .join(UserOrgMembership, UserOrgMembership.org_id == Organization.id)
            .filter(UserOrgMembership.user_id == user.id)
            .order_by(Organization.name.asc())
            .all()
        )
    if not orgs:
        return templates.TemplateResponse(
            "empty.html",
            {"request": request, "user": user, "message": "Нет доступных организаций. Обратитесь к администратору."},
        )

    # Customer: организация зафиксирована (параметр org_id игнорируем).
    selected_org_id = orgs[0].id
    role = get_user_role_for_org(db, user, selected_org_id)
    if role != Role.customer or len(orgs) != 1:
        # Если у пользователя больше одной организации — это уже не "customer" модель, пусть админ настроит роли.
        raise HTTPException(status_code=403, detail="Страница доступна только роли customer (одна организация)")

    page = max(int(page or 1), 1)
    page_size = int(page_size or 50)
    if page_size < 10:
        page_size = 10
    if page_size > 200:
        page_size = 200

    _ensure_org_artifacts_materialized(db, selected_org_id)
    db.commit()

    # базовые фильтры (без статуса) — используем и для табличного вывода, и для расчёта процента заполненности
    filters = [OrgArtifact.org_id == selected_org_id]
    if topic:
        filters.append(Artifact.topic == topic)
    if domain:
        filters.append(Artifact.domain == domain)
    if kb_level:
        filters.append(Artifact.kb_level == kb_level)
    if short_name:
        filters.append(Artifact.short_name == short_name)
    if q:
        like = f"%{q.strip()}%"
        filters.append(
            (Artifact.indicator_name.ilike(like))
            | (Artifact.title.ilike(like))
            | (Artifact.achievement_text.ilike(like))
            | (Artifact.achievement_item_text.ilike(like))
        )

    base_count_q = db.query(OrgArtifact.id).join(Artifact, Artifact.id == OrgArtifact.artifact_id).filter(*filters)

    completion_total = base_count_q.count()
    completion_uploaded = base_count_q.filter(OrgArtifact.status == OrgArtifactStatus.uploaded).count()
    completion_pct = int(round((completion_uploaded * 100.0 / completion_total), 0)) if completion_total else 0

    status_filter = (status or "").strip().lower() or ""
    if status_filter not in ("uploaded", "missing", ""):
        status_filter = ""

    list_count_q = base_count_q
    if status_filter:
        list_count_q = list_count_q.filter(OrgArtifact.status == OrgArtifactStatus(status_filter))
    total = list_count_q.count()

    CommentBy = aliased(User)
    # latest comment per org_artifact (auditor comment visible to customer)
    sub = (
        db.query(
            OrgArtifactComment.org_artifact_id.label("oa_id"),
            func.max(OrgArtifactComment.created_at).label("max_created_at"),
        )
        .filter(OrgArtifactComment.org_id == selected_org_id)
        .group_by(OrgArtifactComment.org_artifact_id)
        .subquery()
    )
    latest_comment = (
        db.query(OrgArtifactComment)
        .join(sub, and_(OrgArtifactComment.org_artifact_id == sub.c.oa_id, OrgArtifactComment.created_at == sub.c.max_created_at))
        .subquery()
    )

    query = (
        db.query(OrgArtifact, Artifact, FileVersion, latest_comment.c.comment_text, latest_comment.c.created_at, CommentBy)
        .join(Artifact, Artifact.id == OrgArtifact.artifact_id)
        .outerjoin(FileVersion, FileVersion.id == OrgArtifact.current_file_version_id)
        .outerjoin(latest_comment, latest_comment.c.org_artifact_id == OrgArtifact.id)
        .outerjoin(CommentBy, CommentBy.id == latest_comment.c.author_user_id)
        .filter(*filters)
        .order_by(Artifact.topic.asc(), Artifact.domain.asc(), Artifact.short_name.asc(), Artifact.achievement_item_no.asc().nullsfirst())
    )
    if status_filter:
        query = query.filter(OrgArtifact.status == OrgArtifactStatus(status_filter))

    offset = (page - 1) * page_size
    rows = []
    for (oa, a, fv, c_text, c_at, c_by) in query.offset(offset).limit(page_size).all():
        rows.append(
            {
                "oa": oa,
                "a": a,
                "fv": fv,
                "comment_text": c_text or "",
                "comment_at": c_at,
                "comment_by": c_by.login if c_by else "",
            }
        )

    topics = [t for (t,) in db.query(Artifact.topic).filter(Artifact.topic != "").distinct().order_by(Artifact.topic.asc()).all()]
    domains = [d for (d,) in db.query(Artifact.domain).filter(Artifact.domain != "").distinct().order_by(Artifact.domain.asc()).all()]
    kb_levels = [k for (k,) in db.query(Artifact.kb_level).filter(Artifact.kb_level != "").distinct().order_by(Artifact.kb_level.asc()).all()]

    total_pages = max((total + page_size - 1) // page_size, 1)
    if page > total_pages:
        page = total_pages
        offset = (page - 1) * page_size

    # Базовый querystring для пагинации (фильтры + page_size), без org_id.
    base_query = urlencode(
        {
            "topic": topic or "",
            "domain": domain or "",
            "kb_level": kb_level or "",
            "short_name": short_name or "",
            "q": q or "",
            "status": status_filter,
            "page_size": str(page_size),
        }
    )

    # Список страниц вокруг текущей (для кликабельных номеров).
    window = 3
    start = max(1, page - window)
    end = min(total_pages, page + window)
    page_links = list(range(start, end + 1))

    resp = templates.TemplateResponse(
        "customer_artifacts.html",
        {
            "request": request,
            "user": user,
            "container_class": "container-wide",
            "org_name": orgs[0].name,
            "selected_org_id": selected_org_id,
            "rows": rows,
            "max_upload_mb": settings.max_upload_mb,
            "topic": topic,
            "domain": domain,
            "kb_level": kb_level,
            "short_name": short_name,
            "q": q,
            "status": status_filter,
            "topics": topics,
            "domains": domains,
            "kb_levels": kb_levels,
            "completion_total": completion_total,
            "completion_uploaded": completion_uploaded,
            "completion_pct": completion_pct,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "has_prev": page > 1,
            "has_next": offset + page_size < total,
            "page_links": page_links,
            "base_query": base_query,
        },
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


def _get_customer_single_org(db: Session, user: User) -> Organization:
    orgs = (
        db.query(Organization)
        .join(UserOrgMembership, UserOrgMembership.org_id == Organization.id)
        .filter(UserOrgMembership.user_id == user.id)
        .order_by(Organization.name.asc())
        .all()
    )
    if not orgs:
        raise HTTPException(status_code=403, detail="Нет доступных организаций")
    if len(orgs) != 1:
        raise HTTPException(status_code=403, detail="Для customer ожидается ровно одна организация")
    role = get_user_role_for_org(db, user, orgs[0].id)
    if role != Role.customer:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    return orgs[0]


def _split_short_name(sn: str) -> list[str]:
    return [p.strip() for p in (sn or "").split(".") if p.strip()]


@router.get("/my/files", response_class=HTMLResponse)
def my_files_explorer(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    path: str | None = None,
) -> HTMLResponse:
    org = _get_customer_single_org(db, user)
    _ensure_org_artifacts_materialized(db, org.id)
    db.commit()

    # Берём все артефакты этой организации (MVP: до нескольких тысяч) и строим дерево путей по short_name.
    CreatedBy = aliased(User)
    UpdatedBy = aliased(User)
    rows = (
        db.query(OrgArtifact, Artifact, FileVersion, CreatedBy, UpdatedBy)
        .join(Artifact, Artifact.id == OrgArtifact.artifact_id)
        .outerjoin(FileVersion, FileVersion.id == OrgArtifact.current_file_version_id)
        .outerjoin(CreatedBy, CreatedBy.id == FileVersion.created_by_user_id)
        .outerjoin(UpdatedBy, UpdatedBy.id == OrgArtifact.updated_by_user_id)
        .filter(OrgArtifact.org_id == org.id)
        .all()
    )

    # Нормализуем path: "ВССТ/КМНК/1" -> ["ВССТ","КМНК","1"]
    path = (path or "").strip().strip("/")
    cur_segments = [p for p in path.split("/") if p] if path else []

    # Вычисляем "детей" текущей директории.
    subfolders: dict[str, int] = {}
    leaf_items: list[dict] = []
    for (oa, a, fv, created_by, updated_by) in rows:
        segs = _split_short_name(a.short_name)
        if not segs:
            continue
        if segs[: len(cur_segments)] != cur_segments:
            continue
        if len(segs) > len(cur_segments):
            nxt = segs[len(cur_segments)]
            subfolders[nxt] = subfolders.get(nxt, 0) + 1
        else:
            leaf_items.append(
                {
                    "oa": oa,
                    "a": a,
                    "fv": fv,
                    "uploaded_by": created_by.login if created_by else "",
                    "updated_by": updated_by.login if updated_by else "",
                }
            )

    # Сортировка папок/листов
    folders_sorted = sorted(subfolders.items(), key=lambda x: x[0])
    leaf_items.sort(key=lambda r: (r["a"].short_name, r["a"].achievement_item_no or 0))

    # Breadcrumbs
    crumbs = []
    acc = []
    for s in cur_segments:
        acc.append(s)
        crumbs.append({"name": s, "path": "/".join(acc)})

    resp = templates.TemplateResponse(
        "customer_files.html",
        {
            "request": request,
            "user": user,
            "container_class": "container-wide",
            "org_name": org.name,
            "path": path,
            "crumbs": crumbs,
            "folders": folders_sorted,
            "leaf_items": leaf_items,
            "max_upload_mb": settings.max_upload_mb,
        },
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


def _get_accessible_orgs_for_auditor(db: Session, user: User) -> list[Organization]:
    if user.is_admin:
        return db.query(Organization).order_by(Organization.name.asc()).all()
    # По текущему MVP правилу auditor считается глобальным, если есть хотя бы один membership auditor.
    is_global_auditor = (
        db.query(UserOrgMembership)
        .filter(UserOrgMembership.user_id == user.id, UserOrgMembership.role == Role.auditor)
        .first()
        is not None
    )
    if is_global_auditor:
        return db.query(Organization).order_by(Organization.name.asc()).all()
    # fallback: только свои (на случай, если правила изменятся)
    return (
        db.query(Organization)
        .join(UserOrgMembership, UserOrgMembership.org_id == Organization.id)
        .filter(UserOrgMembership.user_id == user.id)
        .order_by(Organization.name.asc())
        .all()
    )


@router.get("/auditor/artifacts", response_class=HTMLResponse)
def auditor_artifacts_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    org_id: int | None = None,
    topic: str | None = None,
    domain: str | None = None,
    kb_level: str | None = None,
    short_name: str | None = None,
    q: str | None = None,
    status: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> HTMLResponse:
    # только auditor/admin
    orgs = _get_accessible_orgs_for_auditor(db, user)
    if not orgs:
        return templates.TemplateResponse(
            "empty.html",
            {"request": request, "user": user, "message": "Нет доступных организаций. Обратитесь к администратору."},
        )
    selected_org_id = org_id or orgs[0].id
    role = get_user_role_for_org(db, user, selected_org_id)
    if role not in (Role.auditor, Role.admin):
        raise HTTPException(status_code=403, detail="Требуются права auditor/admin")
    can_delete_files = role == Role.admin
    current_url = request.url.path + (f"?{request.url.query}" if request.url.query else "")

    page = max(int(page or 1), 1)
    page_size = int(page_size or 50)
    if page_size < 10:
        page_size = 10
    if page_size > 200:
        page_size = 200

    _ensure_org_artifacts_materialized(db, selected_org_id)
    db.commit()

    CreatedBy = aliased(User)
    UpdatedBy = aliased(User)
    CommentBy = aliased(User)

    # latest comment per org_artifact
    sub = (
        db.query(
            OrgArtifactComment.org_artifact_id.label("oa_id"),
            func.max(OrgArtifactComment.created_at).label("max_created_at"),
        )
        .filter(OrgArtifactComment.org_id == selected_org_id)
        .group_by(OrgArtifactComment.org_artifact_id)
        .subquery()
    )
    latest_comment = (
        db.query(OrgArtifactComment)
        .join(sub, and_(OrgArtifactComment.org_artifact_id == sub.c.oa_id, OrgArtifactComment.created_at == sub.c.max_created_at))
        .subquery()
    )

    filters = [OrgArtifact.org_id == selected_org_id]
    if topic:
        filters.append(Artifact.topic == topic)
    if domain:
        filters.append(Artifact.domain == domain)
    if kb_level:
        filters.append(Artifact.kb_level == kb_level)
    if short_name:
        filters.append(Artifact.short_name == short_name)
    if q:
        like = f"%{q.strip()}%"
        filters.append(
            (Artifact.indicator_name.ilike(like))
            | (Artifact.title.ilike(like))
            | (Artifact.achievement_text.ilike(like))
            | (Artifact.achievement_item_text.ilike(like))
        )

    base_count_q = db.query(OrgArtifact.id).join(Artifact, Artifact.id == OrgArtifact.artifact_id).filter(*filters)
    completion_total = base_count_q.count()
    completion_uploaded = base_count_q.filter(OrgArtifact.status == OrgArtifactStatus.uploaded).count()
    completion_pct = int(round((completion_uploaded * 100.0 / completion_total), 0)) if completion_total else 0

    status_filter = (status or "").strip().lower() or ""
    if status_filter not in ("uploaded", "missing", ""):
        status_filter = ""

    list_count_q = base_count_q
    if status_filter:
        list_count_q = list_count_q.filter(OrgArtifact.status == OrgArtifactStatus(status_filter))
    total = list_count_q.count()

    query = (
        db.query(OrgArtifact, Artifact, FileVersion, CreatedBy, UpdatedBy, latest_comment.c.comment_text, latest_comment.c.created_at, CommentBy)
        .join(Artifact, Artifact.id == OrgArtifact.artifact_id)
        .outerjoin(FileVersion, FileVersion.id == OrgArtifact.current_file_version_id)
        .outerjoin(CreatedBy, CreatedBy.id == FileVersion.created_by_user_id)
        .outerjoin(UpdatedBy, UpdatedBy.id == OrgArtifact.updated_by_user_id)
        .outerjoin(latest_comment, latest_comment.c.org_artifact_id == OrgArtifact.id)
        .outerjoin(CommentBy, CommentBy.id == latest_comment.c.author_user_id)
        .filter(*filters)
        .order_by(Artifact.topic.asc(), Artifact.domain.asc(), Artifact.short_name.asc(), Artifact.achievement_item_no.asc().nullsfirst())
    )
    if status_filter:
        query = query.filter(OrgArtifact.status == OrgArtifactStatus(status_filter))

    offset = (page - 1) * page_size
    rows = []
    for (oa, a, fv, created_by, updated_by, c_text, c_at, c_by) in query.offset(offset).limit(page_size).all():
        rows.append(
            {
                "oa": oa,
                "a": a,
                "fv": fv,
                "uploaded_by": created_by.login if created_by else "",
                "updated_by": updated_by.login if updated_by else "",
                "comment_text": c_text or "",
                "comment_at": c_at,
                "comment_by": c_by.login if c_by else "",
            }
        )

    topics = [t for (t,) in db.query(Artifact.topic).filter(Artifact.topic != "").distinct().order_by(Artifact.topic.asc()).all()]
    domains = [d for (d,) in db.query(Artifact.domain).filter(Artifact.domain != "").distinct().order_by(Artifact.domain.asc()).all()]
    kb_levels = [k for (k,) in db.query(Artifact.kb_level).filter(Artifact.kb_level != "").distinct().order_by(Artifact.kb_level.asc()).all()]

    total_pages = max((total + page_size - 1) // page_size, 1)
    base_query = urlencode(
        {
            "org_id": str(selected_org_id),
            "topic": topic or "",
            "domain": domain or "",
            "kb_level": kb_level or "",
            "short_name": short_name or "",
            "q": q or "",
            "status": status_filter,
            "page_size": str(page_size),
        }
    )
    export_query = urlencode(
        {
            "org_id": str(selected_org_id),
            "topic": topic or "",
            "domain": domain or "",
            "kb_level": kb_level or "",
            "short_name": short_name or "",
            "q": q or "",
            "status": status_filter,
        }
    )
    window = 3
    start = max(1, page - window)
    end = min(total_pages, page + window)
    page_links = list(range(start, end + 1))

    resp = templates.TemplateResponse(
        "auditor_artifacts.html",
        {
            "request": request,
            "user": user,
            "container_class": "container-wide",
            "orgs": orgs,
            "selected_org_id": selected_org_id,
            "can_delete_files": can_delete_files,
            "current_url": current_url,
            "rows": rows,
            "topic": topic,
            "domain": domain,
            "kb_level": kb_level,
            "short_name": short_name,
            "q": q,
            "status": status_filter,
            "topics": topics,
            "domains": domains,
            "kb_levels": kb_levels,
            "completion_total": completion_total,
            "completion_uploaded": completion_uploaded,
            "completion_pct": completion_pct,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "has_prev": page > 1,
            "has_next": offset + page_size < total,
            "base_query": base_query,
            "page_links": page_links,
            "export_query": export_query,
        },
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@router.post("/auditor/org_artifacts/{org_artifact_id}/comment")
def auditor_add_comment(
    org_artifact_id: int,
    request: Request,
    org_id: int = Form(...),
    comment: str = Form(""),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
) -> Response:
    role = get_user_role_for_org(db, user, org_id)
    if role not in (Role.auditor, Role.admin):
        raise HTTPException(status_code=403, detail="Требуются права auditor/admin")
    oa = db.get(OrgArtifact, org_artifact_id)
    if not oa or oa.org_id != org_id:
        raise HTTPException(status_code=404, detail="Артефакт организации не найден")

    txt = (comment or "").strip()
    if txt:
        db.add(OrgArtifactComment(org_id=org_id, org_artifact_id=oa.id, author_user_id=user.id, comment_text=txt))
        write_audit_log(
            db,
            actor=user,
            org_id=org_id,
            action="comment",
            entity_type="org_artifact",
            entity_id=str(oa.id),
            after={"comment": txt},
            request=request,
        )
        db.commit()

    ref = request.headers.get("referer") or f"/auditor/artifacts?org_id={org_id}"
    # безопасный редирект только на относительный путь
    if "://" in ref:
        ref = f"/auditor/artifacts?org_id={org_id}"
    return _redirect(ref)


@router.get("/auditor/artifacts/export.xlsx")
def auditor_artifacts_export_xlsx(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    org_id: int = 0,
    topic: str | None = None,
    domain: str | None = None,
    kb_level: str | None = None,
    short_name: str | None = None,
    q: str | None = None,
    status: str | None = None,
) -> Response:
    if not org_id:
        raise HTTPException(status_code=400, detail="org_id обязателен")
    role = get_user_role_for_org(db, user, org_id)
    if role not in (Role.auditor, Role.admin):
        raise HTTPException(status_code=403, detail="Требуются права auditor/admin")

    _ensure_org_artifacts_materialized(db, org_id)
    db.commit()

    CreatedBy = aliased(User)
    UpdatedBy = aliased(User)
    CommentBy = aliased(User)

    sub = (
        db.query(
            OrgArtifactComment.org_artifact_id.label("oa_id"),
            func.max(OrgArtifactComment.created_at).label("max_created_at"),
        )
        .filter(OrgArtifactComment.org_id == org_id)
        .group_by(OrgArtifactComment.org_artifact_id)
        .subquery()
    )
    latest_comment = (
        db.query(OrgArtifactComment)
        .join(sub, and_(OrgArtifactComment.org_artifact_id == sub.c.oa_id, OrgArtifactComment.created_at == sub.c.max_created_at))
        .subquery()
    )

    query = (
        db.query(
            OrgArtifact,
            Artifact,
            FileVersion,
            CreatedBy,
            UpdatedBy,
            latest_comment.c.comment_text,
            latest_comment.c.created_at,
            CommentBy,
        )
        .join(Artifact, Artifact.id == OrgArtifact.artifact_id)
        .outerjoin(FileVersion, FileVersion.id == OrgArtifact.current_file_version_id)
        .outerjoin(CreatedBy, CreatedBy.id == FileVersion.created_by_user_id)
        .outerjoin(UpdatedBy, UpdatedBy.id == OrgArtifact.updated_by_user_id)
        .outerjoin(latest_comment, latest_comment.c.org_artifact_id == OrgArtifact.id)
        .outerjoin(CommentBy, CommentBy.id == latest_comment.c.author_user_id)
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

    status_filter = (status or "").strip().lower() or ""
    if status_filter not in ("uploaded", "missing", ""):
        status_filter = ""
    if status_filter:
        query = query.filter(OrgArtifact.status == OrgArtifactStatus(status_filter))

    org = db.get(Organization, org_id)
    org_name = org.name if org else f"org_{org_id}"

    wb = Workbook()
    ws = wb.active
    ws.title = "Artifacts"
    headers = [
        "Тематика",
        "Домен",
        "Показатель",
        "Сокращенное",
        "Пункт",
        "КБ",
        "Артефакт",
        "Статус",
        "Файл",
        "Дата загрузки",
        "Кто загрузил",
        "Дата изменения",
        "Кто изменил",
        "Комментарий",
        "Комментарий (кто)",
        "Комментарий (когда)",
    ]
    ws.append(headers)
    ws.freeze_panes = "A2"

    def fmt_dt(dt: datetime | None) -> str:
        return dt.isoformat(sep=" ", timespec="seconds") if dt else ""

    for (oa, a, fv, created_by, updated_by, c_text, c_at, c_by) in query.all():
        ws.append(
            [
                a.topic,
                a.domain,
                a.indicator_name,
                a.short_name,
                a.achievement_item_no or "",
                a.kb_level,
                a.title,
                oa.status.value,
                fv.original_filename if fv else "",
                fmt_dt(fv.created_at if fv else None),
                created_by.login if created_by else "",
                fmt_dt(oa.updated_at),
                updated_by.login if updated_by else "",
                (c_text or ""),
                (c_by.login if c_by else ""),
                fmt_dt(c_at),
            ]
        )

    # Простая автоподгонка ширин
    for idx, _ in enumerate(headers, start=1):
        col = ws.column_dimensions[ws.cell(row=1, column=idx).column_letter]
        col.width = min(max(12, len(str(headers[idx - 1])) + 2), 40)

    buf = io.BytesIO()
    wb.save(buf)
    content = buf.getvalue()

    # Starlette кодирует заголовки как latin-1, поэтому filename должен быть ASCII.
    # Для удобства добавляем RFC5987 filename* (UTF-8, percent-encoded).
    date_str = datetime.utcnow().date().isoformat()
    filename_ascii = f"audit_org{org_id}_{date_str}.xlsx"
    filename_utf8 = f"audit_{org_name}_{date_str}.xlsx"
    write_audit_log(
        db,
        actor=user,
        org_id=org_id,
        action="export_xlsx",
        entity_type="org",
        entity_id=str(org_id),
        after={"filename": filename_utf8, "filename_ascii": filename_ascii},
        request=request,
    )
    db.commit()

    cd = f"attachment; filename=\"{filename_ascii}\"; filename*=UTF-8''{quote(filename_utf8)}"
    headers_resp = {"Content-Disposition": cd}
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers_resp,
    )


def _require_auditor_or_admin_for_org(db: Session, user: User, org_id: int) -> None:
    role = get_user_role_for_org(db, user, org_id)
    if role not in (Role.auditor, Role.admin):
        raise HTTPException(status_code=403, detail="Требуются права auditor/admin")


@router.get("/auditor/files", response_class=HTMLResponse)
def auditor_files_explorer(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    org_id: int | None = None,
    path: str | None = None,
) -> HTMLResponse:
    orgs = _get_accessible_orgs_for_auditor(db, user)
    if not orgs:
        return templates.TemplateResponse(
            "empty.html",
            {"request": request, "user": user, "message": "Нет доступных организаций. Обратитесь к администратору."},
        )

    selected_org_id = org_id or orgs[0].id
    _require_auditor_or_admin_for_org(db, user, selected_org_id)
    role = get_user_role_for_org(db, user, selected_org_id)
    can_delete_files = role == Role.admin
    current_url = request.url.path + (f"?{request.url.query}" if request.url.query else "")

    _ensure_org_artifacts_materialized(db, selected_org_id)
    db.commit()

    CreatedBy = aliased(User)
    UpdatedBy = aliased(User)
    rows = (
        db.query(OrgArtifact, Artifact, FileVersion, CreatedBy, UpdatedBy)
        .join(Artifact, Artifact.id == OrgArtifact.artifact_id)
        .outerjoin(FileVersion, FileVersion.id == OrgArtifact.current_file_version_id)
        .outerjoin(CreatedBy, CreatedBy.id == FileVersion.created_by_user_id)
        .outerjoin(UpdatedBy, UpdatedBy.id == OrgArtifact.updated_by_user_id)
        .filter(OrgArtifact.org_id == selected_org_id)
        .all()
    )

    path = (path or "").strip().strip("/")
    cur_segments = [p for p in path.split("/") if p] if path else []

    subfolders: dict[str, int] = {}
    leaf_items: list[dict] = []
    for (oa, a, fv, created_by, updated_by) in rows:
        segs = _split_short_name(a.short_name)
        if not segs:
            continue
        if segs[: len(cur_segments)] != cur_segments:
            continue
        if len(segs) > len(cur_segments):
            nxt = segs[len(cur_segments)]
            subfolders[nxt] = subfolders.get(nxt, 0) + 1
        else:
            leaf_items.append(
                {
                    "oa": oa,
                    "a": a,
                    "fv": fv,
                    "uploaded_by": created_by.login if created_by else "",
                    "updated_by": updated_by.login if updated_by else "",
                }
            )

    folders_sorted = sorted(subfolders.items(), key=lambda x: x[0])
    leaf_items.sort(key=lambda r: (r["a"].short_name, r["a"].achievement_item_no or 0))

    crumbs = []
    acc = []
    for s in cur_segments:
        acc.append(s)
        crumbs.append({"name": s, "path": "/".join(acc)})

    resp = templates.TemplateResponse(
        "auditor_files.html",
        {
            "request": request,
            "user": user,
            "container_class": "container-wide",
            "orgs": orgs,
            "selected_org_id": selected_org_id,
            "can_delete_files": can_delete_files,
            "current_url": current_url,
            "path": path,
            "crumbs": crumbs,
            "folders": folders_sorted,
            "leaf_items": leaf_items,
        },
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@router.get("/auditor/index-kb", response_class=HTMLResponse)
def auditor_index_kb_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    org_id: int | None = None,
    sheet: str | None = None,
    q: str | None = None,
) -> HTMLResponse:
    orgs_all = _get_accessible_orgs_for_auditor(db, user)
    # Default — служебная организация, в UI Индекс КБ не показываем, если есть другие.
    orgs = [o for o in orgs_all if o.name != "Default"] or orgs_all
    if not orgs:
        return templates.TemplateResponse(
            "empty.html",
            {"request": request, "user": user, "message": "Нет доступных организаций. Обратитесь к администратору."},
        )

    selected_org_id = org_id or orgs[0].id
    _require_auditor_or_admin_for_org(db, user, selected_org_id)

    template_path = settings.index_kb_template_path
    if not template_path or not os.path.exists(template_path):
        resp = templates.TemplateResponse(
            "auditor_index_kb.html",
            {
                "request": request,
                "user": user,
                "container_class": "container-wide",
                "orgs": orgs,
                "selected_org_id": selected_org_id,
                "template_path": template_path,
                "error": "Не найден эталонный шаблон Индекс КБ (.xlsx).",
            },
            status_code=200,
        )
        resp.headers["Cache-Control"] = "no-store, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        return resp

    tpl = get_index_kb_template(template_path)
    sheet_names = tpl.sheet_names

    resp = templates.TemplateResponse(
        "auditor_index_kb.html",
        {
            "request": request,
            "user": user,
            "container_class": "container-wide",
            "orgs": orgs,
            "selected_org_id": selected_org_id,
            "sheet_names": sheet_names,
            "template_path": template_path,
            "error": None,
        },
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@router.get("/auditor/index-kb/uib", response_class=HTMLResponse)
def auditor_index_kb_uib_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    org_id: int | None = None,
) -> HTMLResponse:
    orgs_all = _get_accessible_orgs_for_auditor(db, user)
    orgs = [o for o in orgs_all if o.name != "Default"] or orgs_all
    if not orgs:
        return templates.TemplateResponse(
            "empty.html",
            {"request": request, "user": user, "message": "Нет доступных организаций. Обратитесь к администратору."},
        )
    selected_org_id = org_id or orgs[0].id
    _require_auditor_or_admin_for_org(db, user, selected_org_id)

    template_path = settings.index_kb_template_path
    if not template_path or not os.path.exists(template_path):
        resp = templates.TemplateResponse(
            "auditor_index_kb_uib.html",
            {
                "request": request,
                "user": user,
                "container_class": "container-wide",
                "orgs": orgs,
                "selected_org_id": selected_org_id,
                "template_path": template_path,
                "error": "Не найден эталонный шаблон Индекс КБ (.xlsx).",
                "rows": [],
            },
            status_code=200,
        )
        resp.headers["Cache-Control"] = "no-store, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        return resp

    org, tpl, rows = build_uib_view(db, org_id=selected_org_id, template_path=template_path, actor=user)
    from app.index_kb.uib_sheet import compute_uib_summary

    summary_rows = compute_uib_summary(rows)
    resp = templates.TemplateResponse(
        "auditor_index_kb_uib.html",
        {
            "request": request,
            "user": user,
            "container_class": "container-wide",
            "orgs": orgs,
            "selected_org_id": selected_org_id,
            "org": org,
            "sheet_name": UIB_SHEET_NAME,
            "rows": rows,
            "summary_rows": summary_rows,
            "error": None,
        },
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@router.post("/auditor/index-kb/uib/manual")
def auditor_index_kb_uib_save_manual(
    request: Request,
    org_id: int = Form(...),
    row_key: str = Form(...),
    value: str = Form(""),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
) -> Response:
    _require_auditor_or_admin_for_org(db, user, org_id)
    try:
        v = float((value or "").strip().replace(",", "."))
    except Exception:
        v = 0.0
    upsert_manual_value(db, org_id=org_id, sheet_name=UIB_SHEET_NAME, row_key=row_key, value=v, actor=user)
    db.commit()
    ref = request.headers.get("referer") or f"/auditor/index-kb/uib?org_id={org_id}"
    return _redirect(ref)


@router.get("/auditor/org_artifacts/{org_artifact_id}/download")
def auditor_download_current_file(
    org_artifact_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    version: int | None = None,
) -> Response:
    oa = db.get(OrgArtifact, org_artifact_id)
    if not oa:
        raise HTTPException(status_code=404, detail="Артефакт организации не найден")
    role = get_user_role_for_org(db, user, oa.org_id)
    if role not in (Role.auditor, Role.admin):
        raise HTTPException(status_code=403, detail="Требуются права auditor/admin")
    # История версий доступна только админу: auditor видит/скачивает только текущую версию
    if version is not None and role != Role.admin:
        raise HTTPException(status_code=403, detail="История версий доступна только админу")

    qv = db.query(FileVersion).filter(FileVersion.org_artifact_id == oa.id)
    if version is not None:
        fv = qv.filter(FileVersion.version_no == version).one_or_none()
    else:
        fv = db.get(FileVersion, oa.current_file_version_id) if oa.current_file_version_id else qv.order_by(FileVersion.version_no.desc()).first()
    if not fv or not fv.blob:
        raise HTTPException(status_code=404, detail="Файл не найден")

    headers = {"Content-Disposition": f'attachment; filename="{fv.original_filename}"'}
    return Response(content=fv.blob, media_type=fv.content_type, headers=headers)


@router.post("/auditor/org_artifacts/{org_artifact_id}/delete")
def admin_delete_current_file_for_org_artifact(
    org_artifact_id: int,
    request: Request,
    back: str = Form(""),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
) -> Response:
    oa = db.get(OrgArtifact, org_artifact_id)
    if not oa:
        raise HTTPException(status_code=404, detail="Артефакт организации не найден")
    role = get_user_role_for_org(db, user, oa.org_id)
    if role != Role.admin:
        raise HTTPException(status_code=403, detail="Требуются права admin")

    before = {"status": oa.status.value, "current_file_version_id": oa.current_file_version_id}

    fv = db.get(FileVersion, oa.current_file_version_id) if oa.current_file_version_id else None
    if fv and (fv.storage_key or "").startswith("nextcloud:"):
        remote_path = (fv.storage_key or "")[len("nextcloud:") :].strip()
        if remote_path:
            # allow re-import on next sync after manual delete
            db.query(NextcloudRemoteFileState).filter(
                NextcloudRemoteFileState.org_id == oa.org_id,
                NextcloudRemoteFileState.remote_path == remote_path,
            ).delete(synchronize_session=False)

    oa.current_file_version_id = None
    oa.status = OrgArtifactStatus.missing
    oa.updated_at = datetime.utcnow()
    oa.updated_by_user_id = user.id
    after = {"status": oa.status.value, "current_file_version_id": oa.current_file_version_id}

    write_audit_log(
        db,
        actor=user,
        org_id=oa.org_id,
        action="delete_file",
        entity_type="org_artifact",
        entity_id=str(oa.id),
        before=before,
        after=after,
        request=request,
    )
    db.commit()

    # Prefer explicit back (sent by form), fallback to referer.
    ref = (back or "").strip() or (request.headers.get("referer") or "")
    if not ref or "://" in ref or not ref.startswith("/"):
        ref = f"/auditor/files?org_id={oa.org_id}"
    return _redirect(ref)


@router.get("/admin/org_artifacts/{org_artifact_id}/versions", response_class=HTMLResponse)
def admin_org_artifact_versions_page(
    org_artifact_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
    back: str | None = None,
) -> HTMLResponse:
    oa = db.get(OrgArtifact, org_artifact_id)
    if not oa:
        raise HTTPException(status_code=404, detail="Артефакт организации не найден")
    a = db.get(Artifact, oa.artifact_id)
    org = db.get(Organization, oa.org_id)
    if not a or not org:
        raise HTTPException(status_code=404, detail="Данные не найдены")

    CreatedBy = aliased(User)
    versions = (
        db.query(FileVersion, CreatedBy)
        .outerjoin(CreatedBy, CreatedBy.id == FileVersion.created_by_user_id)
        .filter(FileVersion.org_artifact_id == oa.id)
        .order_by(FileVersion.version_no.desc())
        .all()
    )
    rows = [
        {
            "id": fv.id,
            "version_no": fv.version_no,
            "original_filename": fv.original_filename,
            "created_at": fv.created_at,
            "created_by_login": created_by.login if created_by else "",
        }
        for (fv, created_by) in versions
    ]

    # safe back url: allow only local paths
    back_url = back or (request.headers.get("referer") or f"/auditor/files?org_id={oa.org_id}")
    if "://" in back_url:
        back_url = f"/auditor/files?org_id={oa.org_id}"

    resp = templates.TemplateResponse(
        "admin/org_artifact_versions.html",
        {"request": request, "user": user, "oa": oa, "a": a, "org": org, "versions": rows, "back_url": back_url},
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@router.post("/my/artifacts/{org_artifact_id}/upload")
def my_artifacts_upload(
    org_artifact_id: int,
    request: Request,
    upload: UploadFile = File(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
) -> Response:
    oa = db.get(OrgArtifact, org_artifact_id)
    if not oa:
        raise HTTPException(status_code=404, detail="Артефакт организации не найден")
    role = get_user_role_for_org(db, user, oa.org_id)
    if role != Role.customer:
        raise HTTPException(status_code=403, detail="Недостаточно прав")

    content = upload.file.read()
    size_bytes = len(content)
    if size_bytes > settings.max_upload_mb * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"Файл слишком большой. Лимит {settings.max_upload_mb} МБ")
    sha256 = hashlib.sha256(content).hexdigest()

    current_max = db.query(func.max(FileVersion.version_no)).filter(FileVersion.org_artifact_id == oa.id).scalar() or 0
    fv = FileVersion(
        org_artifact_id=oa.id,
        version_no=int(current_max) + 1,
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

    before = {"status": oa.status.value, "current_file_version_id": oa.current_file_version_id}
    oa.status = OrgArtifactStatus.uploaded
    oa.current_file_version_id = fv.id
    oa.updated_at = datetime.utcnow()
    oa.updated_by_user_id = user.id
    after = {"status": oa.status.value, "current_file_version_id": oa.current_file_version_id}

    write_audit_log(
        db,
        actor=user,
        org_id=oa.org_id,
        action="upload",
        entity_type="org_artifact",
        entity_id=str(oa.id),
        before=before,
        after=after,
        request=request,
    )
    db.commit()
    return _redirect(f"/my/artifacts")


@router.get("/my/artifacts/{org_artifact_id}/download")
def my_artifacts_download(
    org_artifact_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    version: int | None = None,
) -> Response:
    oa = db.get(OrgArtifact, org_artifact_id)
    if not oa:
        raise HTTPException(status_code=404, detail="Артефакт организации не найден")
    role = get_user_role_for_org(db, user, oa.org_id)
    if role != Role.customer:
        raise HTTPException(status_code=403, detail="Недостаточно прав")
    # Заказчик видит/скачивает только текущую версию
    if version is not None:
        raise HTTPException(status_code=403, detail="История версий недоступна")

    qv = db.query(FileVersion).filter(FileVersion.org_artifact_id == oa.id)
    if version is not None:
        fv = qv.filter(FileVersion.version_no == version).one_or_none()
    else:
        fv = db.get(FileVersion, oa.current_file_version_id) if oa.current_file_version_id else qv.order_by(FileVersion.version_no.desc()).first()
    if not fv or not fv.blob:
        raise HTTPException(status_code=404, detail="Файл не найден")

    headers = {"Content-Disposition": f'attachment; filename="{fv.original_filename}"'}
    return Response(content=fv.blob, media_type=fv.content_type, headers=headers)


@router.post("/my/artifacts/{org_artifact_id}/delete")
def my_artifacts_delete(
    org_artifact_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
) -> Response:
    oa = db.get(OrgArtifact, org_artifact_id)
    if not oa:
        raise HTTPException(status_code=404, detail="Артефакт организации не найден")
    role = get_user_role_for_org(db, user, oa.org_id)
    if role != Role.customer:
        raise HTTPException(status_code=403, detail="Недостаточно прав")

    before = {"status": oa.status.value, "current_file_version_id": oa.current_file_version_id}
    oa.current_file_version_id = None
    oa.status = OrgArtifactStatus.missing
    oa.updated_at = datetime.utcnow()
    oa.updated_by_user_id = user.id
    after = {"status": oa.status.value, "current_file_version_id": oa.current_file_version_id}

    write_audit_log(
        db,
        actor=user,
        org_id=oa.org_id,
        action="delete_file",
        entity_type="org_artifact",
        entity_id=str(oa.id),
        before=before,
        after=after,
        request=request,
    )
    db.commit()
    return _redirect("/my/artifacts")

def _require_admin_or_global_auditor(db: Session, user: User) -> None:
    if user.is_admin:
        return
    is_global_auditor = (
        db.query(UserOrgMembership)
        .filter(UserOrgMembership.user_id == user.id, UserOrgMembership.role == Role.auditor)
        .first()
        is not None
    )
    if not is_global_auditor:
        raise HTTPException(status_code=403, detail="Требуются права admin или auditor")


@router.get("/artifacts", response_class=HTMLResponse)
def artifacts_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
    topic: str | None = None,
    domain: str | None = None,
    kb_level: str | None = None,
    short_name: str | None = None,
    q: str | None = None,
) -> HTMLResponse:
    # Справочник артефактов: только admin/auditor. Здесь ничего не загружаем.
    _require_admin_or_global_auditor(db, user)

    query = (
        db.query(Artifact)
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

    artifacts = query.limit(2000).all()
    rows = artifacts

    # Сколько файлов "требуется" по short_name: если есть пункты 1./2., это количество строк.
    counts = dict(db.query(Artifact.short_name, func.count(Artifact.id)).group_by(Artifact.short_name).all())

    topics = [t for (t,) in db.query(Artifact.topic).filter(Artifact.topic != "").distinct().order_by(Artifact.topic.asc()).all()]
    domains = [d for (d,) in db.query(Artifact.domain).filter(Artifact.domain != "").distinct().order_by(Artifact.domain.asc()).all()]
    kb_levels = [k for (k,) in db.query(Artifact.kb_level).filter(Artifact.kb_level != "").distinct().order_by(Artifact.kb_level.asc()).all()]

    resp = templates.TemplateResponse(
        "artifacts.html",
        {
            "request": request,
            "user": user,
            "container_class": "container-wide",
            "rows": rows,
            "topic": topic,
            "domain": domain,
            "kb_level": kb_level,
            "short_name": short_name,
            "q": q,
            "topics": topics,
            "domains": domains,
            "kb_levels": kb_levels,
            "required_counts": counts,
        },
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp

@router.get("/artifacts/{artifact_id}/edit", response_class=HTMLResponse)
def artifact_edit_page(
    artifact_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
) -> HTMLResponse:
    _require_admin_or_global_auditor(db, user)
    a = db.get(Artifact, artifact_id)
    if not a:
        raise HTTPException(status_code=404, detail="Артефакт не найден")
    resp = templates.TemplateResponse(
        "artifact_edit.html",
        {"request": request, "user": user, "a": a, "error": None, "container_class": "container-wide"},
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@router.post("/artifacts/{artifact_id}/edit")
def artifact_edit_save(
    artifact_id: int,
    request: Request,
    topic: str = Form(""),
    domain: str = Form(""),
    kb_level: str = Form(""),
    indicator_name: str = Form(""),
    title: str = Form(""),
    achievement_text: str = Form(""),
    achievement_item_text: str = Form(""),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
) -> Response:
    _require_admin_or_global_auditor(db, user)
    a = db.get(Artifact, artifact_id)
    if not a:
        raise HTTPException(status_code=404, detail="Артефакт не найден")

    before = {
        "topic": a.topic,
        "domain": a.domain,
        "kb_level": a.kb_level,
        "indicator_name": a.indicator_name,
        "title": a.title,
        "achievement_text": a.achievement_text,
        "achievement_item_text": a.achievement_item_text,
    }
    a.topic = (topic or "").strip()
    a.domain = (domain or "").strip()
    a.kb_level = (kb_level or "").strip()
    a.indicator_name = (indicator_name or "").strip()
    a.title = (title or "").strip()
    a.achievement_text = (achievement_text or "").strip()
    a.achievement_item_text = (achievement_item_text or "").strip()

    write_audit_log(
        db,
        actor=user,
        org_id=None,
        action="update",
        entity_type="artifact",
        entity_id=str(a.id),
        before=before,
        after={
            "topic": a.topic,
            "domain": a.domain,
            "kb_level": a.kb_level,
            "indicator_name": a.indicator_name,
            "title": a.title,
            "achievement_text": a.achievement_text,
            "achievement_item_text": a.achievement_item_text,
        },
        request=request,
    )
    db.commit()
    return _redirect("/artifacts")


@router.post("/files/upload")
def upload_file(
    org_id: int = Form(...),
    note: str = Form(""),
    upload: UploadFile = File(...),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
) -> Response:
    role = get_user_role_for_org(db, user, org_id)
    if not role:
        raise HTTPException(status_code=403, detail="Нет доступа к организации")

    content = upload.file.read()
    size_bytes = len(content)
    if size_bytes > settings.max_upload_mb * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"Файл слишком большой. Лимит {settings.max_upload_mb} МБ")

    sha256 = hashlib.sha256(content).hexdigest()
    stored = StoredFile(
        org_id=org_id,
        original_filename=upload.filename or "file",
        content_type=upload.content_type or "application/octet-stream",
        size_bytes=size_bytes,
        sha256=sha256,
        blob=content,
        created_at=datetime.utcnow(),
        created_by_user_id=user.id,
        note=note or "",
    )
    db.add(stored)
    db.commit()

    return _redirect(f"/?org_id={org_id}")


@router.get("/files/{file_id}/download")
def download_file(
    file_id: int,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
) -> Response:
    stored = db.get(StoredFile, file_id)
    if not stored:
        raise HTTPException(status_code=404, detail="Файл не найден")
    role = get_user_role_for_org(db, user, stored.org_id)
    if not role:
        raise HTTPException(status_code=403, detail="Нет доступа к организации")

    headers = {"Content-Disposition": f'attachment; filename="{stored.original_filename}"'}
    return Response(content=stored.blob, media_type=stored.content_type, headers=headers)


@router.get("/admin", response_class=HTMLResponse)
def admin_index(request: Request, user: User = Depends(require_admin)) -> HTMLResponse:
    return templates.TemplateResponse("admin/index.html", {"request": request, "user": user})


def _get_nextcloud_settings(db: Session) -> NextcloudIntegrationSettings:
    s = db.query(NextcloudIntegrationSettings).order_by(NextcloudIntegrationSettings.id.asc()).first()
    if not s:
        s = NextcloudIntegrationSettings()
        db.add(s)
        db.commit()
        db.refresh(s)
    return s


@router.get("/admin/integrations/nextcloud", response_class=HTMLResponse)
def admin_nextcloud_page(request: Request, db: Session = Depends(get_db), user: User = Depends(require_admin)) -> HTMLResponse:
    s = _get_nextcloud_settings(db)
    ok = "Настройки сохранены." if request.query_params.get("saved") == "1" else None
    return templates.TemplateResponse(
        "admin/nextcloud.html",
        {"request": request, "user": user, "s": s, "error": None, "ok": ok, "discovered_orgs": None, "stats": None},
    )


@router.post("/admin/integrations/nextcloud/save")
def admin_nextcloud_save(
    request: Request,
    base_url: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    root_folder: str = Form(""),
    create_orgs: str = Form("true"),
    is_enabled: str = Form("false"),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
) -> Response:
    s = _get_nextcloud_settings(db)
    s.base_url = (base_url or "").strip()
    s.username = (username or "").strip()
    s.password = password or ""
    s.root_folder = (root_folder or "").strip().strip("/")
    s.create_orgs = str(create_orgs).lower() == "true"
    s.is_enabled = str(is_enabled).lower() == "true"
    s.last_error = ""
    db.commit()
    return _redirect("/admin/integrations/nextcloud?saved=1")


def _dav_from_settings(s: NextcloudIntegrationSettings) -> NextcloudDavClient:
    webdav_base = build_webdav_base_url(s.base_url, s.username)
    return NextcloudDavClient(base_webdav_url=webdav_base, username=s.username, password=s.password)


@router.post("/admin/integrations/nextcloud/test", response_class=HTMLResponse)
def admin_nextcloud_test(request: Request, db: Session = Depends(get_db), user: User = Depends(require_admin)) -> HTMLResponse:
    s = _get_nextcloud_settings(db)
    try:
        dav = _dav_from_settings(s)
        dav.propfind(s.root_folder, depth=1)
        s.last_error = ""
        db.commit()
        ok = "Подключение успешно. WebDAV доступен."
        err = None
    except Exception as e:
        s.last_error = str(e)
        db.commit()
        ok = None
        err = f"Ошибка подключения: {e}"
    return templates.TemplateResponse(
        "admin/nextcloud.html",
        {"request": request, "user": user, "s": s, "error": err, "ok": ok, "discovered_orgs": None, "stats": None},
    )


@router.post("/admin/integrations/nextcloud/discover", response_class=HTMLResponse)
def admin_nextcloud_discover(request: Request, db: Session = Depends(get_db), user: User = Depends(require_admin)) -> HTMLResponse:
    s = _get_nextcloud_settings(db)
    try:
        dav = _dav_from_settings(s)
        items = dav.propfind(s.root_folder, depth=1)
        orgs = sorted({x.name for x in items if x.is_dir and x.name})
        s.last_error = ""
        db.commit()
        return templates.TemplateResponse(
            "admin/nextcloud.html",
            {"request": request, "user": user, "s": s, "error": None, "ok": f"Найдено папок организаций: {len(orgs)}", "discovered_orgs": orgs, "stats": None},
        )
    except Exception as e:
        s.last_error = str(e)
        db.commit()
        return templates.TemplateResponse(
            "admin/nextcloud.html",
            {"request": request, "user": user, "s": s, "error": f"Ошибка: {e}", "ok": None, "discovered_orgs": None, "stats": None},
        )


@router.post("/admin/integrations/nextcloud/sync", response_class=HTMLResponse)
def admin_nextcloud_sync(request: Request, db: Session = Depends(get_db), user: User = Depends(require_admin)) -> HTMLResponse:
    s = _get_nextcloud_settings(db)
    if not s.is_enabled:
        return templates.TemplateResponse(
            "admin/nextcloud.html",
            {"request": request, "user": user, "s": s, "error": "Интеграция выключена (включите и сохраните настройки).", "ok": None, "discovered_orgs": None, "stats": None},
            status_code=400,
        )
    try:
        dav = _dav_from_settings(s)
        stats = sync_from_nextcloud(
            db=db,
            actor=user,
            dav=dav,
            root_folder=s.root_folder,
            create_orgs=s.create_orgs,
            request=request,
        )
        s.last_sync_at = datetime.utcnow()
        s.last_error = ""
        db.commit()
        return templates.TemplateResponse(
            "admin/nextcloud.html",
            {"request": request, "user": user, "s": s, "error": None, "ok": "Синхронизация завершена.", "discovered_orgs": None, "stats": stats},
        )
    except Exception as e:
        s.last_error = str(e)
        db.commit()
        return templates.TemplateResponse(
            "admin/nextcloud.html",
            {"request": request, "user": user, "s": s, "error": f"Ошибка синхронизации: {e}", "ok": None, "discovered_orgs": None, "stats": None},
            status_code=500,
        )


@router.post("/admin/integrations/nextcloud/sync-v2", response_class=HTMLResponse)
def admin_nextcloud_sync_v2(request: Request, db: Session = Depends(get_db), user: User = Depends(require_admin)) -> HTMLResponse:
    """
    New sync (V2): ROOT/Org/03 Артефакты/... (as in docs/03 Артефакты.zip)
    Old sync remains available.
    """
    s = _get_nextcloud_settings(db)
    if not s.is_enabled:
        return templates.TemplateResponse(
            "admin/nextcloud.html",
            {"request": request, "user": user, "s": s, "error": "Интеграция выключена (включите и сохраните настройки).", "ok": None, "discovered_orgs": None, "stats": None},
            status_code=400,
        )
    try:
        dav = _dav_from_settings(s)
        from app.integrations.nextcloud_sync import sync_from_nextcloud_v2

        stats = sync_from_nextcloud_v2(
            db=db,
            actor=user,
            dav=dav,
            root_folder=s.root_folder,
            create_orgs=s.create_orgs,
            request=request,
        )
        s.last_sync_at = datetime.utcnow()
        s.last_error = ""
        db.commit()
        return templates.TemplateResponse(
            "admin/nextcloud.html",
            {"request": request, "user": user, "s": s, "error": None, "ok": "Синхронизация (V2) завершена.", "discovered_orgs": None, "stats": stats},
        )
    except Exception as e:
        s.last_error = str(e)
        db.commit()
        return templates.TemplateResponse(
            "admin/nextcloud.html",
            {"request": request, "user": user, "s": s, "error": f"Ошибка синхронизации (V2): {e}", "ok": None, "discovered_orgs": None, "stats": None},
            status_code=500,
        )


@router.get("/admin/artifacts", response_class=HTMLResponse)
def admin_artifacts(request: Request, user: User = Depends(require_admin)) -> HTMLResponse:
    return templates.TemplateResponse("admin/artifacts.html", {"request": request, "user": user, "result": None, "error": None})


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
    """
    Возвращает (artifact_key, extra_segment_for_node_path).
    - Если item_no задан (есть перечисление 1./2.) — ключ стабилен short_name#item_no и segment=item_no.
    - Если item_no нет — ключ по умолчанию short_name. Но если в БД уже есть другой артефакт с тем же short_name,
      создаём стабильный ключ short_name~<hash8> и кладём hash8 как последний сегмент пути, чтобы не конфликтовать по node_id.
    """
    short_name = (short_name or "").strip()
    base_key = _artifact_key(short_name, item_no)
    if item_no is not None:
        return base_key, str(item_no)

    # В одном Excel иногда встречаются дубли short_name без нумерации 1./2.
    # Тогда делаем стабильный под-ключ (short_name~hash8) и добавляем hash8 как сегмент пути.
    seen_base_keys[base_key] = seen_base_keys.get(base_key, 0) + 1
    if seen_base_keys[base_key] > 1:
        stable = f"{topic}|{domain}|{short_name}|{kb_level}|{indicator_name}|{achievement_item_text}"
        h = hashlib.sha256(stable.encode("utf-8")).hexdigest()[:8]
        return f"{short_name}~{h}", h

    existing = db.query(Artifact).filter(Artifact.artifact_key == base_key).one_or_none()
    if not existing:
        return base_key, None

    # Если это тот же самый артефакт (идемпотентный импорт) — используем базовый ключ.
    if (existing.indicator_name or "") == (indicator_name or "") and (existing.achievement_item_text or "") == (achievement_item_text or ""):
        return base_key, None

    stable = f"{topic}|{domain}|{short_name}|{kb_level}|{indicator_name}|{achievement_item_text}"
    h = hashlib.sha256(stable.encode("utf-8")).hexdigest()[:8]
    key = f"{short_name}~{h}"
    return key, h


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


@router.post("/admin/artifacts/import", response_class=HTMLResponse)
def admin_artifacts_import_apply(
    request: Request,
    upload: UploadFile = File(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
) -> HTMLResponse:
    content = upload.file.read()
    sha = hashlib.sha256(content).hexdigest()
    try:
        rows = parse_program_xlsx(content)
    except Exception as e:
        return templates.TemplateResponse("admin/artifacts.html", {"request": request, "user": user, "result": None, "error": str(e)}, status_code=400)

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
        return templates.TemplateResponse(
            "admin/artifacts.html",
            {"request": request, "user": user, "result": None, "error": f"Ошибка целостности БД при импорте: {msg}"},
            status_code=400,
        )

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

    result = {"sha256": sha, "created": created, "updated": updated}
    return templates.TemplateResponse("admin/artifacts.html", {"request": request, "user": user, "result": result, "error": None})


@router.get("/admin/orgs", response_class=HTMLResponse)
def admin_orgs(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
    page: int = 1,
    page_size: int = 20,
    sort: str = "created_at",
    dir: str = "desc",
) -> HTMLResponse:
    page = max(int(page or 1), 1)
    page_size = int(page_size or 20)
    if page_size < 10:
        page_size = 10
    if page_size > 200:
        page_size = 200

    sort_key = (sort or "created_at").strip().lower()
    sort_dir = (dir or "desc").strip().lower()
    if sort_dir not in ("asc", "desc"):
        sort_dir = "desc"

    # For "created_by" sorting we need the creator login; use an outer join to avoid N+1.
    creator_login = func.coalesce(User.login, Organization.created_via)
    base_q = db.query(Organization, creator_login.label("creator_login")).outerjoin(User, User.id == Organization.created_by_user_id)

    if sort_key == "name":
        order_expr = Organization.name.asc() if sort_dir == "asc" else Organization.name.desc()
        base_q = base_q.order_by(order_expr, Organization.id.desc())
    elif sort_key == "created_by":
        order_expr = creator_login.asc() if sort_dir == "asc" else creator_login.desc()
        base_q = base_q.order_by(order_expr, Organization.created_at.desc(), Organization.id.desc())
    else:
        # created_at
        order_expr = Organization.created_at.asc() if sort_dir == "asc" else Organization.created_at.desc()
        base_q = base_q.order_by(order_expr, Organization.id.desc())

    total = base_q.count()

    total_pages = max((total + page_size - 1) // page_size, 1)
    if page > total_pages:
        page = total_pages
    offset = (page - 1) * page_size

    rows = base_q.offset(offset).limit(page_size).all()

    org_rows = []
    for (o, creator_login_val) in rows:
        if o.created_by_user_id and creator_login_val and str(creator_login_val) not in ("system", "nextcloud", "manual"):
            created_by_label = str(creator_login_val)
        else:
            via = getattr(o, "created_via", "") or ""
            if via == "nextcloud":
                created_by_label = "Синхронизация (Nextcloud)"
            elif via == "system":
                created_by_label = "Система"
            else:
                created_by_label = "—"
        org_rows.append({"org": o, "created_by_label": created_by_label})

    base_query = urlencode({"page_size": str(page_size), "sort": sort_key, "dir": sort_dir})
    window = 3
    start = max(1, page - window)
    end = min(total_pages, page + window)
    page_links = list(range(start, end + 1))

    resp = templates.TemplateResponse(
        "admin/orgs.html",
        {
            "request": request,
            "user": user,
            "orgs": org_rows,
            "error": None,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "has_prev": page > 1,
            "has_next": offset + page_size < total,
            "page_links": page_links,
            "base_query": base_query,
            "sort": sort_key,
            "dir": sort_dir,
        },
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@router.post("/admin/orgs")
def admin_orgs_create(
    request: Request,
    name: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
) -> Response:
    name = name.strip()
    if not name:
        orgs = db.query(Organization).order_by(Organization.name.asc()).all()
        return templates.TemplateResponse(
            "admin/orgs.html",
            {"request": request, "user": user, "orgs": orgs, "error": "Имя организации обязательно"},
            status_code=400,
        )
    exists = db.query(Organization).filter(Organization.name == name).one_or_none()
    if exists:
        orgs = db.query(Organization).order_by(Organization.name.asc()).all()
        return templates.TemplateResponse(
            "admin/orgs.html",
            {"request": request, "user": user, "orgs": orgs, "error": "Организация уже существует"},
            status_code=400,
        )
    db.add(Organization(name=name, created_by_user_id=user.id, created_via="manual"))
    db.commit()
    return _redirect("/admin/orgs")


@router.get("/admin/orgs/{org_id}/edit", response_class=HTMLResponse)
def admin_orgs_edit_page(
    org_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
) -> HTMLResponse:
    org = db.get(Organization, org_id)
    if not org:
        raise HTTPException(status_code=404, detail="Организация не найдена")
    return templates.TemplateResponse("admin/org_edit.html", {"request": request, "user": user, "org": org, "error": None})


@router.post("/admin/orgs/{org_id}/edit")
def admin_orgs_edit_save(
    org_id: int,
    request: Request,
    name: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
) -> Response:
    org = db.get(Organization, org_id)
    if not org:
        raise HTTPException(status_code=404, detail="Организация не найдена")
    name = name.strip()
    if not name:
        return templates.TemplateResponse("admin/org_edit.html", {"request": request, "user": user, "org": org, "error": "Имя организации обязательно"}, status_code=400)
    exists = db.query(Organization).filter(Organization.name == name, Organization.id != org.id).one_or_none()
    if exists:
        return templates.TemplateResponse("admin/org_edit.html", {"request": request, "user": user, "org": org, "error": "Организация с таким именем уже существует"}, status_code=400)
    org.name = name
    db.commit()
    return _redirect("/admin/orgs")


@router.post("/admin/orgs/{org_id}/delete")
def admin_orgs_delete(
    org_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
) -> Response:
    org = db.get(Organization, org_id)
    if not org:
        raise HTTPException(status_code=404, detail="Организация не найдена")
    try:
        db.delete(org)
        db.commit()
        return _redirect("/admin/orgs")
    except IntegrityError:
        db.rollback()
        orgs = db.query(Organization).order_by(Organization.created_at.desc(), Organization.id.desc()).all()
        resp = templates.TemplateResponse(
            "admin/orgs.html",
            {
                "request": request,
                "user": user,
                "orgs": orgs,
                "error": "Нельзя удалить организацию: есть связанные данные (артефакты/пользователи/файлы).",
            },
            status_code=400,
        )
        resp.headers["Cache-Control"] = "no-store, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        return resp


@router.get("/admin/users", response_class=HTMLResponse)
def admin_users(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
    org_id: str | None = None,
    page: int = 1,
    page_size: int = 50,
    sort: str = "login",
    dir: str = "asc",
) -> HTMLResponse:
    page = max(int(page or 1), 1)
    page_size = int(page_size or 50)
    if page_size < 10:
        page_size = 10
    if page_size > 200:
        page_size = 200

    orgs = db.query(Organization).order_by(Organization.name.asc()).all()
    selected_org_id = int(org_id) if (org_id and str(org_id).isdigit()) else None

    sort_key = (sort or "login").strip().lower()
    sort_dir = (dir or "asc").strip().lower()
    if sort_dir not in ("asc", "desc"):
        sort_dir = "asc"

    q = db.query(User)
    if selected_org_id:
        q = q.join(UserOrgMembership, UserOrgMembership.user_id == User.id).filter(UserOrgMembership.org_id == selected_org_id)

    if sort_key == "created_at":
        order_expr = User.created_at.asc() if sort_dir == "asc" else User.created_at.desc()
        q = q.order_by(order_expr, User.login.asc())
    elif sort_key == "is_admin":
        order_expr = User.is_admin.asc() if sort_dir == "asc" else User.is_admin.desc()
        q = q.order_by(order_expr, User.login.asc())
    elif sort_key == "is_active":
        order_expr = User.is_active.asc() if sort_dir == "asc" else User.is_active.desc()
        q = q.order_by(order_expr, User.login.asc())
    else:
        # login
        order_expr = User.login.asc() if sort_dir == "asc" else User.login.desc()
        q = q.order_by(order_expr)

    total = q.count()
    total_pages = max((total + page_size - 1) // page_size, 1)
    if page > total_pages:
        page = total_pages
    offset = (page - 1) * page_size
    users = q.offset(offset).limit(page_size).all()

    base_qd: dict[str, str] = {"page_size": str(page_size), "sort": sort_key, "dir": sort_dir}
    if selected_org_id:
        base_qd["org_id"] = str(selected_org_id)
    base_query = urlencode(base_qd)
    window = 3
    start = max(1, page - window)
    end = min(total_pages, page + window)
    page_links = list(range(start, end + 1))
    current_url = request.url.path + (f"?{request.url.query}" if request.url.query else "")

    resp = templates.TemplateResponse(
        "admin/users.html",
        {
            "request": request,
            "user": user,
            "users": users,
            "orgs": orgs,
            "selected_org_id": selected_org_id,
            "error": None,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "has_prev": page > 1,
            "has_next": offset + page_size < total,
            "page_links": page_links,
            "base_query": base_query,
            "current_url": current_url,
            "sort": sort_key,
            "dir": sort_dir,
        },
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@router.post("/admin/users")
def admin_users_create(
    request: Request,
    login: str = Form(...),
    password: str = Form(...),
    full_name: str = Form(""),
    is_admin: bool = Form(False),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
) -> Response:
    login = login.strip()
    if not login:
        users = db.query(User).order_by(User.login.asc()).all()
        return templates.TemplateResponse("admin/users.html", {"request": request, "user": user, "users": users, "error": "Логин обязателен"}, status_code=400)
    exists = db.query(User).filter(User.login == login).one_or_none()
    if exists:
        users = db.query(User).order_by(User.login.asc()).all()
        return templates.TemplateResponse("admin/users.html", {"request": request, "user": user, "users": users, "error": "Логин уже используется"}, status_code=400)

    pwd_err = _validate_password(password)
    if pwd_err:
        users = db.query(User).order_by(User.login.asc()).all()
        return templates.TemplateResponse("admin/users.html", {"request": request, "user": user, "users": users, "error": pwd_err}, status_code=400)
    new_user = User(
        login=login,
        password_hash="",
        full_name=full_name.strip(),
        is_active=True,
        is_admin=bool(is_admin),
    )
    new_user.password_hash = hash_password(password)
    db.add(new_user)
    db.commit()
    return _redirect("/admin/users")


@router.post("/admin/users/{user_id}/toggle_active")
def admin_users_toggle_active(
    user_id: int,
    request: Request,
    back: str = Form(""),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
) -> Response:
    u = db.get(User, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    if u.id == user.id:
        # Не даём заблокировать себя, чтобы не потерять доступ.
        return _redirect(back or "/admin/users")
    u.is_active = not bool(u.is_active)
    db.commit()
    return _redirect(back or "/admin/users")


@router.post("/admin/users/{user_id}/delete")
def admin_users_delete(
    user_id: int,
    request: Request,
    back: str = Form(""),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
) -> Response:
    u = db.get(User, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    if u.id == user.id:
        # Не даём удалить себя.
        return _redirect(back or "/admin/users")
    try:
        # Сначала удаляем роли/привязки к организациям (иначе ORM пытается проставить NULL в user_id).
        db.query(UserOrgMembership).filter(UserOrgMembership.user_id == u.id).delete(synchronize_session=False)
        db.delete(u)
        db.commit()
        return _redirect(back or "/admin/users")
    except IntegrityError:
        db.rollback()
        # Фоллбек: не падаем 500, а возвращаемся назад.
        return _redirect(back or "/admin/users")


@router.get("/admin/users/{user_id}/edit", response_class=HTMLResponse)
def admin_users_edit_page(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
) -> HTMLResponse:
    u = db.get(User, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    return templates.TemplateResponse("admin/user_edit.html", {"request": request, "user": user, "u": u, "error": None})


@router.post("/admin/users/{user_id}/edit")
def admin_users_edit_save(
    user_id: int,
    request: Request,
    full_name: str = Form(""),
    is_active: str = Form("true"),
    is_admin: str = Form("false"),
    new_password: str = Form(""),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
) -> Response:
    u = db.get(User, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    u.full_name = (full_name or "").strip()
    u.is_active = str(is_active).lower() == "true"
    u.is_admin = str(is_admin).lower() == "true"

    if new_password and new_password.strip():
        pwd_err = _validate_password(new_password)
        if pwd_err:
            return templates.TemplateResponse("admin/user_edit.html", {"request": request, "user": user, "u": u, "error": pwd_err}, status_code=400)
        u.password_hash = hash_password(new_password)

    db.commit()
    return _redirect("/admin/users")


@router.get("/admin/memberships", response_class=HTMLResponse)
def admin_memberships(request: Request, db: Session = Depends(get_db), user: User = Depends(require_admin)) -> HTMLResponse:
    all_users = db.query(User).order_by(User.login.asc()).all()
    # "Системные" аккаунты (показываем отдельно): admin (is_admin) и служебный Auditor.
    system_users = [u for u in all_users if u.is_admin or (u.login or "").strip().lower() in ("admin", "auditor")]
    users = [u for u in all_users if u not in system_users]

    # Default организация — служебная (нужна системе), но в UI не показываем.
    orgs = db.query(Organization).filter(Organization.name != "Default").order_by(Organization.name.asc()).all()
    all_ms = (
        db.query(UserOrgMembership)
        .join(Organization, Organization.id == UserOrgMembership.org_id)
        .join(User, User.id == UserOrgMembership.user_id)
        .order_by(UserOrgMembership.created_at.desc())
        .all()
    )
    system_user_ids = {u.id for u in system_users}
    system_memberships = [m for m in all_ms if (m.org and m.org.name == "Default") or (m.user_id in system_user_ids)]
    org_memberships = [m for m in all_ms if m not in system_memberships and (m.org and m.org.name != "Default")]
    role_labels = {"admin": "Администратор", "auditor": "Аудитор", "customer": "Заказчик"}
    return templates.TemplateResponse(
        "admin/memberships.html",
        {
            "request": request,
            "user": user,
            "users": users,
            "system_users": system_users,
            "orgs": orgs,
            "system_memberships": system_memberships,
            "memberships": org_memberships,
            "roles": [r.value for r in Role],
            "role_labels": role_labels,
            "error": None,
        },
    )


@router.post("/admin/memberships")
def admin_memberships_create(
    request: Request,
    user_id: int = Form(...),
    org_id: int = Form(...),
    role: str = Form(...),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
) -> Response:
    # Не даём назначать роли на служебную Default организацию.
    org = db.get(Organization, org_id)
    if org and org.name == "Default":
        return _redirect("/admin/memberships")
    try:
        role_enum = Role(role)
    except ValueError:
        all_users = db.query(User).order_by(User.login.asc()).all()
        system_users = [u for u in all_users if u.is_admin or (u.login or "").strip().lower() in ("admin", "auditor")]
        users = [u for u in all_users if u not in system_users]
        orgs = db.query(Organization).filter(Organization.name != "Default").order_by(Organization.name.asc()).all()
        all_ms = (
            db.query(UserOrgMembership)
            .join(Organization, Organization.id == UserOrgMembership.org_id)
            .join(User, User.id == UserOrgMembership.user_id)
            .order_by(UserOrgMembership.created_at.desc())
            .all()
        )
        system_user_ids = {u.id for u in system_users}
        system_memberships = [m for m in all_ms if (m.org and m.org.name == "Default") or (m.user_id in system_user_ids)]
        org_memberships = [m for m in all_ms if m not in system_memberships and (m.org and m.org.name != "Default")]
        role_labels = {"admin": "Администратор", "auditor": "Аудитор", "customer": "Заказчик"}
        return templates.TemplateResponse(
            "admin/memberships.html",
            {
                "request": request,
                "user": user,
                "users": users,
                "system_users": system_users,
                "orgs": orgs,
                "system_memberships": system_memberships,
                "memberships": org_memberships,
                "roles": [r.value for r in Role],
                "role_labels": role_labels,
                "error": "Некорректная роль",
            },
            status_code=400,
        )
    exists = (
        db.query(UserOrgMembership)
        .filter(UserOrgMembership.user_id == user_id, UserOrgMembership.org_id == org_id)
        .one_or_none()
    )
    if exists:
        exists.role = role_enum
    else:
        db.add(UserOrgMembership(user_id=user_id, org_id=org_id, role=role_enum))
    db.commit()
    return _redirect("/admin/memberships")


@router.get("/admin/orgs/{org_id}/users", response_class=HTMLResponse)
def admin_org_users(
    org_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
) -> HTMLResponse:
    org = db.get(Organization, org_id)
    if not org:
        raise HTTPException(status_code=404, detail="Организация не найдена")

    memberships = (
        db.query(UserOrgMembership)
        .join(User, User.id == UserOrgMembership.user_id)
        .filter(UserOrgMembership.org_id == org_id)
        .order_by(User.login.asc())
        .all()
    )
    role_labels = {"admin": "Администратор", "auditor": "Аудитор", "customer": "Заказчик"}
    resp = templates.TemplateResponse(
        "admin/org_users.html",
        {
            "request": request,
            "user": user,
            "org": org,
            "memberships": memberships,
            "role_labels": role_labels,
        },
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp
