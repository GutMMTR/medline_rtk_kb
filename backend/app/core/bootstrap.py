from __future__ import annotations

from sqlalchemy.orm import Session

from app.auth.security import hash_password
from app.core.config import settings
from app.db.models import Organization, Role, User, UserOrgMembership


def ensure_default_admin(db: Session) -> None:
    """
    Создаёт дефолтного администратора (по env) и базовую организацию.
    Идемпотентно: повторный запуск ничего не ломает.
    """
    admin = db.query(User).filter(User.login == settings.admin_login).one_or_none()
    if not admin:
        admin = User(
            login=settings.admin_login,
            password_hash=hash_password(settings.admin_password),
            full_name=settings.admin_full_name,
            is_active=True,
            is_admin=True,
        )
        db.add(admin)
        db.flush()
    else:
        # На MVP не обновляем пароль автоматически, чтобы не "перезатирать" руками изменённое.
        if not admin.is_admin:
            admin.is_admin = True

    # System org (служебная). Исторически называлась "Default", теперь — "РТК".
    # Делаем обратную совместимость: если есть только "Default", переименуем в "РТК".
    system_org = db.query(Organization).filter(Organization.name == "РТК").one_or_none()
    legacy_org = None
    if not system_org:
        legacy_org = db.query(Organization).filter(Organization.name == "Default").one_or_none()
    if system_org:
        pass
    elif legacy_org:
        legacy_org.name = "РТК"
        system_org = legacy_org
    else:
        system_org = Organization(name="РТК", created_via="system")
        db.add(system_org)
        db.flush()

    # Дадим администратору membership для удобства UI (хотя admin и так всё может)
    membership = (
        db.query(UserOrgMembership)
        .filter(UserOrgMembership.user_id == admin.id, UserOrgMembership.org_id == system_org.id)
        .one_or_none()
    )
    if not membership:
        db.add(UserOrgMembership(user_id=admin.id, org_id=system_org.id, role=Role.admin))
