from __future__ import annotations

from fastapi import Cookie, Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from app.auth.security import JWT_COOKIE_NAME, decode_access_token
from app.db.models import Role, User, UserOrgMembership
from app.db.session import get_db


def get_current_user(
    db: Session = Depends(get_db),
    access_token: str | None = Cookie(default=None, alias=JWT_COOKIE_NAME),
) -> User:
    if not access_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    user_id = decode_access_token(access_token)
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    user = db.get(User, user_id)
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User inactive")
    return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    if not user.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin required")
    return user


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    parts = authorization.strip().split()
    if len(parts) != 2:
        return None
    scheme, token = parts[0].lower(), parts[1].strip()
    if scheme != "bearer" or not token:
        return None
    return token


def get_current_user_bearer(
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None, alias="Authorization"),
    access_token: str | None = Cookie(default=None, alias=JWT_COOKIE_NAME),
) -> User:
    token = _extract_bearer_token(authorization) or access_token
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    user_id = decode_access_token(token)
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    user = db.get(User, user_id)
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User inactive")
    return user


def require_admin_bearer(user: User = Depends(get_current_user_bearer)) -> User:
    if not user.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin required")
    return user


def get_user_role_for_org(db: Session, user: User, org_id: int) -> Role | None:
    if user.is_admin:
        return Role.admin
    # MVP-допущение: "аудитор" считается глобальной ролью, если у пользователя
    # есть хотя бы один membership с ролью auditor.
    is_global_auditor = (
        db.query(UserOrgMembership)
        .filter(UserOrgMembership.user_id == user.id, UserOrgMembership.role == Role.auditor)
        .first()
        is not None
    )
    if is_global_auditor:
        return Role.auditor
    membership = (
        db.query(UserOrgMembership)
        .filter(UserOrgMembership.user_id == user.id, UserOrgMembership.org_id == org_id)
        .one_or_none()
    )
    return membership.role if membership else None
