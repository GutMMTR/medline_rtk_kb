from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.auth.dependencies import get_current_user, get_current_user_bearer
from app.auth.security import JWT_TTL_MINUTES, create_access_token, verify_password
from app.db.models import Organization, Role, User, UserOrgMembership
from app.db.session import get_db


router = APIRouter(prefix="/api/auth", tags=["auth"])


class LoginRequest(BaseModel):
    login: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


@router.post("/login", response_model=TokenResponse)
def login(req: LoginRequest, db: Session = Depends(get_db)) -> TokenResponse:
    user = db.query(User).filter(User.login == req.login).one_or_none()
    if not user or not user.is_active or not verify_password(req.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid login or password")
    token = create_access_token(user.id)
    return TokenResponse(access_token=token, expires_in=JWT_TTL_MINUTES * 60)


@router.post("/exchange", response_model=TokenResponse)
def exchange_cookie_to_bearer(user: User = Depends(get_current_user)) -> TokenResponse:
    """
    MVP SSO: если пользователь уже залогинен в HTML (cookie JWT),
    SPA может получить Bearer токен без повторного ввода пароля.
    """
    token = create_access_token(user.id)
    return TokenResponse(access_token=token, expires_in=JWT_TTL_MINUTES * 60)


class OrgRole(BaseModel):
    org_id: int
    org_name: str
    role: str


class MeResponse(BaseModel):
    id: int
    login: str
    full_name: str
    is_admin: bool
    is_global_auditor: bool
    orgs: list[OrgRole]


def _is_global_auditor(db: Session, user_id: int) -> bool:
    return (
        db.query(UserOrgMembership)
        .filter(UserOrgMembership.user_id == user_id, UserOrgMembership.role == Role.auditor)
        .first()
        is not None
    )


@router.get("/me", response_model=MeResponse)
def me(db: Session = Depends(get_db), user: User = Depends(get_current_user_bearer)) -> MeResponse:
    return build_me_response(db, user)


def build_me_response(db: Session, user: User) -> MeResponse:
    is_global_auditor = user.is_admin or _is_global_auditor(db, user.id)
    if user.is_admin or is_global_auditor:
        orgs = db.query(Organization).order_by(Organization.name.asc()).all()
        org_roles = [OrgRole(org_id=o.id, org_name=o.name, role=(Role.admin.value if user.is_admin else Role.auditor.value)) for o in orgs]
    else:
        memberships = (
            db.query(UserOrgMembership, Organization)
            .join(Organization, Organization.id == UserOrgMembership.org_id)
            .filter(UserOrgMembership.user_id == user.id)
            .order_by(Organization.name.asc())
            .all()
        )
        org_roles = [OrgRole(org_id=o.id, org_name=o.name, role=m.role.value) for (m, o) in memberships]
    return MeResponse(
        id=user.id,
        login=user.login,
        full_name=user.full_name,
        is_admin=user.is_admin,
        is_global_auditor=is_global_auditor and not user.is_admin,
        orgs=org_roles,
    )

