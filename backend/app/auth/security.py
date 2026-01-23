from __future__ import annotations

from datetime import datetime, timedelta

from jose import JWTError, jwt
from passlib.context import CryptContext

from app.core.config import settings


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

JWT_ALG = "HS256"
JWT_COOKIE_NAME = "access_token"
JWT_TTL_MINUTES = 60 * 12  # 12h for MVP


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, password_hash: str) -> bool:
    return pwd_context.verify(plain_password, password_hash)


def create_access_token(user_id: int) -> str:
    now = datetime.utcnow()
    payload = {"sub": str(user_id), "iat": int(now.timestamp()), "exp": int((now + timedelta(minutes=JWT_TTL_MINUTES)).timestamp())}
    return jwt.encode(payload, settings.app_secret_key, algorithm=JWT_ALG)


def decode_access_token(token: str) -> int | None:
    try:
        payload = jwt.decode(token, settings.app_secret_key, algorithms=[JWT_ALG])
        sub = payload.get("sub")
        if not sub:
            return None
        return int(sub)
    except (JWTError, ValueError):
        return None
