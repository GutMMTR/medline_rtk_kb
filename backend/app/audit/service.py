from __future__ import annotations

from datetime import datetime

from fastapi import Request
from sqlalchemy.orm import Session

from app.db.models import AuditLog, User


def write_audit_log(
    db: Session,
    *,
    actor: User | None,
    org_id: int | None,
    action: str,
    entity_type: str,
    entity_id: str,
    before: dict | None = None,
    after: dict | None = None,
    request: Request | None = None,
) -> None:
    ip = ""
    user_agent = ""
    if request is not None:
        ip = request.client.host if request.client else ""
        user_agent = request.headers.get("user-agent", "")[:1024]
    db.add(
        AuditLog(
            at=datetime.utcnow(),
            actor_user_id=(actor.id if actor else None),
            org_id=org_id,
            action=action,
            entity_type=entity_type,
            entity_id=str(entity_id),
            before_json=before,
            after_json=after,
            ip=ip,
            user_agent=user_agent,
        )
    )

