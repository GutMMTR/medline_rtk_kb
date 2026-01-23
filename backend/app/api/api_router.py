from __future__ import annotations

from fastapi import APIRouter

from app.api.auth_api import router as auth_router
from app.api.admin_api import router as admin_router
from app.api.metrics_api import router as metrics_router
from app.api.org_artifacts_api import router as org_artifacts_router


api_router = APIRouter()
api_router.include_router(auth_router)
api_router.include_router(admin_router)
api_router.include_router(org_artifacts_router)
api_router.include_router(metrics_router)

