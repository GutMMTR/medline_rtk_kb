from __future__ import annotations

from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.status import HTTP_303_SEE_OTHER, HTTP_401_UNAUTHORIZED

from app.api.api_router import api_router
from app.api.web import router as web_router
from app.core.bootstrap import ensure_default_admin
from app.db.session import SessionLocal

import uuid


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        cid = request.headers.get("x-correlation-id") or str(uuid.uuid4())
        request.state.correlation_id = cid
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = cid
        return response


app = FastAPI(title="Медлайн.РТК.КБ - Хранилище файлов (MVP)")
app.add_middleware(CorrelationIdMiddleware)

app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(web_router)
app.include_router(api_router)


@app.on_event("startup")
def _startup() -> None:
    # Дефолтный админ (идемпотентно)
    db = SessionLocal()
    try:
        ensure_default_admin(db)
        db.commit()
    finally:
        db.close()


@app.exception_handler(StarletteHTTPException)
async def _http_exception_handler(request: Request, exc: StarletteHTTPException):
    # Для UI: если не авторизованы — редирект на логин
    if exc.status_code == HTTP_401_UNAUTHORIZED and "text/html" in (request.headers.get("accept") or ""):
        return RedirectResponse(url="/login", status_code=HTTP_303_SEE_OTHER)
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
