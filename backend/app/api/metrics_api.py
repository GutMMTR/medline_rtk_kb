from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

from app.obs.metrics import metrics


router = APIRouter(tags=["metrics"])


@router.get("/metrics", response_class=PlainTextResponse)
def prometheus_metrics() -> str:
    return metrics.render_prometheus()

