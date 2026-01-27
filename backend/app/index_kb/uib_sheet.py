from __future__ import annotations

import os
import re
from dataclasses import dataclass
from functools import lru_cache

from openpyxl import load_workbook
from sqlalchemy import and_, case, func
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Session

from app.db.models import Artifact, IndexKbManualValue, OrgArtifact, OrgArtifactStatus, Organization, User


UIB_SHEET_NAME = "Управление ИБ"

# Token like "СУИБ.УР.1"
_TOKEN_RE = re.compile(r"^[^\d\s][\w-]+(?:\.[\w-]+)+$", re.UNICODE)


@dataclass(frozen=True)
class UibRow:
    kind: str  # group|item
    row_key: str
    title: str
    short_name: str
    group_code: str  # short code of current group section (e.g. "СУИБ", "УР")


@dataclass(frozen=True)
class UibRowView:
    row: UibRow
    is_auto: bool
    value: float | None  # 0..5 for auto/manual, None if not set
    source: str  # auto|manual|empty
    updated_at: object | None
    updated_by: str


@dataclass(frozen=True)
class UibTemplate:
    path: str
    mtime_ns: int
    header_row: int
    col_title: int  # B
    col_short: int  # C
    rows: list[UibRow]


def _find_uib_sheet_name(wb) -> str:
    # Prefer exact match, fallback by substring.
    if UIB_SHEET_NAME in wb.sheetnames:
        return UIB_SHEET_NAME
    for s in wb.sheetnames:
        if "Управление" in s:
            return s
    return wb.sheetnames[0]


def _find_header_row(ws, max_scan_rows: int = 120, max_scan_cols: int = 40) -> int | None:
    for r in range(1, min(ws.max_row, max_scan_rows) + 1):
        texts = []
        for c in range(1, min(ws.max_column, max_scan_cols) + 1):
            v = ws.cell(r, c).value
            if isinstance(v, str) and v.strip():
                texts.append(v)
        joined = " | ".join(texts)
        if "Сокращ" in joined and ("КБ1" in joined or "КБ 1" in joined or "КБ2" in joined or "КБ3" in joined):
            return r
    return None


@lru_cache(maxsize=8)
def _load_uib_template_cached(path: str, mtime_ns: int) -> UibTemplate:
    wb = load_workbook(path, read_only=True, data_only=False)
    ws = wb[_find_uib_sheet_name(wb)]
    hdr = _find_header_row(ws)
    if not hdr:
        raise RuntimeError("Не найдена строка заголовка (Сокращенное/КБ1/КБ2/КБ3) на листе 'Управление ИБ'")

    # From analysis: B=title, C=short, row 41 is header. We'll still detect dynamically.
    col_title = 2
    col_short = 3

    rows: list[UibRow] = []
    current_group_title = ""
    current_group_code = ""

    empty_streak = 0
    for r in range(hdr + 1, ws.max_row + 1):
        title_v = ws.cell(r, col_title).value
        short_v = ws.cell(r, col_short).value

        title = title_v.strip() if isinstance(title_v, str) else ""
        short = short_v.strip() if isinstance(short_v, str) else ""
        if short.startswith("=") or title.startswith("="):
            # skip formula blocks above the main table
            continue

        if not title and not short:
            empty_streak += 1
            if empty_streak >= 8:
                break
            continue
        empty_streak = 0

        # Group header rows look like: title "Система управления ИБ", short "СУИБ" (no dot)
        if title and short and "." not in short:
            current_group_title = title
            current_group_code = short
            rows.append(UibRow(kind="group", row_key=f"group:{short}", title=title, short_name=short, group_code=short))
            continue

        # Item rows: have short_name token with dots
        if short and _TOKEN_RE.match(short):
            # stable key: token itself
            rows.append(
                UibRow(
                    kind="item",
                    row_key=short.upper(),
                    title=(title or current_group_title),
                    short_name=short,
                    group_code=current_group_code,
                )
            )

    return UibTemplate(path=path, mtime_ns=mtime_ns, header_row=hdr, col_title=col_title, col_short=col_short, rows=rows)


def get_uib_template(path: str) -> UibTemplate:
    st = os.stat(path)
    return _load_uib_template_cached(path, st.st_mtime_ns)


def compute_auto_scores(db: Session, org_id: int, short_names: list[str]) -> dict[str, float]:
    """
    Auto-score rule (requested):
    - if artifact exists and all points are AUDITED (audited_file_version_id == current_file_version_id) => 5
    - else => 0
    """
    if not short_names:
        return {}
    sns = sorted({s.upper() for s in short_names if s})

    # Determine which short_names exist in artifact справочнике.
    existing = {
        (sn or "").upper()
        for (sn,) in db.query(Artifact.short_name)
        .filter(Artifact.short_name != "", func.upper(Artifact.short_name).in_(sns))
        .distinct()
        .all()
        if sn
    }
    if not existing:
        return {}

    audited_flag = case(
        (
            and_(
                OrgArtifact.current_file_version_id.isnot(None),
                OrgArtifact.audited_file_version_id.isnot(None),
                OrgArtifact.audited_file_version_id == OrgArtifact.current_file_version_id,
            ),
            1,
        ),
        else_=0,
    )
    rows = (
        db.query(func.upper(Artifact.short_name).label("sn"), func.min(audited_flag).label("all_audited"))
        .join(OrgArtifact, OrgArtifact.artifact_id == Artifact.id)
        .filter(OrgArtifact.org_id == org_id, Artifact.short_name != "", func.upper(Artifact.short_name).in_(sorted(existing)))
        .group_by(func.upper(Artifact.short_name))
        .all()
    )
    out: dict[str, float] = {}
    for sn_u, all_audited in rows:
        out[str(sn_u).upper()] = 5.0 if int(all_audited or 0) == 1 else 0.0
    # If exists in справочнике but нет org_artifacts (не материализовано) — считаем 0
    for sn_u in existing:
        out.setdefault(sn_u, 0.0)
    return out


def load_manual_values(db: Session, org_id: int, sheet_name: str) -> dict[str, IndexKbManualValue]:
    rows = (
        db.query(IndexKbManualValue)
        .options(joinedload(IndexKbManualValue.updated_by))
        .filter(IndexKbManualValue.org_id == org_id, IndexKbManualValue.sheet_name == sheet_name)
        .all()
    )
    return {r.row_key: r for r in rows}


@dataclass(frozen=True)
class UibSummaryRow:
    title: str
    short_name: str
    kb3: float | None
    kb2: float | None
    kb1: float | None
    calc_2025: float | None
    current: float | None


def _mean(vals: list[float]) -> float | None:
    vals2 = [float(v) for v in vals if v is not None]
    if not vals2:
        return None
    return sum(vals2) / len(vals2)


def compute_uib_summary(rows: list[UibRowView]) -> list[UibSummaryRow]:
    """
    Итоговая таблица в Excel (скрин): по каждой категории/группе считается AVERAGE по строкам ниже.
    В нашей упрощённой форме КБ1/КБ2/КБ3 и "расчетный/текущий" пока берём одинаково (из value).
    """
    out: list[UibSummaryRow] = []
    cur_group_title = ""
    cur_group_code = ""
    acc: list[float] = []

    def flush() -> None:
        nonlocal acc, cur_group_title, cur_group_code
        if not cur_group_code:
            return
        m = _mean(acc)
        out.append(
            UibSummaryRow(
                title=cur_group_title,
                short_name=cur_group_code,
                kb3=m,
                kb2=m,
                kb1=m,
                calc_2025=m,
                current=m,
            )
        )
        acc = []

    for rv in rows:
        if rv.row.kind == "group":
            flush()
            cur_group_title = rv.row.title
            cur_group_code = rv.row.short_name
            continue
        if rv.value is None:
            continue
        acc.append(float(rv.value))
    flush()
    return out


def build_uib_view(db: Session, *, org_id: int, template_path: str, actor: User) -> tuple[Organization, UibTemplate, list[UibRowView]]:
    org = db.get(Organization, org_id)
    if not org:
        raise RuntimeError("Организация не найдена")

    tpl = get_uib_template(template_path)
    short_names = [r.short_name for r in tpl.rows if r.kind == "item" and r.short_name]
    auto = compute_auto_scores(db, org_id, short_names)
    manual = load_manual_values(db, org_id, UIB_SHEET_NAME)

    out: list[UibRowView] = []
    for r in tpl.rows:
        if r.kind == "group":
            out.append(UibRowView(row=r, is_auto=True, value=None, source="group", updated_at=None, updated_by=""))
            continue

        key = r.row_key
        sn_u = r.short_name.upper()
        if sn_u in auto:
            out.append(UibRowView(row=r, is_auto=True, value=auto[sn_u], source="auto", updated_at=None, updated_by=""))
            continue

        mv = manual.get(key)
        if mv:
            out.append(
                UibRowView(
                    row=r,
                    is_auto=False,
                    value=float(mv.value),
                    source="manual",
                    updated_at=mv.updated_at,
                    updated_by=mv.updated_by.login if mv.updated_by else "",
                )
            )
        else:
            out.append(UibRowView(row=r, is_auto=False, value=None, source="empty", updated_at=None, updated_by=""))

    return org, tpl, out


def upsert_manual_value(
    db: Session,
    *,
    org_id: int,
    sheet_name: str,
    row_key: str,
    value: float,
    actor: User,
) -> None:
    row_key = (row_key or "").strip()
    if not row_key:
        return
    value = float(value)
    if value < 0:
        value = 0.0
    if value > 5:
        value = 5.0

    exists = (
        db.query(IndexKbManualValue)
        .filter(IndexKbManualValue.org_id == org_id, IndexKbManualValue.sheet_name == sheet_name, IndexKbManualValue.row_key == row_key)
        .one_or_none()
    )
    if exists:
        exists.value = value
        exists.updated_by_user_id = actor.id
        # Let ORM store UTC timestamp; display is converted in UI.
        from datetime import datetime

        exists.updated_at = datetime.utcnow()
    else:
        db.add(
            IndexKbManualValue(
                org_id=org_id,
                sheet_name=sheet_name,
                row_key=row_key,
                value=value,
                updated_by_user_id=actor.id,
            )
        )

