from __future__ import annotations

import os
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from xml.etree import ElementTree as ET

from sqlalchemy import and_, case, func
from sqlalchemy.orm import Session, joinedload

from app.db.models import (
    Artifact,
    ArtifactLevel,
    ArtifactLevelItem,
    AuditPeriod,
    FileVersion,
    IndexKbManualValue,
    IndexKbTemplateRow,
    OrgArtifact,
    OrgArtifactReviewStatus,
    Organization,
    User,
)


SZI_SHEET_NAME = "СЗИ"

# Token like "СЗИ.РА-VPN.1"
_TOKEN_RE = re.compile(r"^[^\d\s][\w-]+(?:\.[\w-]+)+$", re.UNICODE)


@dataclass(frozen=True)
class SziRow:
    kind: str  # group|item
    row_key: str
    title: str
    short_name: str
    group_code: str


@dataclass(frozen=True)
class SziRowView:
    row: SziRow
    is_auto: bool
    value: float | None
    source: str  # group|auto|manual|empty
    updated_at: object | None
    updated_by: str


@dataclass(frozen=True)
class SziTemplate:
    path: str
    mtime_ns: int
    header_row: int
    col_title: int
    col_short: int
    rows: list[SziRow]


def _find_szi_sheet_name(wb) -> str:
    if SZI_SHEET_NAME in wb.sheetnames:
        return SZI_SHEET_NAME
    for s in wb.sheetnames:
        if "СЗИ" in s:
            return s
    return wb.sheetnames[0]


def _find_header_row_from_texts(scan_rows: dict[int, list[str]]) -> int | None:
    """
    Ищем строку заголовка основной таблицы по признакам:
    - есть "Сокращ" и хотя бы один из "КБ1/КБ2/КБ3"
    """
    for r in sorted(scan_rows.keys()):
        joined = " | ".join([t for t in (scan_rows.get(r) or []) if t])
        if "Сокращ" in joined and ("КБ1" in joined or "КБ 1" in joined or "КБ2" in joined or "КБ3" in joined):
            return r
    return None


def _col_letters_to_index(s: str) -> int:
    """Excel column letters (A, B, ..., AA) -> 1-based index."""
    out = 0
    for ch in s:
        if not ("A" <= ch <= "Z"):
            continue
        out = out * 26 + (ord(ch) - ord("A") + 1)
    return out


def _parse_cell_ref(a1: str) -> tuple[int, int] | None:
    """Cell ref like 'C12' -> (row=12, col=3)."""
    if not a1:
        return None
    col_part = []
    row_part = []
    for ch in a1:
        if ch.isalpha():
            col_part.append(ch.upper())
        elif ch.isdigit():
            row_part.append(ch)
    if not col_part or not row_part:
        return None
    col = _col_letters_to_index("".join(col_part))
    row = int("".join(row_part))
    return row, col


def _xlsx_read_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    try:
        xml = zf.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    root = ET.fromstring(xml)
    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    out: list[str] = []
    for si in root.findall(".//s:si", ns):
        # shared string may have multiple <t> fragments (rich text)
        parts = [t.text or "" for t in si.findall(".//s:t", ns)]
        out.append("".join(parts))
    return out


def _xlsx_sheet_xml_path(zf: zipfile.ZipFile, sheet_name: str) -> str:
    ns = {
        "s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }
    wb_xml = ET.fromstring(zf.read("xl/workbook.xml"))
    rid: str | None = None
    fallback_rid: str | None = None
    for sh in wb_xml.findall(".//s:sheets/s:sheet", ns):
        nm = sh.attrib.get("name", "")
        if not fallback_rid:
            fallback_rid = sh.attrib.get(f"{{{ns['r']}}}id")
        if nm == sheet_name or ("СЗИ" in nm and "СЗИ" in sheet_name):
            rid = sh.attrib.get(f"{{{ns['r']}}}id")
            break
    rid = rid or fallback_rid
    if not rid:
        raise RuntimeError("Не удалось найти лист в xl/workbook.xml")

    rels_xml = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rel_ns = {"rel": "http://schemas.openxmlformats.org/package/2006/relationships"}
    target: str | None = None
    for rel in rels_xml.findall(".//rel:Relationship", rel_ns):
        if rel.attrib.get("Id") == rid:
            target = rel.attrib.get("Target")
            break
    if not target:
        raise RuntimeError("Не удалось разрешить путь листа (workbook.xml.rels)")
    target = target.lstrip("/")
    if not target.startswith("xl/"):
        target = "xl/" + target
    return target


def _xlsx_cell_text(c: ET.Element, shared: list[str]) -> str:
    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    t = c.attrib.get("t", "")
    if t == "inlineStr":
        txts = [e.text or "" for e in c.findall(".//s:is/s:t", ns)]
        return "".join(txts).strip()
    v = c.find("s:v", ns)
    if v is None or v.text is None:
        # Sometimes strings are stored as <is><t> even without inlineStr (rare)
        txts = [e.text or "" for e in c.findall(".//s:is/s:t", ns)]
        return "".join(txts).strip()
    raw = v.text.strip()
    if t == "s":
        try:
            return str(shared[int(raw)]).strip()
        except Exception:
            return ""
    return raw


def _xlsx_scan_header_texts(path: str, sheet_name: str, *, max_rows: int = 120, max_cols: int = 40) -> dict[int, list[str]]:
    """
    Быстро читаем первые строки листа из XLSX (zip+xml), без openpyxl.
    Возвращаем dict[row] -> list[texts] для row<=max_rows и col<=max_cols.
    """
    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(path) as zf:
        shared = _xlsx_read_shared_strings(zf)
        sheet_xml_path = _xlsx_sheet_xml_path(zf, sheet_name)
        # iterparse to avoid huge memory usage
        scan: dict[int, list[str]] = {}
        with zf.open(sheet_xml_path) as fp:
            context = ET.iterparse(fp, events=("end",))
            for ev, elem in context:
                if elem.tag.endswith("}row"):
                    r_attr = elem.attrib.get("r")
                    try:
                        r = int(r_attr) if r_attr else None
                    except Exception:
                        r = None
                    if not r:
                        elem.clear()
                        continue
                    if r > max_rows:
                        elem.clear()
                        break
                    texts: list[str] = []
                    for c in elem.findall("s:c", ns):
                        ref = c.attrib.get("r", "")
                        parsed = _parse_cell_ref(ref)
                        if not parsed:
                            continue
                        rr, cc = parsed
                        if rr != r or cc > max_cols:
                            continue
                        val = _xlsx_cell_text(c, shared)
                        if val:
                            texts.append(val)
                    scan[r] = texts
                    elem.clear()
        return scan


def _xlsx_iter_col_values(path: str, sheet_name: str, *, start_row: int, col_indices: tuple[int, int]) -> list[tuple[int, str, str]]:
    """
    Возвращает список (row, colA_text, colB_text) для указанных колонок (1-based),
    начиная со строки start_row включительно, по всему листу.
    """
    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    col_a, col_b = col_indices
    out: list[tuple[int, str, str]] = []
    with zipfile.ZipFile(path) as zf:
        shared = _xlsx_read_shared_strings(zf)
        sheet_xml_path = _xlsx_sheet_xml_path(zf, sheet_name)
        with zf.open(sheet_xml_path) as fp:
            context = ET.iterparse(fp, events=("end",))
            for ev, elem in context:
                if elem.tag.endswith("}row"):
                    r_attr = elem.attrib.get("r")
                    try:
                        r = int(r_attr) if r_attr else None
                    except Exception:
                        r = None
                    if not r or r < start_row:
                        elem.clear()
                        continue
                    a = ""
                    b = ""
                    for c in elem.findall("s:c", ns):
                        ref = c.attrib.get("r", "")
                        parsed = _parse_cell_ref(ref)
                        if not parsed:
                            continue
                        rr, cc = parsed
                        if rr != r:
                            continue
                        if cc == col_a:
                            a = _xlsx_cell_text(c, shared)
                        elif cc == col_b:
                            b = _xlsx_cell_text(c, shared)
                    out.append((r, a, b))
                    elem.clear()
    return out


@lru_cache(maxsize=8)
def _load_szi_template_cached(path: str, mtime_ns: int) -> SziTemplate:
    # NOTE: openpyxl can take 60s+ on large templates with extensions/formatting.
    # For SZI we only need a couple of columns, so we parse XLSX directly (zip+xml).
    sheet_name = SZI_SHEET_NAME
    scan = _xlsx_scan_header_texts(path, sheet_name, max_rows=120, max_cols=40)
    hdr = _find_header_row_from_texts(scan)
    if not hdr:
        raise RuntimeError("Не найдена строка заголовка (Сокращенное/КБ1/КБ2/КБ3) на листе 'СЗИ'")

    col_title = 2  # B
    col_short = 3  # C

    rows: list[SziRow] = []
    current_group_title = ""
    current_group_code = ""
    started = False  # start from the main table (first real SZI.* item), skip the top summary/formula blocks

    # NOTE: В листе бывают "свернутые"/разнесённые блоки и большие промежутки пустых строк.
    # Поэтому НЕ обрываем парсинг по пустой серии, а идём до конца используемого диапазона.
    for r, title_v, short_v in _xlsx_iter_col_values(path, sheet_name, start_row=hdr + 1, col_indices=(col_title, col_short)):
        title = title_v.strip() if isinstance(title_v, str) else ""
        short = short_v.strip() if isinstance(short_v, str) else ""
        if short.startswith("=") or title.startswith("="):
            continue

        if not title and not short:
            continue

        # Group header rows: no dot in short (e.g. "ВПО", "2FA")
        if title and short and "." not in short:
            current_group_title = title
            current_group_code = short
            if started:
                rows.append(SziRow(kind="group", row_key=f"group:{short}", title=title, short_name=short, group_code=short))
            continue

        # Item rows: token with dots
        if short and _TOKEN_RE.match(short):
            # We only consider SZI items for this sheet. Other tokens (e.g. УИБ.*, РЕАГ.*) exist
            # in helper/queue blocks on the same sheet and must be ignored.
            if not short.upper().startswith("СЗИ."):
                continue
            if not started:
                # first real item marks the beginning of the main table
                started = True
                # Ensure we include current group header for the first block.
                if current_group_title and current_group_code:
                    rows.append(
                        SziRow(
                            kind="group",
                            row_key=f"group:{current_group_code}",
                            title=current_group_title,
                            short_name=current_group_code,
                            group_code=current_group_code,
                        )
                    )

            rows.append(
                SziRow(
                    kind="item",
                    row_key=short.upper(),
                    title=(title or current_group_title),
                    short_name=short,
                    group_code=current_group_code,
                )
            )

    return SziTemplate(path=path, mtime_ns=mtime_ns, header_row=hdr, col_title=col_title, col_short=col_short, rows=rows)


def get_szi_template(path: str) -> SziTemplate:
    st = os.stat(path)
    return _load_szi_template_cached(path, st.st_mtime_ns)


def _get_template_rev(db: Session, sheet_name: str) -> int:
    """
    Lightweight "version" for caches: monotonically non-decreasing as we insert rows.
    """
    rev = db.query(func.max(IndexKbTemplateRow.id)).filter(IndexKbTemplateRow.sheet_name == sheet_name).scalar()
    return int(rev or 0)


def _load_template_rows_from_db(db: Session, sheet_name: str) -> list[SziRow]:
    rows = (
        db.query(IndexKbTemplateRow)
        .filter(IndexKbTemplateRow.sheet_name == sheet_name)
        .order_by(IndexKbTemplateRow.sort_order.asc(), IndexKbTemplateRow.id.asc())
        .all()
    )
    out: list[SziRow] = []
    for r in rows:
        out.append(
            SziRow(
                kind=(r.kind or "item"),
                row_key=(r.row_key or ""),
                title=(r.title or ""),
                short_name=(r.short_name or ""),
                group_code=(r.group_code or ""),
            )
        )
    return out


def get_szi_template_from_db(db: Session) -> SziTemplate | None:
    """
    Основной путь: структура листа берётся из БД (без парсинга Excel на запрос).
    """
    rows = _load_template_rows_from_db(db, SZI_SHEET_NAME)
    if not rows:
        return None
    rev = _get_template_rev(db, SZI_SHEET_NAME)
    return SziTemplate(path="", mtime_ns=rev, header_row=0, col_title=2, col_short=3, rows=rows)


def get_szi_template_rev(db: Session) -> int:
    return _get_template_rev(db, SZI_SHEET_NAME)


def ensure_szi_template_loaded(db: Session, *, template_path: str | None, force: bool = False) -> int:
    """
    Загружает структуру листа СЗИ в БД (один раз).
    Возвращает количество строк в шаблоне.

    Примечание: в проде лучше запускать отдельной командой (CLI), а не ждать первого UI-запроса.
    """
    existing_cnt = db.query(func.count(IndexKbTemplateRow.id)).filter(IndexKbTemplateRow.sheet_name == SZI_SHEET_NAME).scalar()
    if int(existing_cnt or 0) > 0 and not force:
        return int(existing_cnt or 0)

    if not template_path or not os.path.exists(template_path):
        raise RuntimeError("Шаблон СЗИ не загружен в БД и Excel-эталон не найден. Нужна загрузка шаблона в БД.")

    tpl = get_szi_template(template_path)
    # Replace all rows for sheet atomically.
    db.query(IndexKbTemplateRow).filter(IndexKbTemplateRow.sheet_name == SZI_SHEET_NAME).delete(synchronize_session=False)
    for i, r in enumerate(tpl.rows, start=1):
        db.add(
            IndexKbTemplateRow(
                sheet_name=SZI_SHEET_NAME,
                sort_order=i,
                kind=r.kind,
                row_key=r.row_key,
                title=r.title,
                short_name=r.short_name,
                group_code=r.group_code,
            )
        )
    db.commit()
    return len(tpl.rows)


def compute_auto_scores(db: Session, org_id: int, short_names: list[str]) -> dict[str, float]:
    raise RuntimeError("use compute_auto_scores_v2()")


def _period_bounds_utc(*, date_from: date, date_to: date) -> tuple[datetime, datetime]:
    start = datetime.combine(date_from, datetime.min.time())
    end = datetime.combine(date_to, datetime.min.time()) + timedelta(days=1)
    return start, end


def _get_selected_period_bounds(
    db: Session,
    *,
    org: Organization,
    period_id: int | None,
) -> tuple[AuditPeriod | None, datetime | None, datetime | None]:
    pid = int(period_id or 0) or int(getattr(org, "audit_period_id", 0) or 0)
    if not pid:
        return None, None, None
    ap = db.get(AuditPeriod, pid)
    if not ap:
        return None, None, None
    df = getattr(ap, "date_from", None)
    dt = getattr(ap, "date_to", None)
    if not df or not dt:
        return ap, None, None
    try:
        start, end = _period_bounds_utc(date_from=df, date_to=dt)
        return ap, start, end
    except Exception:
        return ap, None, None


def _allowed_artifact_ids_subquery(db: Session, *, org: Organization):
    level_id = getattr(org, "artifact_level_id", None)
    if not level_id:
        return None
    lvl = db.get(ArtifactLevel, int(level_id))
    if not lvl:
        return None
    return (
        db.query(ArtifactLevelItem.artifact_id)
        .join(ArtifactLevel, ArtifactLevel.id == ArtifactLevelItem.level_id)
        .filter(ArtifactLevel.sort_order <= lvl.sort_order)
        .subquery()
    )


def compute_auto_scores_v2(
    db: Session,
    *,
    org: Organization,
    short_names: list[str],
    period_id: int | None,
) -> tuple[dict[str, float], set[str], set[str]]:
    """
    Auto-score:
    - only artifacts within org effective level
    - for selected calendar period:
      - take latest uploaded version in the period per org_artifact
      - 5 only if that version is audited (approved) in the same period
    """
    if not short_names:
        return {}, set(), set()
    sns = sorted({s.upper() for s in short_names if s})

    art_rows = (
        db.query(Artifact.id, func.upper(Artifact.short_name).label("sn_u"))
        .filter(Artifact.short_name != "", func.upper(Artifact.short_name).in_(sns))
        .all()
    )
    if not art_rows:
        return {}, set(), set()
    id_to_sn: dict[int, str] = {int(aid): str(sn_u).upper() for (aid, sn_u) in art_rows if aid and sn_u}
    existing_all = set(id_to_sn.values())
    artifact_ids_all = sorted(id_to_sn.keys())

    allowed_sub = _allowed_artifact_ids_subquery(db, org=org)
    if allowed_sub is not None:
        allowed_ids = {
            int(aid)
            for (aid,) in db.query(Artifact.id)
            .filter(Artifact.id.in_(artifact_ids_all), Artifact.id.in_(allowed_sub))
            .all()
            if aid
        }
    else:
        allowed_ids = set(artifact_ids_all)
    artifact_ids = sorted(allowed_ids)
    existing_allowed = {id_to_sn[i] for i in artifact_ids if i in id_to_sn}
    excluded = set(existing_all) - set(existing_allowed)
    if not artifact_ids:
        return {}, excluded, set()

    _, p_start, p_end = _get_selected_period_bounds(db, org=org, period_id=period_id)
    if p_start and p_end:
        sub = (
            db.query(
                FileVersion.org_artifact_id.label("oa_id"),
                func.max(FileVersion.id).label("fv_id"),
            )
            .filter(FileVersion.created_at >= p_start, FileVersion.created_at < p_end)
            .group_by(FileVersion.org_artifact_id)
            .subquery()
        )
        audited_flag = case(
            (
                and_(
                    sub.c.fv_id.isnot(None),
                    OrgArtifact.audited_file_version_id.isnot(None),
                    OrgArtifact.audited_file_version_id == sub.c.fv_id,
                    OrgArtifact.review_status == OrgArtifactReviewStatus.approved,
                    OrgArtifact.audited_at.isnot(None),
                    OrgArtifact.audited_at >= p_start,
                    OrgArtifact.audited_at < p_end,
                ),
                1,
            ),
            else_=0,
        )
        rows = (
            db.query(OrgArtifact.artifact_id.label("aid"), func.min(audited_flag).label("all_audited"))
            .outerjoin(sub, sub.c.oa_id == OrgArtifact.id)
            .filter(OrgArtifact.org_id == int(org.id), OrgArtifact.artifact_id.in_(artifact_ids))
            .group_by(OrgArtifact.artifact_id)
            .all()
        )
    else:
        audited_flag = case(
            (
                and_(
                    OrgArtifact.current_file_version_id.isnot(None),
                    OrgArtifact.audited_file_version_id.isnot(None),
                    OrgArtifact.audited_file_version_id == OrgArtifact.current_file_version_id,
                    OrgArtifact.review_status == OrgArtifactReviewStatus.approved,
                ),
                1,
            ),
            else_=0,
        )
        rows = (
            db.query(OrgArtifact.artifact_id.label("aid"), func.min(audited_flag).label("all_audited"))
            .filter(OrgArtifact.org_id == int(org.id), OrgArtifact.artifact_id.in_(artifact_ids))
            .group_by(OrgArtifact.artifact_id)
            .all()
        )

    out: dict[str, float] = {}
    for aid, all_audited in rows:
        sn_u = id_to_sn.get(int(aid or 0))
        if not sn_u:
            continue
        out[sn_u] = 5.0 if int(all_audited or 0) == 1 else 0.0
    for sn_u in existing_allowed:
        out.setdefault(sn_u, 0.0)
    return out, excluded, set(existing_allowed)


def load_manual_values(db: Session, org_id: int, sheet_name: str) -> dict[str, IndexKbManualValue]:
    rows = (
        db.query(IndexKbManualValue)
        .options(joinedload(IndexKbManualValue.updated_by))
        .filter(IndexKbManualValue.org_id == org_id, IndexKbManualValue.sheet_name == sheet_name)
        .all()
    )
    return {r.row_key: r for r in rows}


@dataclass(frozen=True)
class SziSummaryRow:
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


def compute_szi_summary(rows: list[SziRowView]) -> list[SziSummaryRow]:
    out: list[SziSummaryRow] = []
    cur_group_title = ""
    cur_group_code = ""
    acc: list[float] = []

    def flush() -> None:
        nonlocal acc, cur_group_title, cur_group_code
        if not cur_group_code:
            return
        m = _mean(acc)
        out.append(
            SziSummaryRow(
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


def build_szi_view(
    db: Session,
    *,
    org_id: int,
    actor: User,
    period_id: int | None = None,
) -> tuple[Organization, SziTemplate, list[SziRowView]]:
    org = db.get(Organization, org_id)
    if not org:
        raise RuntimeError("Организация не найдена")

    tpl = get_szi_template_from_db(db)
    if not tpl:
        # Important: do NOT parse Excel on first request. Seed must be done via Alembic migration.
        raise RuntimeError("Шаблон СЗИ не загружен в БД (нужна seed-миграция)")
    short_names = [r.short_name for r in tpl.rows if r.kind == "item" and r.short_name]

    # Small TTL cache for auto scores (audited==current) for snappy UI.
    # Keyed by (org_id, template_mtime_ns).
    global _SZI_AUTO_CACHE
    try:
        _SZI_AUTO_CACHE
    except NameError:
        _SZI_AUTO_CACHE = {}  # type: ignore[var-annotated]
    cache_key = (int(org_id), int(tpl.mtime_ns), int(period_id or 0), int(getattr(org, "audit_period_id", 0) or 0))
    now = time.time()
    cached = _SZI_AUTO_CACHE.get(cache_key)  # type: ignore[name-defined]
    if cached and (now - float(cached[0])) < 15.0:
        auto, excluded = cached[1], cached[2]
    else:
        auto, excluded, _existing_allowed = compute_auto_scores_v2(db, org=org, short_names=short_names, period_id=period_id)
        _SZI_AUTO_CACHE[cache_key] = (now, auto, excluded)  # type: ignore[name-defined]

    manual = load_manual_values(db, org_id, SZI_SHEET_NAME)
    # Tokens present on the sheet but missing in Artifact справочнике.
    # We still show them (as before), but mark explicitly.
    sn_all = {s.upper() for s in short_names if s}
    sn_have = {k.upper() for k in (auto or {}).keys()}
    sn_missing_artifact = sn_all - sn_have - set(excluded)

    out: list[SziRowView] = []
    for r in tpl.rows:
        if r.kind == "group":
            out.append(SziRowView(row=r, is_auto=True, value=None, source="group", updated_at=None, updated_by=""))
            continue
        key = r.row_key
        sn_u = r.short_name.upper()
        if sn_u in excluded:
            out.append(SziRowView(row=r, is_auto=True, value=None, source="вне уровня", updated_at=None, updated_by=""))
            continue
        if sn_u in auto:
            out.append(SziRowView(row=r, is_auto=True, value=auto[sn_u], source="auto", updated_at=None, updated_by=""))
            continue
        if sn_u in sn_missing_artifact:
            # Keep UX consistent: row exists, but we can't auto-score since there's no artifact in reference.
            out.append(SziRowView(row=r, is_auto=False, value=0.0, source="нет артефакта", updated_at=None, updated_by=""))
            continue
        mv = manual.get(key)
        if mv:
            out.append(
                SziRowView(
                    row=r,
                    is_auto=False,
                    value=float(mv.value),
                    source="manual",
                    updated_at=mv.updated_at,
                    updated_by=mv.updated_by.login if mv.updated_by else "",
                )
            )
        else:
            out.append(SziRowView(row=r, is_auto=False, value=None, source="empty", updated_at=None, updated_by=""))

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

