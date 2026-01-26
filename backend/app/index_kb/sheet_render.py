from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.utils.cell import range_boundaries


@dataclass(frozen=True)
class RenderCell:
    row: int
    col: int
    value: str
    rowspan: int
    colspan: int
    css_class: str


def _sheet_bounds(ws: Worksheet) -> tuple[int, int, int, int]:
    dim = ws.calculate_dimension()  # e.g. "A1:Z200"
    min_col, min_row, max_col, max_row = range_boundaries(dim)
    return min_row, max_row, min_col, max_col


def build_merge_map(ws: Worksheet) -> dict[tuple[int, int], tuple[int, int, int, int]]:
    """Map top-left cell (row,col) -> (min_row,max_row,min_col,max_col)."""
    out: dict[tuple[int, int], tuple[int, int, int, int]] = {}
    for cr in ws.merged_cells.ranges:
        min_col, min_row, max_col, max_row = range_boundaries(str(cr))
        out[(min_row, min_col)] = (min_row, max_row, min_col, max_col)
    return out


def build_covered_cells(ws: Worksheet) -> set[tuple[int, int]]:
    covered: set[tuple[int, int]] = set()
    for cr in ws.merged_cells.ranges:
        min_col, min_row, max_col, max_row = range_boundaries(str(cr))
        for r in range(min_row, max_row + 1):
            for c in range(min_col, max_col + 1):
                covered.add((r, c))
        covered.discard((min_row, min_col))  # keep top-left
    return covered


def iter_render_rows(
    ws: Worksheet,
    get_cell_text: callable,
    get_cell_class: callable,
    max_cells: int = 30000,
) -> Iterable[list[RenderCell]]:
    min_row, max_row, min_col, max_col = _sheet_bounds(ws)
    merges = build_merge_map(ws)
    covered = build_covered_cells(ws)

    emitted = 0
    for r in range(min_row, max_row + 1):
        row_cells: list[RenderCell] = []
        for c in range(min_col, max_col + 1):
            if (r, c) in covered:
                continue
            cell = ws.cell(row=r, column=c)
            text = get_cell_text(cell)
            css = get_cell_class(cell, text)
            rowspan = 1
            colspan = 1
            m = merges.get((r, c))
            if m:
                mr1, mr2, mc1, mc2 = m
                rowspan = mr2 - mr1 + 1
                colspan = mc2 - mc1 + 1
            row_cells.append(
                RenderCell(
                    row=r,
                    col=c,
                    value=text,
                    rowspan=rowspan,
                    colspan=colspan,
                    css_class=css,
                )
            )
            emitted += 1
            if emitted >= max_cells:
                return
        yield row_cells

