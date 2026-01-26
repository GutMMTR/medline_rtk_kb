from __future__ import annotations

import tempfile
from dataclasses import dataclass

from xlcalculator import ModelCompiler
from xlcalculator import Evaluator


@dataclass(frozen=True)
class FormulaEvaluator:
    evaluator: Evaluator

    def eval(self, sheet_name: str, cell_addr: str):
        # xlcalculator wants "'Sheet Name'!A1" for names with spaces
        ref = f"'{sheet_name}'!{cell_addr}"
        return self.evaluator.evaluate(ref)


def build_evaluator_from_openpyxl_workbook(wb) -> FormulaEvaluator:
    # xlcalculator works with a saved xlsx archive
    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        wb.save(tmp.name)
        compiler = ModelCompiler()
        model = compiler.read_and_parse_archive(tmp.name)
        return FormulaEvaluator(evaluator=Evaluator(model))

