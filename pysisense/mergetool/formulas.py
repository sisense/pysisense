from __future__ import annotations

from ..migration.formulas import FormulasMigrationMixin


# Thin subclass kept for MergeTool; the implementation lives in pysisense/migration/formulas.py.
class FormulasMergeMixin(FormulasMigrationMixin):
    pass
