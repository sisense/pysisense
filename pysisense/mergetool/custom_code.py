from __future__ import annotations

from ..migration.custom_code import CustomCodeMigrationMixin


# Thin subclass kept for MergeTool; the implementation lives in pysisense/migration/custom_code.py.
class CustomCodeMergeMixin(CustomCodeMigrationMixin):
    pass
