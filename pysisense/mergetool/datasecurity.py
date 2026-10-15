from __future__ import annotations

from ..migration.datasecurity import DatasecurityMigrationMixin


# Thin subclass kept for MergeTool; the implementation lives in pysisense/migration/datasecurity.py.
class DatasecurityMergeMixin(DatasecurityMigrationMixin):
    pass
