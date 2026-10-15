from __future__ import annotations

from ..migration.filters import FiltersMigrationMixin


# Thin subclass kept for MergeTool; the implementation lives in pysisense/migration/filters.py.
class FiltersMergeMixin(FiltersMigrationMixin):
    pass
