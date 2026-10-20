from __future__ import annotations

from ..migration.filters import FiltersMigrationMixin
from .base import _deprecated_migration_alias


# Deprecated: the implementation lives in pysisense/migration/filters.py and Migration
# exposes the same methods. These aliases only add the deprecation marker.
class FiltersMergeMixin(FiltersMigrationMixin):
    migrate_saved_filters = _deprecated_migration_alias(FiltersMigrationMixin.migrate_saved_filters)
    migrate_all_saved_filters = _deprecated_migration_alias(FiltersMigrationMixin.migrate_all_saved_filters)
