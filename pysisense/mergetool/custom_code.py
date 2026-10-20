from __future__ import annotations

from ..migration.custom_code import CustomCodeMigrationMixin
from .base import _deprecated_migration_alias


# Deprecated: the implementation lives in pysisense/migration/custom_code.py and Migration
# exposes the same methods. These aliases only add the deprecation marker.
class CustomCodeMergeMixin(CustomCodeMigrationMixin):
    migrate_notebooks = _deprecated_migration_alias(CustomCodeMigrationMixin.migrate_notebooks)
    migrate_all_notebooks = _deprecated_migration_alias(CustomCodeMigrationMixin.migrate_all_notebooks)
