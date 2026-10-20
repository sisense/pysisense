from __future__ import annotations

from ..migration.datasecurity import DatasecurityMigrationMixin
from .base import _deprecated_migration_alias


# Deprecated: the implementation lives in pysisense/migration/datasecurity.py and Migration
# exposes the same methods. These aliases only add the deprecation marker.
class DatasecurityMergeMixin(DatasecurityMigrationMixin):
    migrate_datasecurity = _deprecated_migration_alias(DatasecurityMigrationMixin.migrate_datasecurity)
    migrate_all_datasecurity = _deprecated_migration_alias(DatasecurityMigrationMixin.migrate_all_datasecurity)
