from __future__ import annotations

from ..migration.folder import FolderMigrationMixin
from .base import _deprecated_migration_alias


# Deprecated: the implementation lives in pysisense/migration/folder.py and Migration
# exposes the same methods. These aliases only add the deprecation marker.
class FolderMergeMixin(FolderMigrationMixin):
    migrate_folders = _deprecated_migration_alias(FolderMigrationMixin.migrate_folders)
    migrate_all_folders = _deprecated_migration_alias(FolderMigrationMixin.migrate_all_folders)
