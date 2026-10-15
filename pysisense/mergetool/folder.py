from __future__ import annotations

from ..migration.folder import FolderMigrationMixin


# Thin subclass kept for MergeTool; the implementation lives in pysisense/migration/folder.py.
class FolderMergeMixin(FolderMigrationMixin):
    pass
