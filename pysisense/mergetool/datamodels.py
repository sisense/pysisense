from __future__ import annotations

from ..migration.datamodels_v2 import DatamodelsV2MigrationMixin
from .base import _deprecated_migration_alias


# Deprecated: the implementation lives in pysisense/migration/datamodels_v2.py. MergeTool's
# migrate_datamodels / migrate_all_datamodels are Migration's *_v2 methods under their old names;
# the inherited *_v2 names are marked deprecated too, so every MergeTool method warns.
class DatamodelsMergeMixin(DatamodelsV2MigrationMixin):
    migrate_datamodels = _deprecated_migration_alias(DatamodelsV2MigrationMixin.migrate_datamodels_v2, name="migrate_datamodels")
    migrate_all_datamodels = _deprecated_migration_alias(DatamodelsV2MigrationMixin.migrate_all_datamodels_v2, name="migrate_all_datamodels")
    migrate_datamodels_v2 = _deprecated_migration_alias(DatamodelsV2MigrationMixin.migrate_datamodels_v2)
    migrate_all_datamodels_v2 = _deprecated_migration_alias(DatamodelsV2MigrationMixin.migrate_all_datamodels_v2)
