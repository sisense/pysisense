from __future__ import annotations

from ..migration.dashboards_v2 import DashboardsV2MigrationMixin
from .base import _deprecated_migration_alias


# Deprecated: the implementation lives in pysisense/migration/dashboards_v2.py. MergeTool's
# migrate_dashboards / migrate_all_dashboards are Migration's *_v2 methods under their old names;
# the inherited *_v2 names are marked deprecated too, so every MergeTool method warns.
class DashboardMergeMixin(DashboardsV2MigrationMixin):
    migrate_dashboards = _deprecated_migration_alias(DashboardsV2MigrationMixin.migrate_dashboards_v2, name="migrate_dashboards")
    migrate_all_dashboards = _deprecated_migration_alias(DashboardsV2MigrationMixin.migrate_all_dashboards_v2, name="migrate_all_dashboards")
    migrate_dashboards_v2 = _deprecated_migration_alias(DashboardsV2MigrationMixin.migrate_dashboards_v2)
    migrate_all_dashboards_v2 = _deprecated_migration_alias(DashboardsV2MigrationMixin.migrate_all_dashboards_v2)
