from __future__ import annotations

from ..migration.groups_v2 import GroupsV2MigrationMixin
from .base import _deprecated_migration_alias


# Deprecated: the implementation lives in pysisense/migration/groups_v2.py. MergeTool's
# migrate_groups / migrate_all_groups are Migration's *_v2 methods under their old names;
# the inherited *_v2 names are marked deprecated too, so every MergeTool method warns.
class GroupsMergeMixin(GroupsV2MigrationMixin):
    migrate_groups = _deprecated_migration_alias(GroupsV2MigrationMixin.migrate_groups_v2, name="migrate_groups")
    migrate_all_groups = _deprecated_migration_alias(GroupsV2MigrationMixin.migrate_all_groups_v2, name="migrate_all_groups")
    migrate_groups_v2 = _deprecated_migration_alias(GroupsV2MigrationMixin.migrate_groups_v2)
    migrate_all_groups_v2 = _deprecated_migration_alias(GroupsV2MigrationMixin.migrate_all_groups_v2)
