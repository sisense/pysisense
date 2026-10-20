from __future__ import annotations

from ..migration.users_v2 import UsersV2MigrationMixin
from .base import _deprecated_migration_alias


# Deprecated: the implementation lives in pysisense/migration/users_v2.py. MergeTool's
# migrate_users / migrate_all_users are Migration's *_v2 methods under their old names;
# the inherited *_v2 names are marked deprecated too, so every MergeTool method warns.
class UsersMergeMixin(UsersV2MigrationMixin):
    migrate_users = _deprecated_migration_alias(UsersV2MigrationMixin.migrate_users_v2, name="migrate_users")
    migrate_all_users = _deprecated_migration_alias(UsersV2MigrationMixin.migrate_all_users_v2, name="migrate_all_users")
    migrate_users_v2 = _deprecated_migration_alias(UsersV2MigrationMixin.migrate_users_v2)
    migrate_all_users_v2 = _deprecated_migration_alias(UsersV2MigrationMixin.migrate_all_users_v2)
