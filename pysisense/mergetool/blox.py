from __future__ import annotations

from ..migration.blox import BloxMigrationMixin
from .base import _deprecated_migration_alias


# Deprecated: the implementation lives in pysisense/migration/blox.py and Migration
# exposes the same methods. These aliases only add the deprecation marker.
class BloxMergeMixin(BloxMigrationMixin):
    migrate_blox_actions = _deprecated_migration_alias(BloxMigrationMixin.migrate_blox_actions)
    migrate_all_blox_actions = _deprecated_migration_alias(BloxMigrationMixin.migrate_all_blox_actions)
