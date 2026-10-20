from __future__ import annotations

from ..migration.formulas import FormulasMigrationMixin
from .base import _deprecated_migration_alias


# Deprecated: the implementation lives in pysisense/migration/formulas.py and Migration
# exposes the same methods. These aliases only add the deprecation marker.
class FormulasMergeMixin(FormulasMigrationMixin):
    migrate_saved_formulas = _deprecated_migration_alias(FormulasMigrationMixin.migrate_saved_formulas)
    migrate_all_saved_formulas = _deprecated_migration_alias(FormulasMigrationMixin.migrate_all_saved_formulas)
