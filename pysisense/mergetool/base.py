from __future__ import annotations

from typing_extensions import deprecated

from ..migration.base import MigrationBaseMixin


# Deprecated alias: _run_concurrently now lives on MigrationBaseMixin
# (pysisense/migration/base.py). Kept for one deprecation window so imports of
# pysisense.mergetool.base keep working; subclassing it emits a DeprecationWarning.
@deprecated("use MigrationBaseMixin")
class MergeToolConcurrencyMixin(MigrationBaseMixin):
    pass
