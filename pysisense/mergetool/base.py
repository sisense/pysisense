from __future__ import annotations

from typing_extensions import deprecated

from ..migration import Migration
from ..migration.base import MigrationBaseMixin


# Deprecated alias: _run_concurrently now lives on MigrationBaseMixin
# (pysisense/migration/base.py). Kept for one deprecation window so imports of
# pysisense.mergetool.base keep working; subclassing it emits a DeprecationWarning.
@deprecated("use MigrationBaseMixin")
class MergeToolConcurrencyMixin(MigrationBaseMixin):
    pass


class MergeToolDelegationMixin:
    def _as_migration(self) -> Migration:
        """Return a Migration on this MergeTool's source and target clients.

        Every MergeTool method is a deprecated wrapper that runs the matching
        Migration method through this object.
        """
        return Migration(source_client=self.source_client, target_client=self.target_client)
