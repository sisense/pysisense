from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any

from typing_extensions import deprecated

from ..migration.base import MigrationBaseMixin


# Deprecated alias: _run_concurrently now lives on MigrationBaseMixin
# (pysisense/migration/base.py). Kept for one deprecation window so imports of
# pysisense.mergetool.base keep working; subclassing it emits a DeprecationWarning.
@deprecated("use MigrationBaseMixin")
class MergeToolConcurrencyMixin(MigrationBaseMixin):
    pass


def _deprecated_migration_alias(func: Callable[..., Any], name: str | None = None) -> Callable[..., Any]:
    """Wrap a Migration method as a deprecated MergeTool method.

    The wrapper keeps the original signature and docstring (``functools.wraps``),
    carries the PEP 702 marker ``"use Migration.<func name>"``, and adds a
    one-line deprecation note to the docstring. ``name`` is the attribute name
    on MergeTool when it differs from the Migration method's name.

    ``typing_extensions.deprecated`` also sets ``__deprecated__`` on the
    function it decorates, so it is applied to a separate forwarding function:
    decorating ``func`` itself would mark the Migration method as deprecated.
    """

    @functools.wraps(func)
    def forward(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    if name is not None:
        forward.__name__ = name
        forward.__qualname__ = name

    wrapper = deprecated(f"use Migration.{func.__name__}")(forward)
    summary, _, rest = (func.__doc__ or "").partition("\n")
    wrapper.__doc__ = f"{summary}\n\n        Deprecated: use ``Migration.{func.__name__}``, which runs the same code.\n{rest}"
    return wrapper
