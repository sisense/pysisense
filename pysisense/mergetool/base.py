from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any


class MergeToolConcurrencyMixin:
    def _run_concurrently(
        self,
        items: list[Any],
        worker: Callable[[Any], None],
        concurrency: int,
        context_label: str,
    ) -> None:
        """Run ``worker(item)`` once per item in ``items``, optionally in parallel.

        The SDK's HTTP client (``requests``) is synchronous, so this is a
        concurrency *scheduler*, not a non-blocking I/O mechanism: each
        ``worker`` call runs in a background thread via ``asyncio.to_thread``,
        bounded by an ``asyncio.Semaphore(concurrency)``. ``worker`` is
        expected to record its own outcome as a side effect (e.g. appending to
        a shared ``summary`` dict) rather than return a value.

        Sequential (``concurrency <= 1`` or a single item) skips asyncio
        entirely and just calls ``worker`` in a plain loop — this is the
        default and preserves the exact behavior migrations had before
        concurrency support existed.

        Parameters
        ----------
        items : list[Any]
            Items to process. Must be safe to migrate independently of each
            other — callers with ordering dependencies (e.g. a folder
            hierarchy, parents before children) must batch ``items`` into
            dependency-respecting groups and call this once per group.
        worker : Callable[[Any], None]
            Synchronous callable invoked once per item.
        concurrency : int
            Maximum number of items processed at once.
        context_label : str
            Short description used only in the nested-event-loop fallback
            warning (for example ``"dashboards"`` or ``"depth 2"``).

        Notes
        -----
        If called from code that is already running an asyncio event loop,
        falls back to sequential processing (a nested event loop cannot be
        started via ``asyncio.run``) and logs a warning.
        """
        if concurrency <= 1 or len(items) <= 1:
            for item in items:
                worker(item)
            return

        async def _run_all() -> None:
            semaphore = asyncio.Semaphore(concurrency)

            async def _run_one(item: Any) -> None:
                async with semaphore:
                    await asyncio.to_thread(worker, item)

            await asyncio.gather(*(_run_one(item) for item in items))

        try:
            asyncio.run(_run_all())
        except RuntimeError:
            self.logger.warning("An asyncio event loop is already running — falling back to sequential processing for %s.", context_label)
            for item in items:
                worker(item)
