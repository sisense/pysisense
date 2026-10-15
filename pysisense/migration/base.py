from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any


class MigrationBaseMixin:
    def _emit(
        self,
        emit: Callable[[dict[str, Any]], None] | None,
        event: dict[str, Any],
    ) -> None:
        """
        Safely emit a progress event to the provided callback.

        Parameters
        ----------
        emit : Callable[[Dict[str, Any]], None] or None
            Callback provided by the caller. If None, emission is a no-op.
        event : Dict[str, Any]
            Event payload to send.
        """
        if emit is None:
            return
        try:
            emit(event)
        except Exception:
            # Never let progress reporting break the actual migration.
            self.logger.debug("Progress emitter raised; ignoring.", exc_info=True)

    def _safe_status_code(self, resp: Any) -> int | None:
        """
        Safely extract an HTTP status code from a response-like object.
        """
        try:
            return int(resp.status_code)
        except Exception:
            return None

    def _truncate(self, text: str, limit: int = 500) -> str:
        if text is None:
            return ""
        return text if len(text) <= limit else (text[:limit] + "...")

    def _safe_json(self, resp: Any) -> tuple[dict[str, Any] | None, str | None]:
        """
        Returns (json_dict, error_reason).
        """
        if not resp:
            return None, "No response from server"
        try:
            return resp.json(), None
        except Exception:
            return None, f"Non-JSON response: {self._truncate(getattr(resp, 'text', '') or '')}"

    def _safe_error_payload(self, resp: Any, *, context: str) -> Any:
        """
        Best-effort extraction of an error payload.

        Parameters
        ----------
        resp : Any
            Response-like object (typically requests.Response) or None.
        context : str
            Short string to identify where this extraction was triggered (used in payload).

        Returns
        -------
        Any
            Parsed JSON payload if available, else response text, else a helpful dict.
            This function never returns None.
        """
        if resp is None:
            return {
                "message": "No response object returned by the HTTP client.",
                "context": context,
                "hint": "This usually means the HTTP client returned None on non-2xx or hit an exception. Check client logs.",
            }

        try:
            return resp.json()
        except Exception:
            self.logger.debug("Response is not JSON; falling back to raw text.", exc_info=True)

        try:
            txt = getattr(resp, "text", None)
            if txt:
                return txt
        except Exception:
            self.logger.debug("Failed to read response text; falling back to generic error payload.", exc_info=True)

        return {"message": "Failed to extract error payload from response.", "context": context}

    def _extract_error_detail(self, resp: Any) -> str:
        payload, err = self._safe_json(resp)
        if err:
            return err
        if isinstance(payload, dict):
            if isinstance(payload.get("detail"), str):
                return payload["detail"]
            if isinstance(payload.get("message"), str):
                return payload["message"]
            if isinstance(payload.get("error"), dict) and isinstance(payload["error"].get("message"), str):
                return payload["error"]["message"]
            if isinstance(payload.get("title"), str):
                return payload["title"]
        return self._truncate(getattr(resp, "text", "") or "") or "Unknown error"

    def _export_dashboard(self, oid: str) -> tuple[dict[str, Any] | None, str | None]:
        """
        Export dashboard from source. Tries adminAccess=true and falls back
        without it ONLY on auth failures (401/403) — a fallback can never help
        a server error or gateway timeout (the same request minus a query
        parameter fails identically), and blindly retrying doubles the cost of
        every slow failure. Returns (exported_json, error_reason).
        """
        # Primary: adminAccess=true
        resp = self.source_client.get(f"/api/dashboards/{oid}/export?adminAccess=true")
        if resp is not None and resp.status_code == 200:
            try:
                data = resp.json()
                if isinstance(data, dict):
                    return data, None
                return None, "Export returned non-dict JSON"
            except Exception:
                return None, f"Export returned invalid JSON: {self._truncate(resp.text or '')}"

        status = self._safe_status_code(resp)
        if status not in (401, 403):
            # Server error, timeout, or connection failure — fail fast with the
            # real reason; a no-adminAccess retry cannot succeed where this failed.
            if resp is None:
                return None, "Export failed: no response from the server (connection failure or timeout)"
            return None, f"Export failed (HTTP {status}): {self._extract_error_detail(resp)}"

        # Auth failure — maybe the token cannot use adminAccess; retry plain.
        resp2 = self.source_client.get(f"/api/dashboards/{oid}/export")
        if resp2 is not None and resp2.status_code == 200:
            try:
                data = resp2.json()
                if isinstance(data, dict):
                    return data, None
                return None, "Export returned non-dict JSON (fallback path)"
            except Exception:
                return None, f"Export returned invalid JSON (fallback path): {self._truncate(resp2.text or '')}"

        status2 = self._safe_status_code(resp2)
        detail = self._extract_error_detail(resp2) if resp2 is not None else "no response from the server"
        return None, f"Export failed (adminAccess=HTTP {status}, fallback=HTTP {status2}): {detail}"

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
