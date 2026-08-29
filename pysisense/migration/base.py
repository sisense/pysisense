from __future__ import annotations

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
