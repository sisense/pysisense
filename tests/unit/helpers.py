"""Shared fake helpers for pysisense unit tests.

Intended usage in test files:
    from helpers import FakeLogger, FakeResponse, FakeApiClient
"""

from __future__ import annotations

from typing import Any


class FakeLogger:
    """Captures log calls without writing to disk."""

    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    def _log(self, level: str, msg: str) -> None:
        self.messages.append({"level": level, "msg": msg})

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log("debug", msg)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log("info", msg)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log("warning", msg)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log("error", msg)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log("exception", msg)


class FakeResponse:
    """Minimal stand-in for requests.Response."""

    def __init__(self, status_code: int, json_data: Any, text: str = "") -> None:
        self.status_code = status_code
        self._json_data = json_data
        self.text = text or str(json_data)
        self.content = b"content"

    @property
    def ok(self) -> bool:
        return self.status_code < 400

    def json(self) -> Any:
        return self._json_data


class FakeApiClient:
    """Fake SisenseClient that returns pre-configured responses keyed by URL.

    Separate response dicts per HTTP method.  The lookup strategy is:
    1. Exact URL match.
    2. Strip query-params (``?...``) and retry exact match.
    3. Longest prefix match (handles paths with dynamic segments).

    Parameters
    ----------
    get_responses / post_responses / put_responses / patch_responses / delete_responses
        ``{url_key: FakeResponse | None}`` mapping.  Use ``None`` as the
        value to simulate the client returning ``None`` (network failure).
    logger
        Optional FakeLogger; a fresh instance is created if omitted.
    """

    def __init__(
        self,
        get_responses: dict[str, FakeResponse | None] | None = None,
        post_responses: dict[str, FakeResponse | None] | None = None,
        put_responses: dict[str, FakeResponse | None] | None = None,
        patch_responses: dict[str, FakeResponse | None] | None = None,
        delete_responses: dict[str, FakeResponse | None] | None = None,
        logger: FakeLogger | None = None,
        operating_system: str = "linux",
    ) -> None:
        self._get = get_responses or {}
        self._post = post_responses or {}
        self._put = put_responses or {}
        self._patch = patch_responses or {}
        self._delete = delete_responses or {}
        self.logger = logger or FakeLogger()
        self.operating_system = operating_system

    def _lookup(self, store: dict, url: str) -> FakeResponse | None:
        # 1. Exact
        if url in store:
            return store[url]
        # 2. Strip query params
        base = url.split("?")[0]
        if base in store:
            return store[base]
        # 3. Longest prefix match (for URLs with dynamic path segments)
        best, best_len = None, 0
        for key, val in store.items():
            if url.startswith(key) and len(key) > best_len:
                best, best_len = val, len(key)
        return best

    def get(self, url: str, params: dict | None = None, **kwargs: Any) -> FakeResponse | None:
        return self._lookup(self._get, url)

    def post(self, url: str, data: Any = None, **kwargs: Any) -> FakeResponse | None:
        return self._lookup(self._post, url)

    def put(self, url: str, data: Any = None, **kwargs: Any) -> FakeResponse | None:
        return self._lookup(self._put, url)

    def patch(self, url: str, data: Any = None, **kwargs: Any) -> FakeResponse | None:
        return self._lookup(self._patch, url)

    def delete(self, url: str, **kwargs: Any) -> FakeResponse | None:
        return self._lookup(self._delete, url)
