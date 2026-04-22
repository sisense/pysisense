"""Unit tests for pysisense.migration.Migration."""

import pytest
from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.migration import Migration

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fake_client(get_responses=None, post_responses=None):
    """Return a FakeApiClient with a FakeLogger, usable as a source/target client."""
    logger = FakeLogger()
    return FakeApiClient(
        get_responses=get_responses,
        post_responses=post_responses,
        logger=logger,
    )


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------


class TestMigrationInit:
    def test_client_based_init_succeeds(self):
        src = _make_fake_client()
        tgt = _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        assert m.source_client is src
        assert m.target_client is tgt

    def test_missing_both_yaml_and_clients_raises(self):
        with pytest.raises(ValueError):
            Migration()

    def test_partial_client_init_raises(self):
        src = _make_fake_client()
        with pytest.raises(ValueError):
            Migration(source_client=src)  # missing target_client

    def test_logger_comes_from_source_client(self):
        logger = FakeLogger()
        src = FakeApiClient(logger=logger)
        tgt = _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        assert m.logger is logger


# ---------------------------------------------------------------------------
# _emit helper
# ---------------------------------------------------------------------------


class TestEmitHelper:
    def test_emit_with_none_callback_is_noop(self):
        src, tgt = _make_fake_client(), _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        # Should not raise
        m._emit(None, {"type": "progress", "count": 1})

    def test_emit_calls_the_callback(self):
        events = []
        src, tgt = _make_fake_client(), _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        m._emit(events.append, {"type": "done"})
        assert events == [{"type": "done"}]

    def test_emit_swallows_callback_exceptions(self):
        def bad_cb(_):
            raise RuntimeError("explode")

        src, tgt = _make_fake_client(), _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        # Should not propagate
        m._emit(bad_cb, {"type": "event"})


# ---------------------------------------------------------------------------
# _safe_status_code helper
# ---------------------------------------------------------------------------


class TestSafeStatusCode:
    def test_extracts_status_code_from_response(self):
        src, tgt = _make_fake_client(), _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        resp = FakeResponse(200, {})
        assert m._safe_status_code(resp) == 200

    def test_returns_none_when_no_response(self):
        src, tgt = _make_fake_client(), _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        assert m._safe_status_code(None) is None


# ---------------------------------------------------------------------------
# _truncate helper
# ---------------------------------------------------------------------------


class TestTruncateHelper:
    def test_short_text_is_unchanged(self):
        src, tgt = _make_fake_client(), _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        assert m._truncate("hello") == "hello"

    def test_long_text_is_truncated(self):
        src, tgt = _make_fake_client(), _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        result = m._truncate("x" * 600, limit=500)
        assert len(result) <= 503  # 500 + "..."
        assert result.endswith("...")

    def test_none_input_returns_empty_string(self):
        src, tgt = _make_fake_client(), _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        assert m._truncate(None) == ""


# ---------------------------------------------------------------------------
# _safe_json helper
# ---------------------------------------------------------------------------


class TestSafeJson:
    def test_returns_json_dict_on_success(self):
        src, tgt = _make_fake_client(), _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        resp = FakeResponse(200, {"key": "value"})
        data, err = m._safe_json(resp)
        assert data == {"key": "value"}
        assert err is None

    def test_returns_error_reason_when_no_response(self):
        src, tgt = _make_fake_client(), _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        data, err = m._safe_json(None)
        assert data is None
        assert err is not None


# ---------------------------------------------------------------------------
# _safe_error_payload helper
# ---------------------------------------------------------------------------


class TestSafeErrorPayload:
    def test_returns_dict_when_response_is_none(self):
        src, tgt = _make_fake_client(), _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        result = m._safe_error_payload(None, context="test")
        assert isinstance(result, dict)
        assert "message" in result

    def test_returns_json_when_response_has_json(self):
        src, tgt = _make_fake_client(), _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        resp = FakeResponse(400, {"error": "bad request"})
        result = m._safe_error_payload(resp, context="test")
        assert result == {"error": "bad request"}


# ---------------------------------------------------------------------------
# _extract_error_detail helper
# ---------------------------------------------------------------------------


class TestExtractErrorDetail:
    def test_extracts_detail_field(self):
        src, tgt = _make_fake_client(), _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        resp = FakeResponse(400, {"detail": "Something went wrong"})
        result = m._extract_error_detail(resp)
        assert result == "Something went wrong"

    def test_extracts_message_field_as_fallback(self):
        src, tgt = _make_fake_client(), _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        resp = FakeResponse(400, {"message": "Bad input"})
        result = m._extract_error_detail(resp)
        assert result == "Bad input"

    def test_returns_error_string_when_no_response(self):
        src, tgt = _make_fake_client(), _make_fake_client()
        m = Migration(source_client=src, target_client=tgt)
        result = m._extract_error_detail(None)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Public method existence
# ---------------------------------------------------------------------------


class TestMigrationPublicMethodsExist:
    """Verify that all public migration methods are present on the class.

    These tests will catch regressions if a refactor accidentally removes
    or renames a method.
    """

    def _migration(self):
        src, tgt = _make_fake_client(), _make_fake_client()
        return Migration(source_client=src, target_client=tgt)

    def test_migrate_groups_exists(self):
        assert callable(getattr(self._migration(), "migrate_groups", None))

    def test_migrate_all_groups_exists(self):
        assert callable(getattr(self._migration(), "migrate_all_groups", None))

    def test_migrate_users_exists(self):
        assert callable(getattr(self._migration(), "migrate_users", None))

    def test_migrate_all_users_exists(self):
        assert callable(getattr(self._migration(), "migrate_all_users", None))

    def test_migrate_dashboard_shares_exists(self):
        assert callable(getattr(self._migration(), "migrate_dashboard_shares", None))

    def test_migrate_dashboards_exists(self):
        assert callable(getattr(self._migration(), "migrate_dashboards", None))

    def test_migrate_all_dashboards_exists(self):
        assert callable(getattr(self._migration(), "migrate_all_dashboards", None))

    def test_migrate_datamodels_exists(self):
        assert callable(getattr(self._migration(), "migrate_datamodels", None))

    def test_migrate_all_datamodels_exists(self):
        assert callable(getattr(self._migration(), "migrate_all_datamodels", None))
