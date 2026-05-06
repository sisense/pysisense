"""Unit tests for pysisense.plugins.Plugins."""

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.plugins import Plugins

# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------

_PLUGIN_ENABLED = {
    "name": "AdditionalInfoTooltip",
    "folderName": "plugin-AdditionalInfoTooltip",
    "isEnabled": True,
}

_PLUGIN_DISABLED = {
    "name": "CustomTodayFilter",
    "folderName": "plugin-CustomTodayFilter",
    "isEnabled": False,
}

_PLUGINS_PAGE = {
    "plugins": [_PLUGIN_ENABLED, _PLUGIN_DISABLED],
    "count": 2,
}

_PATCH_OK = FakeResponse(200, {})
_PATCH_FAIL = FakeResponse(500, {"error": "internal server error"})


def _make_plugins(get_responses=None, patch_responses=None):
    """Build a Plugins instance backed by FakeApiClient."""
    logger = FakeLogger()
    client = FakeApiClient(
        get_responses=get_responses,
        patch_responses=patch_responses,
        logger=logger,
    )
    return Plugins(api_client=client)


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------


class TestPluginsInit:
    def test_creates_with_fake_client(self):
        p = _make_plugins()
        assert p is not None
        assert hasattr(p, "api_client")
        assert hasattr(p, "logger")


# ---------------------------------------------------------------------------
# get_all_plugins
# ---------------------------------------------------------------------------


class TestGetAllPlugins:
    def test_returns_list_on_success(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.get_all_plugins()
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["name"] == "AdditionalInfoTooltip"

    def test_returns_error_on_none_response(self):
        p = _make_plugins()  # no responses → None
        result = p.get_all_plugins()
        assert isinstance(result, list)
        assert "error" in result[0]

    def test_returns_error_on_non_200(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(403, {"message": "forbidden"})})
        result = p.get_all_plugins()
        assert "error" in result[0]

    def test_returns_all_plugin_fields(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.get_all_plugins()
        plugin = result[0]
        assert "name" in plugin
        assert "folderName" in plugin
        assert "isEnabled" in plugin


# ---------------------------------------------------------------------------
# get_plugin
# ---------------------------------------------------------------------------


class TestGetPlugin:
    def test_found_by_api_name(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.get_plugin("AdditionalInfoTooltip")
        assert result["folderName"] == "plugin-AdditionalInfoTooltip"

    def test_found_by_folder_name(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.get_plugin("plugin-AdditionalInfoTooltip")
        assert result["name"] == "AdditionalInfoTooltip"

    def test_found_case_insensitive(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.get_plugin("additionalinfotooltip")
        assert result["folderName"] == "plugin-AdditionalInfoTooltip"

    def test_found_strips_plugin_prefix(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        # Searching by folder name with prefix should match api name without prefix
        result = p.get_plugin("plugin-CustomTodayFilter")
        assert result["name"] == "CustomTodayFilter"

    def test_not_found_returns_error(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.get_plugin("NoSuchPlugin")
        assert "error" in result

    def test_returns_error_if_get_all_plugins_fails(self):
        p = _make_plugins()  # no responses → None
        result = p.get_plugin("AdditionalInfoTooltip")
        assert "error" in result


# ---------------------------------------------------------------------------
# enable_plugin
# ---------------------------------------------------------------------------


class TestEnablePlugin:
    def test_enables_disabled_plugin(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_OK},
        )
        result = p.enable_plugin("CustomTodayFilter")
        assert result["isEnabled"] is True
        assert result["changed"] is True
        assert result["folderName"] == "plugin-CustomTodayFilter"

    def test_skips_already_enabled_plugin(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
        )
        result = p.enable_plugin("AdditionalInfoTooltip")
        assert result["isEnabled"] is True
        assert result["changed"] is False

    def test_returns_error_if_plugin_not_found(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.enable_plugin("NoSuchPlugin")
        assert "error" in result

    def test_returns_error_if_patch_fails(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_FAIL},
        )
        result = p.enable_plugin("CustomTodayFilter")
        assert "error" in result

    def test_accepts_folder_name_with_prefix(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_OK},
        )
        result = p.enable_plugin("plugin-CustomTodayFilter")
        assert result["changed"] is True


# ---------------------------------------------------------------------------
# disable_plugin
# ---------------------------------------------------------------------------


class TestDisablePlugin:
    def test_disables_enabled_plugin(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_OK},
        )
        result = p.disable_plugin("AdditionalInfoTooltip")
        assert result["isEnabled"] is False
        assert result["changed"] is True

    def test_skips_already_disabled_plugin(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.disable_plugin("CustomTodayFilter")
        assert result["isEnabled"] is False
        assert result["changed"] is False

    def test_returns_error_if_plugin_not_found(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.disable_plugin("NoSuchPlugin")
        assert "error" in result

    def test_returns_error_if_patch_fails(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_FAIL},
        )
        result = p.disable_plugin("AdditionalInfoTooltip")
        assert "error" in result


# ---------------------------------------------------------------------------
# enable_plugins (bulk)
# ---------------------------------------------------------------------------


class TestEnablePluginsBulk:
    def test_enables_disabled_plugin_in_bulk(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_OK},
        )
        result = p.enable_plugins(["CustomTodayFilter"])
        assert "plugin-CustomTodayFilter" in result["changed"]
        assert result["not_found"] == []
        assert result["errors"] == []

    def test_skips_already_enabled_plugin(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.enable_plugins(["AdditionalInfoTooltip"])
        assert result["changed"] == []
        assert "plugin-AdditionalInfoTooltip" in result["already_enabled"]

    def test_marks_not_found_names(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.enable_plugins(["NoSuchPlugin"])
        assert "NoSuchPlugin" in result["not_found"]
        assert result["changed"] == []

    def test_mixed_found_and_not_found(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_OK},
        )
        result = p.enable_plugins(["CustomTodayFilter", "NoSuchPlugin"])
        assert "plugin-CustomTodayFilter" in result["changed"]
        assert "NoSuchPlugin" in result["not_found"]

    def test_accepts_bare_string_input(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_OK},
        )
        result = p.enable_plugins("CustomTodayFilter")
        assert "plugin-CustomTodayFilter" in result["changed"]

    def test_returns_error_if_get_all_plugins_fails(self):
        p = _make_plugins()  # no responses → None
        result = p.enable_plugins(["CustomTodayFilter"])
        assert "error" in result

    def test_returns_error_if_bulk_patch_fails(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_FAIL},
        )
        result = p.enable_plugins(["CustomTodayFilter"])
        assert "error" in result


# ---------------------------------------------------------------------------
# enable_plugins (non-bulk)
# ---------------------------------------------------------------------------


class TestEnablePluginsNonBulk:
    def test_enables_plugins_one_by_one(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_OK},
        )
        result = p.enable_plugins(["CustomTodayFilter"], bulk=False)
        assert "plugin-CustomTodayFilter" in result["changed"]
        assert result["errors"] == []

    def test_tracks_patch_errors_per_plugin(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_FAIL},
        )
        result = p.enable_plugins(["CustomTodayFilter"], bulk=False)
        assert "plugin-CustomTodayFilter" in result["errors"]
        assert result["changed"] == []


# ---------------------------------------------------------------------------
# disable_plugins (bulk)
# ---------------------------------------------------------------------------


class TestDisablePluginsBulk:
    def test_disables_enabled_plugin_in_bulk(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_OK},
        )
        result = p.disable_plugins(["AdditionalInfoTooltip"])
        assert "plugin-AdditionalInfoTooltip" in result["changed"]
        assert result["errors"] == []

    def test_skips_already_disabled_plugin(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.disable_plugins(["CustomTodayFilter"])
        assert result["changed"] == []
        assert "plugin-CustomTodayFilter" in result["already_disabled"]

    def test_marks_not_found_names(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.disable_plugins(["NoSuchPlugin"])
        assert "NoSuchPlugin" in result["not_found"]

    def test_accepts_bare_string_input(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_OK},
        )
        result = p.disable_plugins("AdditionalInfoTooltip")
        assert "plugin-AdditionalInfoTooltip" in result["changed"]


# ---------------------------------------------------------------------------
# save_snapshot
# ---------------------------------------------------------------------------


class TestSaveSnapshot:
    def test_returns_created_and_plugins_keys(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.save_snapshot()
        assert "created" in result
        assert "plugins" in result

    def test_snapshot_only_includes_enabled_plugins(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.save_snapshot()
        assert "plugin-AdditionalInfoTooltip" in result["plugins"]
        assert "plugin-CustomTodayFilter" not in result["plugins"]

    def test_plugins_list_is_sorted(self):
        page = {
            "plugins": [
                {"name": "ZPlugin", "folderName": "plugin-ZPlugin", "isEnabled": True},
                {"name": "APlugin", "folderName": "plugin-APlugin", "isEnabled": True},
            ],
            "count": 2,
        }
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, page)})
        result = p.save_snapshot()
        assert result["plugins"] == sorted(result["plugins"])

    def test_returns_error_if_get_all_plugins_fails(self):
        p = _make_plugins()  # no responses → None
        result = p.save_snapshot()
        assert "error" in result

    def test_created_timestamp_is_utc_iso8601(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.save_snapshot()
        ts = result["created"]
        assert ts.endswith("Z")
        assert "T" in ts


# ---------------------------------------------------------------------------
# restore_snapshot
# ---------------------------------------------------------------------------


class TestRestoreSnapshot:
    def _snapshot_with(self, folder_names):
        return {"created": "2026-05-06T00:00:00Z", "plugins": folder_names}

    def test_enables_plugins_missing_from_instance(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_OK},
        )
        snapshot = self._snapshot_with(["plugin-AdditionalInfoTooltip", "plugin-CustomTodayFilter"])
        result = p.restore_snapshot(snapshot)
        # CustomTodayFilter was disabled — should now be enabled
        assert "plugin-CustomTodayFilter" in result["enabled"]
        assert result["errors"] == []

    def test_disables_plugins_not_in_snapshot(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_OK},
        )
        # Snapshot contains nothing — AdditionalInfoTooltip (enabled) should be disabled
        snapshot = self._snapshot_with([])
        result = p.restore_snapshot(snapshot)
        assert "plugin-AdditionalInfoTooltip" in result["disabled"]

    def test_already_set_count_when_no_changes_needed(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
        )
        # Snapshot exactly matches current state
        snapshot = self._snapshot_with(["plugin-AdditionalInfoTooltip"])
        result = p.restore_snapshot(snapshot)
        assert result["enabled"] == []
        assert result["disabled"] == []
        assert result["already_set"] >= 0

    def test_not_in_instance_for_unknown_folder(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_OK},
        )
        snapshot = self._snapshot_with(["plugin-AdditionalInfoTooltip", "plugin-DoesNotExist"])
        result = p.restore_snapshot(snapshot)
        assert "plugin-DoesNotExist" in result["not_in_instance"]

    def test_returns_error_if_plugins_key_missing(self):
        p = _make_plugins(get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)})
        result = p.restore_snapshot({"created": "2026-05-06T00:00:00Z"})
        assert "error" in result

    def test_returns_error_if_get_all_plugins_fails(self):
        p = _make_plugins()  # no responses → None
        result = p.restore_snapshot(self._snapshot_with(["plugin-AdditionalInfoTooltip"]))
        assert "error" in result

    def test_non_bulk_mode_tracks_errors(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_FAIL},
        )
        snapshot = self._snapshot_with(["plugin-AdditionalInfoTooltip", "plugin-CustomTodayFilter"])
        result = p.restore_snapshot(snapshot, bulk=False)
        # CustomTodayFilter was disabled, should be enabled — but PATCH fails
        assert "plugin-CustomTodayFilter" in result["errors"]

    def test_bulk_mode_returns_error_if_patch_fails(self):
        p = _make_plugins(
            get_responses={"/api/v1/plugins": FakeResponse(200, _PLUGINS_PAGE)},
            patch_responses={"/api/v1/plugins": _PATCH_FAIL},
        )
        snapshot = self._snapshot_with(["plugin-AdditionalInfoTooltip", "plugin-CustomTodayFilter"])
        result = p.restore_snapshot(snapshot)
        assert "error" in result
