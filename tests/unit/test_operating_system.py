"""Unit tests for operating_system support across SisenseClient and modules."""

import pytest
from helpers import FakeApiClient, FakeLogger, FakeResponse  # noqa: F401

from pysisense.blox import Blox
from pysisense.sisenseclient import SisenseClient

# ---------------------------------------------------------------------------
# SisenseClient — operating_system parameter
# ---------------------------------------------------------------------------


class TestSisenseClientOperatingSystem:
    def test_defaults_to_linux(self):
        client = SisenseClient(domain="host", token="tok")
        assert client.operating_system == "linux"

    def test_accepts_windows(self):
        client = SisenseClient(domain="host", token="tok", operating_system="windows")
        assert client.operating_system == "windows"

    def test_accepts_linux_explicit(self):
        client = SisenseClient(domain="host", token="tok", operating_system="linux")
        assert client.operating_system == "linux"

    def test_normalizes_to_lowercase(self):
        client = SisenseClient(domain="host", token="tok", operating_system="Windows")
        assert client.operating_system == "windows"

    def test_normalizes_mixed_case(self):
        client = SisenseClient(domain="host", token="tok", operating_system="LINUX")
        assert client.operating_system == "linux"

    def test_rejects_invalid_value(self):
        with pytest.raises(ValueError, match="operating_system"):
            SisenseClient(domain="host", token="tok", operating_system="macos")

    def test_empty_string_falls_back_to_linux(self):
        client = SisenseClient(domain="host", token="tok", operating_system="")
        assert client.operating_system == "linux"

    def test_none_string_falls_back_to_linux(self):
        client = SisenseClient(domain="host", token="tok", operating_system="none")
        assert client.operating_system == "linux"

    def test_na_falls_back_to_linux(self):
        client = SisenseClient(domain="host", token="tok", operating_system="NA")
        assert client.operating_system == "linux"

    def test_null_string_falls_back_to_linux(self):
        client = SisenseClient(domain="host", token="tok", operating_system="null")
        assert client.operating_system == "linux"

    def test_yaml_config_sets_operating_system(self, tmp_path):
        config = tmp_path / "config.yaml"
        config.write_text("domain: myhost\nis_ssl: false\ntoken: secret\noperating_system: windows\n")
        client = SisenseClient(config_file=str(config))
        assert client.operating_system == "windows"

    def test_yaml_config_overrides_argument(self, tmp_path):
        config = tmp_path / "config.yaml"
        config.write_text("domain: myhost\nis_ssl: false\ntoken: secret\noperating_system: windows\n")
        # Argument says "linux" but YAML says "windows" — YAML wins
        client = SisenseClient(config_file=str(config), operating_system="linux")
        assert client.operating_system == "windows"

    def test_yaml_config_null_value_falls_back_to_linux(self, tmp_path):
        # YAML `operating_system:` with no value is parsed as None
        config = tmp_path / "config.yaml"
        config.write_text("domain: myhost\nis_ssl: false\ntoken: secret\noperating_system:\n")
        client = SisenseClient(config_file=str(config))
        assert client.operating_system == "linux"

    def test_yaml_config_none_string_falls_back_to_linux(self, tmp_path):
        config = tmp_path / "config.yaml"
        config.write_text("domain: myhost\nis_ssl: false\ntoken: secret\noperating_system: none\n")
        client = SisenseClient(config_file=str(config))
        assert client.operating_system == "linux"

    def test_yaml_config_na_falls_back_to_linux(self, tmp_path):
        config = tmp_path / "config.yaml"
        config.write_text("domain: myhost\nis_ssl: false\ntoken: secret\noperating_system: NA\n")
        client = SisenseClient(config_file=str(config))
        assert client.operating_system == "linux"

    def test_yaml_config_missing_key_uses_linux(self, tmp_path):
        config = tmp_path / "config.yaml"
        config.write_text("domain: myhost\nis_ssl: false\ntoken: secret\n")
        client = SisenseClient(config_file=str(config))
        assert client.operating_system == "linux"

    def test_yaml_config_invalid_value_raises(self, tmp_path):
        config = tmp_path / "config.yaml"
        config.write_text("domain: myhost\nis_ssl: false\ntoken: secret\noperating_system: solaris\n")
        with pytest.raises(ValueError, match="operating_system"):
            SisenseClient(config_file=str(config))

    def test_from_connection_defaults_to_linux(self):
        client = SisenseClient.from_connection(domain="host", token="tok")
        assert client.operating_system == "linux"

    def test_from_connection_accepts_windows(self):
        client = SisenseClient.from_connection(domain="host", token="tok", operating_system="windows")
        assert client.operating_system == "windows"


class TestDefaultPorts:
    def test_linux_non_ssl_defaults_to_30845(self):
        client = SisenseClient(domain="host", token="tok", is_ssl=False)
        assert client.base_url == "http://host:30845"

    def test_windows_non_ssl_defaults_to_8081(self):
        client = SisenseClient(domain="host", token="tok", is_ssl=False, operating_system="windows")
        assert client.base_url == "http://host:8081"

    def test_explicit_port_overrides_os_default(self):
        client = SisenseClient(domain="host", token="tok", is_ssl=False, port=9999, operating_system="windows")
        assert client.base_url == "http://host:9999"

    def test_yaml_windows_no_port_uses_8081(self, tmp_path):
        config = tmp_path / "config.yaml"
        config.write_text("domain: myhost\nis_ssl: false\ntoken: secret\noperating_system: windows\n")
        client = SisenseClient(config_file=str(config))
        assert client.base_url == "http://myhost:8081"


# ---------------------------------------------------------------------------
# Blox — OS-specific endpoint routing
# ---------------------------------------------------------------------------

_LINUX_ENDPOINT = "/api/v1/blox/getCustomActions"
_WINDOWS_ENDPOINT = "/api/v1/getCustomActions/actions"
_ACTIONS = [{"type": "OpenDashboard"}]


def _make_blox_with_os(os: str, endpoint_key: str):
    logger = FakeLogger()
    client = FakeApiClient(
        get_responses={endpoint_key: FakeResponse(200, _ACTIONS)},
        logger=logger,
        operating_system=os,
    )
    return Blox(api_client=client)


class TestBloxOperatingSystemRouting:
    def test_linux_uses_linux_endpoint(self):
        b = _make_blox_with_os("linux", _LINUX_ENDPOINT)
        result = b.get_blox_actions()
        assert isinstance(result, list)
        assert len(result) == 1

    def test_windows_uses_windows_endpoint(self):
        b = _make_blox_with_os("windows", _WINDOWS_ENDPOINT)
        result = b.get_blox_actions()
        assert isinstance(result, list)
        assert len(result) == 1

    def test_linux_does_not_hit_windows_endpoint(self):
        # Only the Windows endpoint is wired — Linux client should get None
        b = _make_blox_with_os("linux", _WINDOWS_ENDPOINT)
        result = b.get_blox_actions()
        assert "error" in result[0]

    def test_windows_does_not_hit_linux_endpoint(self):
        # Only the Linux endpoint is wired — Windows client should get None
        b = _make_blox_with_os("windows", _LINUX_ENDPOINT)
        result = b.get_blox_actions()
        assert "error" in result[0]
