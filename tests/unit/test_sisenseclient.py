"""Unit tests for pysisense.sisenseclient.SisenseClient."""

import pytest

from pysisense.sisenseclient import SisenseClient


class TestSisenseClientInit:
    def test_direct_connection_ssl_builds_https_url(self):
        client = SisenseClient(domain="myserver.com", token="mytoken", is_ssl=True)
        assert client.base_url == "https://myserver.com"
        assert client.token == "mytoken"

    def test_direct_connection_no_ssl_uses_port_30845(self):
        client = SisenseClient(domain="myserver.com", token="mytoken", is_ssl=False)
        assert client.base_url == "http://myserver.com:30845"

    def test_direct_connection_no_ssl_custom_port(self):
        client = SisenseClient(domain="myserver.com", token="mytoken", is_ssl=False, port=9090)
        assert client.base_url == "http://myserver.com:9090"

    def test_yaml_config_custom_port(self, tmp_path):
        config = tmp_path / "config.yaml"
        config.write_text("domain: myhost\nis_ssl: false\ntoken: secret\nport: 4000\n")
        client = SisenseClient(config_file=str(config))
        assert client.base_url == "http://myhost:4000"

    def test_yaml_config_no_ssl_default_port(self, tmp_path):
        config = tmp_path / "config.yaml"
        config.write_text("domain: myhost\nis_ssl: false\ntoken: secret\n")
        client = SisenseClient(config_file=str(config))
        assert client.base_url == "http://myhost:30845"

    def test_domain_with_protocol_prefix_is_stripped(self):
        client = SisenseClient(domain="https://myserver.com", token="tok", is_ssl=True)
        assert client.base_url == "https://myserver.com"

    def test_domain_with_port_strips_port(self):
        client = SisenseClient(domain="myserver.com:8080", token="tok", is_ssl=True)
        assert "myserver.com" in client.base_url
        assert "8080" not in client.base_url

    def test_missing_domain_raises_value_error(self):
        with pytest.raises(ValueError, match="domain"):
            SisenseClient(token="tok")

    def test_missing_token_raises_value_error(self):
        with pytest.raises(ValueError, match="token"):
            SisenseClient(domain="myserver.com")

    def test_no_config_file_and_no_inline_raises(self):
        with pytest.raises(ValueError):
            SisenseClient(config_file=None)

    def test_is_ssl_defaults_to_true_when_not_specified(self):
        client = SisenseClient(domain="myserver.com", token="tok")
        assert client.base_url.startswith("https://")


class TestSisenseClientFromConnection:
    def test_creates_ssl_client(self):
        client = SisenseClient.from_connection(domain="example.com", token="tok", is_ssl=True)
        assert client.base_url == "https://example.com"
        assert client.token == "tok"

    def test_creates_non_ssl_client(self):
        client = SisenseClient.from_connection(domain="example.com", token="tok", is_ssl=False)
        assert "30845" in client.base_url

    def test_auth_header_contains_bearer_token(self):
        client = SisenseClient.from_connection(domain="example.com", token="secret123")
        assert client.headers["Authorization"] == "Bearer secret123"


class TestSisenseClientToDataframe:
    def test_delegates_to_convert_to_dataframe(self):
        client = SisenseClient.from_connection(domain="x.com", token="tok")
        df = client.to_dataframe([{"a": 1, "b": 2}])
        assert df is not None
        assert "a" in df.columns

    def test_invalid_data_returns_none(self):
        client = SisenseClient.from_connection(domain="x.com", token="tok")
        assert client.to_dataframe(12345) is None


class TestSisenseClientExportToCsv:
    def test_creates_csv_file(self, tmp_path):
        client = SisenseClient.from_connection(domain="x.com", token="tok")
        output = str(tmp_path / "result.csv")
        client.export_to_csv([{"col": "val"}], file_name=output)
        import os

        assert os.path.exists(output)
