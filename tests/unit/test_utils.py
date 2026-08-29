"""Unit tests for pysisense.utils."""

import os

from pysisense.utils import convert_to_dataframe, convert_utc_to_local, export_to_csv, redact_secrets


class TestRedactSecrets:
    def test_redacts_password_key(self):
        result = redact_secrets({"userName": "bob", "password": "hunter2"})
        assert result == {"userName": "bob", "password": "***REDACTED***"}

    def test_redacts_case_insensitively(self):
        result = redact_secrets({"Password": "hunter2", "TOKEN": "abc"})
        assert result == {"Password": "***REDACTED***", "TOKEN": "***REDACTED***"}

    def test_redacts_nested_dicts_and_lists(self):
        data = {"parameters": {"password": "s3cret"}, "items": [{"secret": "x"}, {"name": "ok"}]}
        result = redact_secrets(data)
        assert result == {"parameters": {"password": "***REDACTED***"}, "items": [{"secret": "***REDACTED***"}, {"name": "ok"}]}

    def test_leaves_non_sensitive_keys_untouched(self):
        data = {"name": "connection1", "region": "us-east-1"}
        assert redact_secrets(data) == data

    def test_does_not_mutate_input(self):
        data = {"password": "hunter2"}
        redact_secrets(data)
        assert data == {"password": "hunter2"}

    def test_passes_through_non_dict_non_list_values(self):
        assert redact_secrets("plain-string") == "plain-string"
        assert redact_secrets(None) is None
        assert redact_secrets(42) == 42


class TestConvertToDataframe:
    def test_list_of_dicts_returns_dataframe(self):
        data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        df = convert_to_dataframe(data)
        assert df is not None
        assert list(df.columns) == ["a", "b"]
        assert len(df) == 2

    def test_single_dict_returns_dataframe(self):
        data = {"x": 10, "y": 20}
        df = convert_to_dataframe(data)
        assert df is not None
        assert "x" in df.columns and "y" in df.columns

    def test_plain_list_uses_column_a(self):
        data = ["alpha", "beta", "gamma"]
        df = convert_to_dataframe(data)
        assert df is not None
        assert "Column_A" in df.columns
        assert len(df) == 3

    def test_empty_list_returns_empty_dataframe(self):
        df = convert_to_dataframe([])
        assert df is not None
        assert len(df) == 0

    def test_nested_dict_in_list_flattens(self):
        data = [{"user": {"id": 1, "name": "Alice"}}]
        df = convert_to_dataframe(data)
        assert df is not None
        assert "user.id" in df.columns or "user" in df.columns

    def test_invalid_input_returns_none(self):
        df = convert_to_dataframe(42)
        assert df is None

    def test_mixed_list_returns_none(self):
        df = convert_to_dataframe([{"a": 1}, "not_a_dict"])
        assert df is None


class TestExportToCsv:
    def test_creates_csv_file(self, tmp_path):
        data = [{"col1": "v1", "col2": "v2"}, {"col1": "v3", "col2": "v4"}]
        output = str(tmp_path / "out.csv")
        export_to_csv(data, file_name=output)
        assert os.path.exists(output)

    def test_invalid_data_does_not_raise(self):
        # Should swallow the error gracefully
        export_to_csv(99999)


class TestConvertUtcToLocal:
    def test_valid_utc_string(self):
        result = convert_utc_to_local("2025-05-14T16:24:33.537Z")
        assert result is not None
        assert "2025-05-14" in result

    def test_empty_string_returns_none(self):
        assert convert_utc_to_local("") is None

    def test_none_returns_none(self):
        assert convert_utc_to_local(None) is None

    def test_invalid_string_returns_error_message(self):
        result = convert_utc_to_local("not-a-real-date")
        assert result is not None
        assert "Invalid timestamp" in result
