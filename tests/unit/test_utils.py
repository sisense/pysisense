"""Unit tests for pysisense.utils."""

import os

from pysisense.utils import convert_to_dataframe, convert_utc_to_local, export_to_csv


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
