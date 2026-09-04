"""Unit tests for pysisense.utils."""

import os

import pytest

from pysisense.utils import convert_to_dataframe, convert_utc_to_local, export_to_csv, load_config, redact_secrets


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


class TestLoadConfig:
    def test_mapping_is_copied_not_shared(self):
        source = {"domain": "h", "token": "t"}
        result = load_config(source)
        assert result == source
        result["is_ssl"] = False
        assert "is_ssl" not in source

    def test_yaml_file(self, tmp_path):
        path = tmp_path / "config.yaml"
        path.write_text("domain: myhost\ntoken: secret\nis_ssl: false\n")
        assert load_config(str(path)) == {"domain": "myhost", "token": "secret", "is_ssl": False}

    def test_yml_extension_is_yaml(self, tmp_path):
        path = tmp_path / "config.yml"
        path.write_text("domain: myhost\ntoken: secret\n")
        assert load_config(str(path)) == {"domain": "myhost", "token": "secret"}

    def test_json_file(self, tmp_path):
        path = tmp_path / "config.json"
        path.write_text('{"domain": "myhost", "token": "secret", "port": 4000}')
        assert load_config(str(path)) == {"domain": "myhost", "token": "secret", "port": 4000}

    def test_pathlike_is_accepted(self, tmp_path):
        path = tmp_path / "config.json"
        path.write_text('{"domain": "myhost", "token": "secret"}')
        assert load_config(path) == {"domain": "myhost", "token": "secret"}

    def test_unknown_extension_falls_back_to_yaml_which_also_reads_json(self, tmp_path):
        path = tmp_path / "config.conf"
        path.write_text('{"domain": "myhost", "token": "secret"}')
        assert load_config(str(path)) == {"domain": "myhost", "token": "secret"}

    def test_empty_file_raises_value_error(self, tmp_path):
        path = tmp_path / "config.yaml"
        path.write_text("")
        with pytest.raises(ValueError, match="top-level mapping"):
            load_config(str(path))

    def test_non_mapping_document_raises_value_error(self, tmp_path):
        path = tmp_path / "config.json"
        path.write_text('["domain", "token"]')
        with pytest.raises(ValueError, match="top-level mapping"):
            load_config(str(path))

    def test_unsupported_source_type_raises_type_error(self):
        with pytest.raises(TypeError, match="file path or a mapping"):
            load_config(42)


# ---------------------------------------------------------------------------
# JAQL dim parsing — _parse_dim_candidates / _parse_dim / _column_name_variants
# ---------------------------------------------------------------------------

from pysisense.utils import (  # noqa: E402
    _column_name_variants,
    _datasource_title,
    _extract_dashboard_columns,
    _extract_dashboard_references,
    _parse_dim,
    _parse_dim_candidates,
    _reference_from_jaql,
    _split_dim,
)


class TestParseDimCandidates:
    @pytest.mark.parametrize(
        ("dim", "expected"),
        [
            # the two ordinary forms
            ("[Orders.Amount]", [("Orders", "Amount")]),
            ("[Orders].[Amount]", [("Orders", "Amount")]),  # the old parser mangled this
            ("[Orders].[Date (Calendar)]", [("Orders", "Date (Calendar)")]),
            ("[Orders.Date (Calendar)]", [("Orders", "Date (Calendar)")]),
            # dotted table name: every split is a candidate, schema decides
            ("[dbo.Orders.Amount]", [("dbo", "Orders.Amount"), ("dbo.Orders", "Amount")]),
            # leading-dot names (real: a column called .tpep_pickup_datetime)
            ("[trips..tpep_pickup_datetime]", [("trips", ".tpep_pickup_datetime"), ("trips.", "tpep_pickup_datetime")]),
            ("[trips].[.tpep_pickup_datetime]", [("trips", ".tpep_pickup_datetime")]),
            ("[.trips.col]", [(".trips", "col")]),
            # quotes inside names are just characters
            ('[trips].[."tpep_pickup_datetime]', [("trips", '."tpep_pickup_datetime')]),
            ("[Table 'q'.Col \"x\"]", [("Table 'q'", 'Col "x"')]),
            # whitespace around the whole dim is tolerated
            ("  [Orders].[Amount]  ", [("Orders", "Amount")]),
        ],
    )
    def test_ordinary_and_odd_names(self, dim, expected):
        assert _parse_dim_candidates(dim) == expected

    # Sisense places NO restriction on table or column names. The parser must be
    # name-agnostic, so this proves the property over a character set rather
    # than over a handful of examples. Each hostile character is tried leading,
    # trailing and in the middle, for tables and for columns.
    _HOSTILE = list(".[]'\"@#$%&()-+/\\ ;:,!?*")

    @pytest.mark.parametrize("ch", _HOSTILE)
    @pytest.mark.parametrize("position", ["leading", "trailing", "middle"])
    def test_any_character_in_a_column_name_round_trips(self, ch, position):
        column = {"leading": f"{ch}col", "trailing": f"col{ch}", "middle": f"co{ch}l"}[position]
        table = "tbl"
        assert (table, column) in _parse_dim_candidates(f"[{table}.{column}]")
        assert (table, column) in _parse_dim_candidates(f"[{table}].[{column}]")

    @pytest.mark.parametrize("ch", _HOSTILE)
    @pytest.mark.parametrize("position", ["leading", "trailing", "middle"])
    def test_any_character_in_a_table_name_round_trips(self, ch, position):
        table = {"leading": f"{ch}tbl", "trailing": f"tbl{ch}", "middle": f"t{ch}bl"}[position]
        column = "col"
        assert (table, column) in _parse_dim_candidates(f"[{table}.{column}]")
        assert (table, column) in _parse_dim_candidates(f"[{table}].[{column}]")

    @pytest.mark.parametrize(
        ("table", "column"),
        [
            # Concrete examples from a real sandbox model built to be hostile
            # (live-verified 18/18 on 2026-09-02) — illustrations of the
            # property above, not the extent of what is supported.
            ("@trips", '."tpep_pickup_datetime.'),  # leading dot, quote, trailing dot
            ("@trips", "[trip_distance"),  # leading bracket
        ],
    )
    def test_real_world_examples(self, table, column):
        assert (table, column) in _parse_dim_candidates(f"[{table}.{column}]")
        assert (table, column) in _parse_dim_candidates(f"[{table}].[{column}]")

    def test_the_one_inherent_ambiguity_is_sisense_format_not_the_parser(self):
        # A table ending in "]" next to a column starting with "[" makes the
        # ONE-bracket form spell "[T].[C]" — byte-identical to the TWO-bracket
        # form for the plainer pair. No parser can tell them apart, and neither
        # can Sisense; the two-bracket reading wins. Documented, not "fixed".
        assert _parse_dim_candidates("[T].[C]") == [("T", "C")]

    def test_brackets_inside_names_are_allowed_not_refused(self):
        # A table literally named "Sales [EU]". The strict grammar refused
        # these; names can contain brackets, so the parser must offer them.
        assert _parse_dim_candidates("[Sales [EU].Amount]") == [("Sales [EU]", "Amount")]
        cands = _parse_dim_candidates("[Sales [EU]].[Amount]")
        assert cands[0] == ("Sales [EU]", "Amount")  # the two-bracket reading, first

    def test_two_bracket_form_does_not_resplit_dots_inside_names(self):
        # Regression: the leading dot of ".tpep_pickup_datetime" was also being
        # offered as a split, yielding a bogus ("trips].[", "tpep...") candidate
        # and turning a single reading into an ambiguous one.
        assert _parse_dim_candidates("[trips].[.tpep_pickup_datetime]") == [("trips", ".tpep_pickup_datetime")]
        assert _parse_dim_candidates("[a.b].[c.d]") == [("a.b", "c.d")]

    def test_three_bracket_groups_offer_every_separator(self):
        # Pathological but not refused — the schema will reject the wrong one.
        assert _parse_dim_candidates("[T].[C].[D]") == [("T", "C].[D"), ("T].[C", "D")]

    @pytest.mark.parametrize(
        "dim",
        [
            "Orders.Amount",  # no brackets at all
            "[Orders]",  # no separator -> no column
            "[Sales [EU]]",  # no separator
            "[]",
            "[.]",  # separator with empty sides
            "",
            "   ",
            None,
            123,
            ["[Orders.Amount]"],
        ],
    )
    def test_not_a_reference_yields_empty(self, dim):
        assert _parse_dim_candidates(dim) == []

    def test_outer_brackets_stripped_exactly_once(self):
        # The defining fix: names never lose or gain a bracket at the edges.
        for dim, expected_first in [
            ("[Orders].[Amount]", ("Orders", "Amount")),
            ("[Orders.Amount]", ("Orders", "Amount")),
        ]:
            assert _parse_dim_candidates(dim)[0] == expected_first


class TestParseDim:
    def test_single_reading(self):
        assert _parse_dim("[Orders].[Amount]") == ("Orders", "Amount")
        assert _parse_dim("[Orders.Amount]") == ("Orders", "Amount")
        assert _parse_dim("[trips].[.tpep_pickup_datetime]") == ("trips", ".tpep_pickup_datetime")

    def test_ambiguous_returns_none_not_a_guess(self):
        assert _parse_dim("[dbo.Orders.Amount]") is None
        assert _parse_dim("[trips..tpep_pickup_datetime]") is None

    def test_not_a_reference_returns_none(self):
        assert _parse_dim("Orders.Amount") is None
        assert _parse_dim("[Orders]") is None
        assert _parse_dim(None) is None


class TestColumnNameVariants:
    def test_calendar_suffix_yields_raw_then_stripped(self):
        assert _column_name_variants("Date (Calendar)") == ["Date (Calendar)", "Date"]

    def test_plain_column_yields_itself(self):
        assert _column_name_variants("Amount") == ["Amount"]

    def test_suffix_only_is_not_stripped_to_empty(self):
        assert _column_name_variants(" (Calendar)") == [" (Calendar)"]


def test_old_split_logic_mangled_the_two_bracket_form():
    # Permanent record of the defect the shared parser replaces, so nobody
    # reintroduces strip("[]").split(".", 1).
    dim = "[Orders].[Amount]"
    assert tuple(dim.strip("[]").split(".", 1)) == ("Orders]", "[Amount")  # the bug
    assert _parse_dim(dim) == ("Orders", "Amount")  # the fix


# Dims exactly as Sisense's own field list (GET /api/datasources/{name}/fields)
# emitted them for a live model whose tables/columns were renamed to hostile
# identity names. Live-captured 2026-09-03; every row is a real Sisense output.
#
# Three facts these rows establish:
#   1. Sisense emits the one-bracket dotted form ``[table.column]`` with the
#      names raw -- no escaping, even when a name itself contains brackets.
#   2. The dim carries the IDENTITY name, never the display name:
#      ``r_regionkey`` had display name ``test_1_r_regionkey`` at capture time.
#   3. A table whose identity name starts with ``[`` is byte-identical to a
#      doubled opening bracket, and a column ending in ``]`` to a doubled
#      closing one -- so any parser that strips bracket *runs* loses them.
_REAL_EMITTED_DIMS = [
    ('[@trips.."tpep_pickup_datetime. (Calendar)]', "@trips", '."tpep_pickup_datetime.'),
    ("[@trips.[trip_distance]", "@trips", "[trip_distance"),
    ("[@trips.dropoff_zip]", "@trips", "dropoff_zip"),
    ("[@trips.tpep_dropoff_datetime (Calendar)]", "@trips", "tpep_dropoff_datetime"),
    ("[[region.r_comment]", "[region", "r_comment"),
    ("[[region.r_regionkey]", "[region", "r_regionkey"),
]


@pytest.mark.parametrize(("dim", "table", "column"), _REAL_EMITTED_DIMS)
def test_dims_as_sisense_actually_emits_them_are_recoverable(dim, table, column):
    """Every live-emitted dim yields its true (table, column) among the candidates."""
    found = [(t, c) for t, c in _parse_dim_candidates(dim) if t == table and column in _column_name_variants(c)]
    assert found, f"{dim!r} -> {_parse_dim_candidates(dim)!r} never offers ({table!r}, {column!r})"


def test_old_strip_logic_ate_a_leading_bracket_from_a_real_table_name():
    """``strip("[]")`` removes bracket *runs*, so a table named ``[region`` came
    back as ``region`` -- a name that does not exist in the schema. Every column
    of that table would have been reported unused. Pinned against the emitted dim."""
    assert "[[region.r_comment]".strip("[]").split(".", 1) == ["region", "r_comment"]
    assert _parse_dim_candidates("[[region.r_comment]") == [("[region", "r_comment")]


# ---------------------------------------------------------------------------
# _split_dim / _extract_dashboard_columns — the shared walk behind
# Dashboard.get_dashboard_columns and AccessManagement.get_unused_columns_bulk
# ---------------------------------------------------------------------------


class TestSplitDim:
    def test_ordinary_one_bracket_form(self):
        assert _split_dim("[Orders.Amount]") == ("Orders", "Amount")

    def test_two_bracket_form_no_longer_mangled(self):
        assert _split_dim("[Orders].[Amount]") == ("Orders", "Amount")

    def test_table_name_starting_with_bracket_survives(self):
        assert _split_dim("[[region.r_comment]") == ("[region", "r_comment")

    def test_dotted_names_resolve_against_known_columns(self):
        dim = '[@trips.."tpep_pickup_datetime. (Calendar)]'
        known = {("@trips", '."tpep_pickup_datetime.'), ("@trips", "fare_amount")}
        assert _split_dim(dim, known) == ("@trips", '."tpep_pickup_datetime. (Calendar)')

    def test_dotted_names_without_schema_fall_back_to_first_dot(self):
        # Same reading the old first-dot split produced -- unchanged behaviour
        # for callers that have no schema to consult.
        assert _split_dim("[a.b.c]") == ("a", "b.c")

    def test_known_columns_that_match_nothing_still_fall_back_to_first_dot(self):
        assert _split_dim("[a.b.c]", {("x", "y")}) == ("a", "b.c")

    def test_non_reference_strings_keep_legacy_behaviour(self):
        assert _split_dim("Unknown.Table") == ("Unknown", "Table")
        assert _split_dim("[justatable]") == ("justatable", "Unknown Column")
        assert _split_dim("plain") == ("plain", "Unknown Column")


# Shaped exactly like a live export (2026-09-03): dashboard filter with a
# plain jaql, a dependent filter with levels (date, Calendar suffix), a pivot
# row item, an indicator with a widget-level filter panel, a formula whose
# columns live in `context`, an empty context, and an item with no dim.
_EXPORT = {
    "title": "fes_assistant",
    "filters": [
        {"jaql": {"dim": "[region.r_name]", "title": "r_name"}},
        {"levels": [{"dim": '[@trips.."tpep_pickup_datetime. (Calendar)]'}, {"dim": "[@trips.tpep_dropoff_datetime (Calendar)]"}]},
        "not-a-dict",
    ],
    "widgets": [
        {"oid": "w-pivot", "type": "pivot2", "metadata": {"panels": [{"name": "rows", "items": [{"jaql": {"dim": "[region.r_name]"}}]}, {"name": "values", "items": []}]}},
        {
            "oid": "w-ind",
            "type": "indicator",
            "metadata": {
                "panels": [
                    {"name": "value", "items": [{"jaql": {"dim": "[region.r_regionkey]", "agg": "sum"}}]},
                    {"name": "filters", "items": [{"jaql": {"dim": "[@trips.fare_amount]", "filter": {"members": ["-4"]}}}]},
                ]
            },
        },
        {
            "oid": "w-formula",
            "metadata": {
                "panels": [
                    {
                        "items": [
                            {"jaql": {"formula": "SUM([A]) / SUM([B])", "context": {"[A]": {"dim": "[@trips.fare_amount]"}, "[B]": {"dim": "[@trips.[trip_distance]"}, "junk": None}}},
                            {"jaql": {"formula": "1", "context": {}}},
                            {"jaql": {"title": "no dim here"}},
                            {"jaql": {"dim": None}},
                        ]
                    }
                ]
            },
        },
        {"oid": "w-empty", "metadata": {}},
        None,
    ],
    "layout": {"columns": []},
}


class TestExtractDashboardColumns:
    def test_rows_in_document_order_with_source_and_widget_id(self):
        rows = _extract_dashboard_columns(_EXPORT)
        assert [(r["source"], r["widget_id"], r["table"], r["column"]) for r in rows] == [
            ("filter", "N/A", "region", "r_name"),
            ("filter", "N/A", "@trips", '."tpep_pickup_datetime. (Calendar)'),
            ("filter", "N/A", "@trips", "tpep_dropoff_datetime (Calendar)"),
            ("widget", "w-pivot", "region", "r_name"),
            ("widget", "w-ind", "region", "r_regionkey"),
            ("widget", "w-ind", "@trips", "fare_amount"),
            ("widget", "w-formula", "@trips", "fare_amount"),
            ("widget", "w-formula", "@trips", "[trip_distance"),
        ]
        assert all(r["dashboard_name"] == "fes_assistant" for r in rows)
        # An item with no dim used to yield a fabricated "Unknown"/"Table" row; it no longer does.
        assert ("Unknown", "Table") not in {(r["table"], r["column"]) for r in rows}

    def test_widget_id_is_the_widgets_own_oid_not_a_layout_position(self):
        rows = _extract_dashboard_columns(_EXPORT)
        assert {r["widget_id"] for r in rows if r["source"] == "widget"} == {"w-pivot", "w-ind", "w-formula"}

    def test_dashboard_name_override(self):
        assert _extract_dashboard_columns(_EXPORT, "override")[0]["dashboard_name"] == "override"

    def test_explicit_none_dim_is_skipped_not_crashed(self):
        # The old walkers did `"." in dim_value` on filter dims and crashed on None.
        rows = _extract_dashboard_columns({"filters": [{"jaql": {"dim": None}}, {"levels": [{"dim": None}]}]})
        assert rows == []

    def test_calendar_suffix_left_on_column_for_callers(self):
        rows = _extract_dashboard_columns({"filters": [{"levels": [{"dim": "[T.Date (Calendar)]"}]}]})
        assert rows[0]["column"] == "Date (Calendar)"

    def test_known_columns_pick_the_right_reading_for_dotted_names(self):
        # With a schema the row carries the model's own spelling (no Calendar suffix).
        rows = _extract_dashboard_columns(_EXPORT, known_columns={("@trips", '."tpep_pickup_datetime.')})
        assert ("@trips", '."tpep_pickup_datetime.') in {(r["table"], r["column"]) for r in rows}

    def test_two_bracket_form_resolves(self):
        rows = _extract_dashboard_columns({"widgets": [{"oid": "w", "metadata": {"panels": [{"items": [{"jaql": {"dim": "[Orders].[Amount]"}}]}]}}]})
        assert (rows[0]["table"], rows[0]["column"]) == ("Orders", "Amount")

    def test_not_a_dict_yields_empty(self):
        assert _extract_dashboard_columns(None) == []
        assert _extract_dashboard_columns([]) == []


class TestReferenceFromJaql:
    """Explicit table/column keys beat dim parsing; the schema beats both on spelling.

    Census of 509 live dashboards (2026-09-03): 12,306 of 13,080 dim-bearing
    nodes carry table+column; 82 of them, in 20 dashboards, name a table with
    a dot in it (every CSV upload is called ``something.csv``) and were
    misread by the first-dot split.
    """

    def test_explicit_keys_recover_a_dotted_table_name_without_a_schema(self):
        node = {"dim": "[T1.csv.C1]", "table": "T1.csv", "column": "C1"}
        assert _reference_from_jaql(node) == ("T1.csv", "C1")

    def test_explicit_column_has_no_calendar_suffix(self):
        node = {"dim": "[Orders.Date (Calendar)]", "table": "Orders", "column": "Date"}
        assert _reference_from_jaql(node) == ("Orders", "Date")

    def test_falls_back_to_dim_parsing_when_keys_are_absent(self):
        assert _reference_from_jaql({"dim": "[Orders.Amount]"}) == ("Orders", "Amount")
        assert _reference_from_jaql({"dim": "[Orders].[Amount]"}) == ("Orders", "Amount")

    def test_schema_spelling_wins_on_a_case_mismatch(self):
        # Live-observed: dim "[Category.Category]" with table "category".
        node = {"dim": "[Category.Category]", "table": "category", "column": "Category"}
        assert _reference_from_jaql(node, {("Category", "Category")}) == ("Category", "Category")
        assert _reference_from_jaql(node) == ("category", "Category")  # no schema: as written

    def test_schema_resolves_dotted_dim_when_keys_are_absent(self):
        node = {"dim": "[T1.csv.C1]"}
        assert _reference_from_jaql(node, {("T1.csv", "C1")}) == ("T1.csv", "C1")

    def test_missing_dim_key_keeps_the_legacy_placeholder(self):
        assert _reference_from_jaql({"title": "x"}) == ("Unknown", "Table")

    def test_explicit_none_or_empty_dim_is_unusable(self):
        assert _reference_from_jaql({"dim": None}) is None
        assert _reference_from_jaql({"dim": ""}) is None

    def test_partial_explicit_keys_do_not_count(self):
        assert _reference_from_jaql({"dim": "[A.B]", "table": "A"}) == ("A", "B")
        assert _reference_from_jaql({"dim": "[A.B]", "table": "", "column": "B"}) == ("A", "B")


def test_walk_uses_explicit_keys_so_csv_tables_come_out_right():
    export = {"widgets": [{"oid": "w", "metadata": {"panels": [{"items": [{"jaql": {"dim": "[bank_churn_train.csv.Geography]", "table": "bank_churn_train.csv", "column": "Geography"}}]}]}}]}
    rows = _extract_dashboard_columns(export)
    assert (rows[0]["table"], rows[0]["column"]) == ("bank_churn_train.csv", "Geography")


# ---------------------------------------------------------------------------
# _extract_dashboard_references — full coverage, datasource gating, diagnostics.
# Locations come from a census of 509 live dashboards (2026-09-03).
# ---------------------------------------------------------------------------

_DS_ROOT = {"title": "Sales", "id": "aROOT", "fullname": "LocalHost/Sales", "live": False}
_DS_OTHER = {"title": "Other Model", "id": "aOTHER", "fullname": "LocalHost/Other Model", "live": False}


def _ref(dim, table=None, column=None, **extra):
    node = {"dim": dim}
    if table:
        node.update(table=table, column=column)
    node.update(extra)
    return node


def _pairs(rows):
    return [(r["source"], r["widget_id"], r["table"], r["column"]) for r in rows]


class TestDatasourceTitle:
    def test_forms(self):
        assert _datasource_title({"title": "Sales"}) == "sales"
        assert _datasource_title({"fullname": "live:fes_assistant"}) == "fes_assistant"
        assert _datasource_title({"fullname": "LocalHost/Sales"}) == "sales"
        assert _datasource_title(" Sales ") == "sales"
        assert _datasource_title({}) is None
        assert _datasource_title(None) is None
        assert _datasource_title({"title": ""}) is None


class TestExtractDashboardReferencesCoverage:
    def test_every_census_location_is_read(self):
        dash = {
            "title": "D",
            "datasource": _DS_ROOT,
            "filters": [{"jaql": _ref("[Orders.Country]", "Orders", "Country", filter={"by": _ref("[Orders.Amount]", "Orders", "Amount", agg="sum")})}],
            "defaultFilters": [{"levels": [_ref("[Orders.Date (Calendar)]", "Orders", "Date")]}],
            "hierarchies": [{"title": "Geo", "levels": [_ref("[Geo.Region]", "Geo", "Region"), _ref("[Geo.City]", "Geo", "City")]}],
            "widgets": [
                {
                    "oid": "w1",
                    "type": "pivot2",
                    "metadata": {
                        "panels": [
                            {
                                "items": [
                                    {
                                        "jaql": {
                                            "formula": "[A]/[B]",
                                            "context": {"[A]": _ref("[Orders.Amount]", "Orders", "Amount"), "[B]": {"formula": "[C]", "context": {"[C]": _ref("[Orders.Qty]", "Orders", "Qty")}}},
                                        },
                                        "format": {"color": {"conditions": [{"expression": {"jaql": {"formula": "[X]", "context": {"[X]": _ref("[Orders.Cost]", "Orders", "Cost")}}}}]}},
                                    },
                                    {"jaql": {"dimension": _ref("[Orders.Category]", "Orders", "Category")}},
                                    {"jaql": _ref("[Orders.Day]", "Orders", "Day"), "parent": {"jaql": _ref("[Orders.Month]", "Orders", "Month")}},
                                ]
                            }
                        ],
                        "drillHistory": [{"jaql": _ref("[Orders.Year]", "Orders", "Year"), "through": {"jaql": _ref("[Orders.Quarter]", "Orders", "Quarter")}}],
                    },
                    "query": {"metadata": [{"jaql": _ref("[Orders.Brand]", "Orders", "Brand")}]},
                    "style": {"tableState": {"headers": [_ref("[Orders.Brand]")]}},
                },
            ],
        }
        rows = _extract_dashboard_references(dash)["rows"]
        got = {(r["source"], r["table"], r["column"]) for r in rows}
        assert got == {
            ("filter", "Orders", "Country"),
            ("filter", "Orders", "Amount"),  # measured filter (filter.by)
            ("filter", "Orders", "Date"),  # defaultFilters levels
            ("hierarchy", "Geo", "Region"),
            ("hierarchy", "Geo", "City"),
            ("widget", "Orders", "Amount"),
            ("widget", "Orders", "Qty"),  # context nested two deep
            ("widget", "Orders", "Cost"),  # conditional formatting
            ("widget", "Orders", "Category"),  # jaql.dimension wrapper
            ("widget", "Orders", "Day"),
            ("widget", "Orders", "Month"),  # item.parent drill chain
            ("widget", "Orders", "Year"),  # drillHistory
            ("widget", "Orders", "Quarter"),  # drillHistory.through
            ("widget", "Orders", "Brand"),  # query.metadata and tableState.headers
        }
        assert all(r["widget_id"] == "w1" for r in rows if r["source"] == "widget")
        result = _extract_dashboard_references(dash)
        assert result["issues"] == []
        assert result["stats"]["unclassified"] == 0

    def test_no_placeholder_row_for_items_without_a_dim(self):
        dash = {"widgets": [{"oid": "w", "metadata": {"panels": [{"items": [{"jaql": {"title": "text only"}}, {"jaql": {"formula": "1", "context": {}}}]}]}}]}
        assert _extract_dashboard_references(dash)["rows"] == []


class TestExtractDashboardReferencesDatasourceGating:
    def _dash(self):
        return {
            "title": "D",
            "datasource": _DS_ROOT,
            "filters": [
                {"jaql": _ref("[Orders.Country]", "Orders", "Country", datasource=_DS_ROOT)},
                {"jaql": _ref("[Foreign.Thing]", "Foreign", "Thing", datasource=_DS_OTHER)},
                {"jaql": _ref("[Orders.Inherited]", "Orders", "Inherited")},
            ],
            "widgets": [
                {"oid": "w-root", "datasource": _DS_ROOT, "metadata": {"panels": [{"items": [{"jaql": _ref("[Orders.Amount]", "Orders", "Amount")}]}]}},
                {"oid": "w-other", "type": "chart/bar", "title": "Other", "datasource": _DS_OTHER, "metadata": {"panels": [{"items": [{"jaql": _ref("[Foreign.Col]", "Foreign", "Col")}]}]}},
                {"oid": "w-none", "metadata": {"panels": [{"items": [{"jaql": _ref("[Orders.Qty]", "Orders", "Qty")}]}]}},
            ],
        }

    def test_without_a_datasource_everything_is_kept(self):
        result = _extract_dashboard_references(self._dash())
        assert {(r["table"], r["column"]) for r in result["rows"]} == {
            ("Orders", "Country"),
            ("Foreign", "Thing"),
            ("Orders", "Inherited"),
            ("Orders", "Amount"),
            ("Foreign", "Col"),
            ("Orders", "Qty"),
        }
        assert result["skipped_widgets"] == []

    def test_with_a_datasource_only_its_references_count(self):
        result = _extract_dashboard_references(self._dash(), datasource="sales")
        assert {(r["table"], r["column"]) for r in result["rows"]} == {("Orders", "Country"), ("Orders", "Inherited"), ("Orders", "Amount"), ("Orders", "Qty")}
        assert [w["widget_id"] for w in result["skipped_widgets"]] == ["w-other"]
        kinds = {i["kind"] for i in result["issues"]}
        assert kinds == {"widget_other_datasource", "reference_other_datasource"}
        assert result["stats"]["widgets_scanned"] == 2 and result["stats"]["widgets_skipped"] == 1

    def test_datasource_matches_by_title_case_insensitively_or_by_dict(self):
        assert len(_extract_dashboard_references(self._dash(), datasource="SALES")["rows"]) == 4
        assert len(_extract_dashboard_references(self._dash(), datasource=_DS_ROOT)["rows"]) == 4

    def test_skipped_widget_references_are_not_reported_as_unclassified(self):
        result = _extract_dashboard_references(self._dash(), datasource="sales")
        assert result["stats"]["unclassified"] == 0


class TestExtractDashboardReferencesDiagnostics:
    def test_blox_and_scripts_are_flagged_not_analysed(self):
        dash = {"script": "prism.on('x', ...)", "widgets": [{"oid": "b", "type": "BloX", "script": "var a = [0];", "metadata": {"panels": []}}]}
        issues = _extract_dashboard_references(dash)["issues"]
        assert sorted((i["kind"], i["widget_id"]) for i in issues) == [("blox_widget", "b"), ("script_present", "N/A"), ("script_present", "b")]
        assert all(i["severity"] == "warning" for i in issues)

    def test_unreadable_dim_is_an_error_but_still_a_row(self):
        dash = {"filters": [{"jaql": {"dim": "not a reference"}}]}
        result = _extract_dashboard_references(dash)
        assert [(i["severity"], i["kind"]) for i in result["issues"]] == [("error", "unreadable_dim")]
        assert len(result["rows"]) == 1  # kept, conservatively

    def test_ambiguous_dim_without_schema_is_a_warning(self):
        dash = {"filters": [{"jaql": {"dim": "[T1.csv.C1]"}}]}
        result = _extract_dashboard_references(dash)
        assert [i["kind"] for i in result["issues"]] == ["ambiguous_dim"]
        assert _extract_dashboard_references(dash, known_columns={("T1.csv", "C1")})["issues"] == []

    def test_reference_at_an_unexpected_location_is_kept_and_flagged(self):
        dash = {"widgets": [{"oid": "w", "somePlugin": {"config": _ref("[Orders.Secret]", "Orders", "Secret")}}], "options": {"x": _ref("[Orders.Top]", "Orders", "Top")}}
        result = _extract_dashboard_references(dash)
        assert _pairs(result["rows"]) == [("widget", "w", "Orders", "Secret"), ("filter", "N/A", "Orders", "Top")]
        assert [(i["kind"], i["widget_id"]) for i in result["issues"]] == [("unclassified_location", "w"), ("unclassified_location", "N/A")]
        assert result["stats"]["unclassified"] == 2

    def test_stats(self):
        dash = {"filters": [{}], "defaultFilters": [{}, {}], "hierarchies": [{}], "widgets": [{"oid": "w"}]}
        assert _extract_dashboard_references(dash)["stats"] == {
            "filters": 1,
            "default_filters": 2,
            "hierarchies": 1,
            "widgets": 1,
            "widgets_scanned": 1,
            "widgets_skipped": 0,
            "unclassified": 0,
            "rows": 0,
        }

    def test_not_a_dict(self):
        assert _extract_dashboard_references(None)["rows"] == []
        assert _extract_dashboard_columns("nope") == []
