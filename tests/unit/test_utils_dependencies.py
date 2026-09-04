"""Data model column dependencies: what a set of columns needs beyond itself."""

from __future__ import annotations

from pysisense.utils import _build_schema_index as build_schema_index
from pysisense.utils import _compute_dependency_closure as compute_dependency_closure


def _col(oid, name, expression=None):
    return {"oid": oid, "name": name, "expression": expression, "isCustom": expression is not None}


def _table(oid, name, columns, sql=None):
    t = {"oid": oid, "name": name, "type": "custom" if sql else "base", "columns": columns}
    if sql:
        t["expression"] = {"oid": f"{oid}-expr", "expression": sql, "isDatabaseDialect": False}
    return t


def _rel(*ends):
    return {"oid": "-".join(e[1] for e in ends), "columns": [{"dataset": "ds", "table": t, "column": c, "isDropped": None} for t, c in ends], "type": None}


# Orders -- Customers -- Regions      (chain)
# Orders -- Products                  (leaf)
# Orders -- ShipA -- Hubs, Orders -- ShipB -- Hubs   (diamond: two shortest paths Orders..Hubs)
# Islands: no relation
SCHEMA = {
    "datasets": [
        {
            "oid": "ds",
            "schema": {
                "tables": [
                    _table(
                        "T_ord",
                        "Orders",
                        [
                            _col("c_oid", "OrderID"),
                            _col("c_cust", "CustomerID"),
                            _col("c_prod", "ProductID"),
                            _col("c_amt", "Amount"),
                            _col("c_qty", "Qty"),
                            _col("c_total", "Total", "[Amount] * [Qty]"),
                            _col("c_shipa", "ShipA_ID"),
                            _col("c_shipb", "ShipB_ID"),
                        ],
                    ),
                    _table(
                        "T_cus",
                        "Customers",
                        [
                            _col("c_cid", "CustomerID"),
                            _col("c_cname", "Name"),
                            _col("c_rid", "RegionID"),
                            _col("c_html", "NameHTML", "'<b>' + [Customers].[Name] + '</b>'"),
                            _col("c_bare", "Upper", "case when name in ('x') then upper(name) else null end"),
                        ],
                    ),
                    _table("T_reg", "Regions", [_col("c_rgid", "RegionID"), _col("c_rname", "RegionName")]),
                    _table("T_prd", "Products", [_col("c_pid", "ProductID"), _col("c_pname", "ProductName"), _col("c_cross", "CrossRef", "[Regions].[RegionName]")]),
                    _table("T_sa", "ShipA", [_col("c_sa_o", "OrderRef"), _col("c_sa_h", "HubRef")]),
                    _table("T_sb", "ShipB", [_col("c_sb_o", "OrderRef"), _col("c_sb_h", "HubRef")]),
                    _table("T_hub", "Hubs", [_col("c_h_id", "HubID"), _col("c_h_name", "HubName")]),
                    _table("T_isl", "Islands", [_col("c_i1", "A"), _col("c_i2", "B")]),
                    _table("T_view", "OrderView", [_col("c_v1", "OrderID"), _col("c_v2", "rev")], sql="select o.[OrderID], o.[Amount] as rev from [Orders] o where o.[Qty] > 1"),
                    _table("T_star", "AllProducts", [_col("c_s1", "x")], sql="select * from [Products]"),
                    _table("T_bad", "Broken", [_col("c_b1", "x")], sql="select a.[x] from [NoSuchTable] a"),
                    _table("T_cte", "WithCte", [_col("c_w1", "x")], sql="with t as (select [HubID] from [Hubs]) select * from t"),
                    None,
                ]
            },
        },
        "not-a-dataset",
    ],
    "relations": [
        _rel(("T_ord", "c_cust"), ("T_cus", "c_cid")),
        _rel(("T_cus", "c_rid"), ("T_reg", "c_rgid")),
        _rel(("T_ord", "c_prod"), ("T_prd", "c_pid")),
        _rel(("T_ord", "c_shipa"), ("T_sa", "c_sa_o")),
        _rel(("T_sa", "c_sa_h"), ("T_hub", "c_h_id")),
        _rel(("T_ord", "c_shipb"), ("T_sb", "c_sb_o")),
        _rel(("T_sb", "c_sb_h"), ("T_hub", "c_h_id")),
        {"oid": "malformed", "columns": [{"table": "T_ord"}]},
        None,
    ],
}
INDEX = build_schema_index(SCHEMA)


def _keys(result):
    return set(result["retained"])


def _reasons(result, key):
    return {r["reason"] for r in result["retained"][key]}


class TestBuildSchemaIndex:
    def test_tables_columns_and_names(self):
        assert set(INDEX["tables"]) == {"T_ord", "T_cus", "T_reg", "T_prd", "T_sa", "T_sb", "T_hub", "T_isl", "T_view", "T_star", "T_bad", "T_cte"}
        assert INDEX["tables"]["T_ord"]["columns_by_name"]["amount"] == "c_amt"
        assert INDEX["tables_by_name"]["orders"] == ["T_ord"]
        assert INDEX["tables"]["T_view"]["sql"].startswith("select o.[OrderID]")
        assert INDEX["tables"]["T_ord"]["sql"] is None
        assert INDEX["tables"]["T_ord"]["columns"]["c_total"]["is_custom"] is True

    def test_relations_keep_only_well_formed_groups(self):
        assert len(INDEX["relations"]) == 7

    def test_tolerates_garbage(self):
        assert build_schema_index(None)["tables"] == {}
        assert build_schema_index({"datasets": [None, {"schema": None}]})["tables"] == {}


class TestJoinPaths:
    def test_direct_relation_keeps_both_join_columns(self):
        result = compute_dependency_closure(INDEX, {("T_ord", "c_amt"), ("T_cus", "c_cname")}, custom_columns=False, custom_tables=False)
        assert _keys(result) == {("T_ord", "c_cust"), ("T_cus", "c_cid")}
        assert _reasons(result, ("T_ord", "c_cust")) == {"join_column"}
        assert result["tables"] == {}

    def test_chain_keeps_intermediate_table_with_only_its_join_columns(self):
        result = compute_dependency_closure(INDEX, {("T_ord", "c_amt"), ("T_reg", "c_rname")}, custom_columns=False, custom_tables=False)
        assert _keys(result) == {("T_ord", "c_cust"), ("T_cus", "c_cid"), ("T_cus", "c_rid"), ("T_reg", "c_rgid")}
        assert set(result["tables"]) == {"T_cus"}
        assert result["tables"]["T_cus"][0]["reason"] == "join_path_table"
        assert result["join_paths"] == [{"from": "T_ord", "to": "T_reg", "tables": ["T_ord", "T_cus", "T_reg"]}]

    def test_diamond_keeps_every_shortest_path(self):
        result = compute_dependency_closure(INDEX, {("T_ord", "c_amt"), ("T_hub", "c_h_name")}, custom_columns=False, custom_tables=False)
        assert _keys(result) == {("T_ord", "c_shipa"), ("T_sa", "c_sa_o"), ("T_sa", "c_sa_h"), ("T_ord", "c_shipb"), ("T_sb", "c_sb_o"), ("T_sb", "c_sb_h"), ("T_hub", "c_h_id")}
        assert set(result["tables"]) == {"T_sa", "T_sb"}

    def test_unrelated_tables_are_reported_not_invented(self):
        result = compute_dependency_closure(INDEX, {("T_ord", "c_amt"), ("T_isl", "c_i1")}, custom_columns=False, custom_tables=False)
        assert _keys(result) == set()
        assert [(i["severity"], i["kind"]) for i in result["issues"]] == [("info", "tables_not_joined")]

    def test_used_columns_are_never_listed_as_retained(self):
        result = compute_dependency_closure(INDEX, {("T_ord", "c_cust"), ("T_cus", "c_cid")}, custom_columns=False, custom_tables=False)
        assert _keys(result) == set()

    def test_single_table_needs_nothing(self):
        assert compute_dependency_closure(INDEX, {("T_ord", "c_amt")}, custom_columns=False, custom_tables=False)["retained"] == {}

    def test_switch_off(self):
        result = compute_dependency_closure(INDEX, {("T_ord", "c_amt"), ("T_reg", "c_rname")}, join_paths=False, custom_columns=False, custom_tables=False)
        assert result["retained"] == {} and result["tables"] == {}


class TestCustomColumns:
    def test_same_table_bracket_tokens(self):
        result = compute_dependency_closure(INDEX, {("T_ord", "c_total")}, join_paths=False, custom_tables=False)
        assert _keys(result) == {("T_ord", "c_amt"), ("T_ord", "c_qty")}
        assert result["retained"][("T_ord", "c_amt")][0]["required_by"] == ("T_ord", "c_total")

    def test_two_part_token_reaches_another_table_and_then_joins_to_it(self):
        result = compute_dependency_closure(INDEX, {("T_prd", "c_cross")}, custom_tables=False)
        # the formula reads Regions.RegionName, and Regions then needs a join path from Products
        assert ("T_reg", "c_rname") in _keys(result)
        assert {"T_ord", "T_cus"} <= set(result["tables"])  # Products -> Orders -> Customers -> Regions

    def test_two_part_token_naming_own_table(self):
        result = compute_dependency_closure(INDEX, {("T_cus", "c_html")}, join_paths=False, custom_tables=False)
        assert _keys(result) == {("T_cus", "c_cname")}

    def test_bare_column_names_in_sql_style_formulas(self):
        # live-observed: "case when employeename in (...) then null else employeename end"
        result = compute_dependency_closure(INDEX, {("T_cus", "c_bare")}, join_paths=False, custom_tables=False)
        assert _keys(result) == {("T_cus", "c_cname")}
        assert result["issues"] == []

    def test_unresolved_token_is_a_warning(self):
        schema = {"datasets": [{"oid": "d", "schema": {"tables": [_table("T", "T", [_col("a", "A"), _col("b", "B", "[Nope] + [Other].[X]")])]}}], "relations": []}
        result = compute_dependency_closure(build_schema_index(schema), {("T", "b")})
        assert result["retained"] == {}
        assert sorted(i["kind"] for i in result["issues"]) == ["custom_column_token_unresolved", "custom_column_token_unresolved"]

    def test_transitive_custom_columns(self):
        schema = {"datasets": [{"oid": "d", "schema": {"tables": [_table("T", "T", [_col("a", "A"), _col("b", "B", "[A]"), _col("c", "C", "[B] * 2")])]}}], "relations": []}
        result = compute_dependency_closure(build_schema_index(schema), {("T", "c")})
        assert _keys(result) == {("T", "b"), ("T", "a")}


class TestCustomTables:
    def test_mode_all_keeps_every_column_of_the_source(self):
        result = compute_dependency_closure(INDEX, {("T_view", "c_v2")}, join_paths=False, custom_columns=False)
        assert _keys(result) == {("T_ord", c) for c in INDEX["tables"]["T_ord"]["columns"]}
        assert set(result["tables"]) == {"T_ord"}
        assert result["tables"]["T_ord"][0]["reason"] == "custom_table_source"

    def test_mode_parsed_keeps_only_named_columns(self):
        result = compute_dependency_closure(INDEX, {("T_view", "c_v2")}, join_paths=False, custom_columns=False, custom_table_columns="parsed")
        assert _keys(result) == {("T_ord", "c_oid"), ("T_ord", "c_amt"), ("T_ord", "c_qty")}

    def test_select_star_keeps_all_even_in_parsed_mode(self):
        result = compute_dependency_closure(INDEX, {("T_star", "c_s1")}, join_paths=False, custom_columns=False, custom_table_columns="parsed")
        assert _keys(result) == {("T_prd", c) for c in INDEX["tables"]["T_prd"]["columns"]}

    def test_unknown_source_table_is_an_error(self):
        result = compute_dependency_closure(INDEX, {("T_bad", "c_b1")}, join_paths=False, custom_columns=False)
        assert result["retained"] == {}
        assert [(i["severity"], i["kind"]) for i in result["issues"]] == [("error", "custom_table_sql_unresolved")]

    def test_cte_is_flagged_and_still_parsed(self):
        result = compute_dependency_closure(INDEX, {("T_cte", "c_w1")}, join_paths=False, custom_columns=False)
        assert ("T_hub", "c_h_id") in _keys(result)
        assert "custom_table_sql_complex" in {i["kind"] for i in result["issues"]}

    def test_custom_table_source_columns_feed_custom_column_closure(self):
        # AllProducts selects * from Products; Products.CrossRef reads Regions.RegionName -> retained transitively
        result = compute_dependency_closure(INDEX, {("T_star", "c_s1")}, join_paths=False)
        assert ("T_reg", "c_rname") in _keys(result)


def test_options_echoed_and_unknown_used_ignored():
    result = compute_dependency_closure(INDEX, {("nope", "x"), ("T_ord", "c_amt")})
    assert result["options"] == {"join_paths": True, "custom_columns": True, "custom_tables": True, "custom_table_columns": "all"}
    assert result["retained"] == {}


def test_index_keeps_display_and_original_names_as_aliases():
    schema = {
        "datasets": [{"oid": "d", "schema": {"tables": [{"oid": "T", "name": "T", "columns": [{"oid": "a", "name": "test", "id": "Brand", "displayName": "Brand Name"}, {"oid": "b", "name": "B"}]}]}}],
        "relations": [],
    }
    table = build_schema_index(schema)["tables"]["T"]
    assert table["columns"]["a"]["id"] == "Brand" and table["columns"]["a"]["display_name"] == "Brand Name"
    assert table["columns_by_name"] == {"test": "a", "b": "b"}
    assert table["columns_by_alias"] == {"brand": "a", "brand name": "a"}
