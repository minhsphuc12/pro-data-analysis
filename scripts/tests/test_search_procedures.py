"""
Unit tests for search_procedures.py.
Test pure helpers (_match, _oracle_build_text_filter, _to_oracle_literal, _executable_sql),
formatters (format_text, format_json, format_markdown), and search_procedures with mocked DB.
"""
import io
import json
import pytest
from unittest.mock import patch, MagicMock

import search_procedures as sp


# =============================================================================
# _match
# =============================================================================

class TestMatch:
    """[Test] _match: substring and regex, empty/None."""

    def test_substring_match_case_insensitive(self):
        assert sp._match("hello WORLD", "world", use_regex=False) is True
        assert sp._match("HELLO", "hello", use_regex=False) is True

    def test_substring_no_match(self):
        assert sp._match("hello", "xyz", use_regex=False) is False

    def test_regex_match(self):
        assert sp._match("INSERT INTO t1", r"INSERT\s+INTO", use_regex=True) is True
        assert sp._match("Dim_Customer", r"dim_.*", use_regex=True) is True

    def test_regex_no_match(self):
        assert sp._match("SELECT * FROM t", r"INSERT\s+INTO", use_regex=True) is False

    def test_empty_or_none_returns_false(self):
        assert sp._match("", "x", use_regex=False) is False
        assert sp._match("x", "", use_regex=False) is False
        assert sp._match(None, "x", use_regex=False) is False
        assert sp._match("x", None, use_regex=False) is False


# =============================================================================
# _oracle_build_text_filter
# =============================================================================

class TestOracleBuildTextFilter:
    """[Test] _oracle_build_text_filter: builds SQL condition and populates params."""

    def test_table_only_no_regex(self):
        params = {}
        cond = sp._oracle_build_text_filter("DIM_CUST", None, False, params)
        assert "table_val" in params
        assert params["table_val"] == "DIM_CUST"
        assert "UPPER(TEXT) LIKE '%' || :table_val || '%'" in cond

    def test_table_only_regex(self):
        params = {}
        cond = sp._oracle_build_text_filter("DIM_\\w+", None, True, params)
        assert "re_table" in params
        assert "REGEXP_LIKE(TEXT, :re_table, 'i')" in cond

    def test_text_content_only(self):
        params = {}
        cond = sp._oracle_build_text_filter(None, "INSERT INTO", False, params)
        assert params.get("text_val") == "INSERT INTO"
        assert "UPPER(TEXT) LIKE '%' || :text_val || '%'" in cond

    def test_both_table_and_text_or_conditions(self):
        params = {}
        cond = sp._oracle_build_text_filter("T1", "COMMIT", False, params)
        assert "table_val" in params and "text_val" in params
        assert ") OR (" in cond

    def test_no_table_no_text_returns_1eq0(self):
        params = {}
        cond = sp._oracle_build_text_filter(None, None, False, params)
        assert cond == "1=0"


# =============================================================================
# _to_oracle_literal
# =============================================================================

class TestToOracleLiteral:
    """[Test] _to_oracle_literal: escape quotes and handle None."""

    def test_none_returns_null(self):
        assert sp._to_oracle_literal(None) == "NULL"

    def test_simple_string(self):
        assert sp._to_oracle_literal("hello") == "'hello'"

    def test_escapes_single_quotes(self):
        assert sp._to_oracle_literal("it's") == "'it''s'"


# =============================================================================
# _executable_sql
# =============================================================================

class TestExecutableSql:
    """[Test] _executable_sql: substitute bind params with literals."""

    def test_substitutes_string_params(self):
        sql = "SELECT * FROM t WHERE name = :n"
        out = sp._executable_sql(sql, {"n": "O'Brien"})
        assert "O'Brien" in out or "''" in out
        assert ":n" not in out

    def test_longer_param_names_first(self):
        sql = "SELECT :a, :ab FROM t"
        out = sp._executable_sql(sql, {"a": "1", "ab": "2"})
        assert ":a" not in out and ":ab" not in out


# =============================================================================
# Formatters
# =============================================================================

class TestFormatText:
    """[Test] format_text output."""

    def test_empty_results(self):
        assert sp.format_text([]) == "No matching procedure/package found."

    def test_single_result_no_match_count(self):
        results = [{"schema": "S", "name": "P", "type": "PROCEDURE", "lines": [{"line": 1, "text": "BEGIN NULL; END;"}], "match_count": 0, "matching_line_numbers": []}]
        out = sp.format_text(results)
        assert "1 object(s)" in out
        assert "[PROCEDURE] S.P" in out
        assert "BEGIN NULL; END;" in out

    def test_result_with_match_count(self):
        results = [{"schema": "S", "name": "P", "type": "PACKAGE", "match_count": 3, "matching_line_numbers": [1, 2, 3], "lines": [{"line": 1, "text": "x"}]}]
        out = sp.format_text(results)
        assert "3 lines reference search term" in out


class TestFormatJson:
    """[Test] format_json output."""

    def test_empty_list(self):
        assert sp.format_json([]) == "[]"

    def test_serializes_results(self):
        results = [{"schema": "A", "name": "B", "type": "FUNCTION"}]
        out = sp.format_json(results)
        parsed = json.loads(out)
        assert parsed == results


class TestFormatMarkdown:
    """[Test] format_markdown output."""

    def test_empty_results(self):
        assert sp.format_markdown([]) == "No matching procedure/package found."

    def test_includes_code_block(self):
        results = [{"schema": "S", "name": "P", "type": "PROCEDURE", "lines": [{"line": 1, "text": "code"}], "match_count": 0, "matching_line_numbers": []}]
        out = sp.format_markdown(results)
        assert "```" in out
        assert "code" in out


# =============================================================================
# search_procedures (entry point with mocked DB)
# =============================================================================

class TestSearchProcedures:
    """[Test] search_procedures: non-Oracle raises; Oracle path uses get_connection."""

    def test_raises_value_error_for_non_oracle(self):
        with patch.object(sp, "get_db_type", return_value="postgresql"):
            with pytest.raises(ValueError) as exc:
                sp.search_procedures(table_name="T", db_alias="X")
            assert "Oracle" in str(exc.value)

    def test_oracle_fetch_by_name_path(self):
        mock_cursor = MagicMock()
        mock_cursor.__iter__ = lambda self: iter([
            ("OWNER1", "PKG_FOO", "PACKAGE", 1, "line1"),
            ("OWNER1", "PKG_FOO", "PACKAGE", 2, "line2"),
        ])
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch.object(sp, "get_db_type", return_value="oracle"), \
             patch.object(sp, "get_connection", return_value=mock_conn):
            results = sp.search_procedures(object_name="OWNER1.PKG_FOO", db_alias="DWH")
        assert len(results) == 1
        assert results[0]["schema"] == "OWNER1" and results[0]["name"] == "PKG_FOO"
        assert results[0]["type"] == "PACKAGE"
        assert len(results[0]["lines"]) == 2

    def test_oracle_search_path_with_table_returns_from_cursor(self):
        first_rows = [
            ("SCHEMA1", "PROC1", "PROCEDURE", 5, "  INSERT INTO DIM_CUSTOMER"),
        ]
        second_rows = [
            ("SCHEMA1", "PROC1", "PROCEDURE", 1, "PROCEDURE proc1 AS"),
            ("SCHEMA1", "PROC1", "PROCEDURE", 2, "BEGIN"),
            ("SCHEMA1", "PROC1", "PROCEDURE", 5, "  INSERT INTO DIM_CUSTOMER"),
        ]
        mock_cursor = MagicMock()
        mock_cursor.execute = MagicMock()
        mock_cursor.__iter__ = MagicMock(side_effect=[iter(first_rows), iter(second_rows)])
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch.object(sp, "get_db_type", return_value="oracle"), \
             patch.object(sp, "get_connection", return_value=mock_conn):
            results = sp.search_procedures(table_name="DIM_CUSTOMER", db_alias="DWH")
        assert len(results) == 1
        assert results[0]["schema"] == "SCHEMA1" and results[0]["name"] == "PROC1"
        assert results[0]["match_count"] == 1
        assert len(results[0]["lines"]) == 3

    def test_oracle_search_empty_table_and_text_returns_empty(self):
        with patch.object(sp, "get_db_type", return_value="oracle"), \
             patch.object(sp, "get_connection") as mock_get_conn:
            mock_conn = MagicMock()
            mock_conn.__enter__ = MagicMock(return_value=mock_conn)
            mock_conn.__exit__ = MagicMock(return_value=False)
            mock_get_conn.return_value = mock_conn
            results = sp.search_procedures(table_name=None, text_content=None, db_alias="DWH")
        assert results == []


# =============================================================================
# _oracle_fetch_by_name (with mock cursor)
# =============================================================================

class TestOracleFetchByName:
    """[Test] _oracle_fetch_by_name: groups rows by (owner, name, type), applies limit_lines."""

    def test_groups_lines_by_object(self):
        cursor = MagicMock()
        cursor.execute = MagicMock()
        cursor.__iter__ = lambda self: iter([
            ("S1", "PKG_A", "PACKAGE", 1, "line1"),
            ("S1", "PKG_A", "PACKAGE", 2, "line2"),
        ])
        results = sp._oracle_fetch_by_name(cursor, object_name="PKG_A", object_owner=None, object_types=["PACKAGE"], limit_lines_per_object=0, show_query=False)
        assert len(results) == 1
        assert results[0]["schema"] == "S1" and results[0]["name"] == "PKG_A"
        assert [ln["text"] for ln in results[0]["lines"]] == ["line1", "line2"]

    def test_limit_lines_per_object_truncates(self):
        cursor = MagicMock()
        cursor.execute = MagicMock()
        cursor.__iter__ = lambda self: iter([
            ("S1", "P", "PROCEDURE", i, f"line{i}") for i in range(1, 11)
        ])
        results = sp._oracle_fetch_by_name(cursor, object_name="P", object_owner="S1", object_types=["PROCEDURE"], limit_lines_per_object=3, show_query=False)
        assert len(results[0]["lines"]) == 3
        assert results[0]["lines"][0]["text"] == "line1"


# =============================================================================
# _oracle_search_procedures (with mock cursor)
# =============================================================================

class TestOracleSearchProcedures:
    """[Test] _oracle_search_procedures: no table/text returns []; filter and limit logic."""

    def test_no_table_no_text_returns_empty(self):
        cursor = MagicMock()
        assert sp._oracle_search_procedures(cursor, table_name=None, text_content=None, schema=None, object_types=["PROCEDURE"], use_regex=False, limit_objects=10, limit_lines_per_object=0) == []

    def test_returns_filtered_objects_with_full_source(self):
        search_rows = [
            ("S1", "PROC1", "PROCEDURE", 2, "  INSERT INTO DIM_CUST (id) VALUES (1)"),
        ]
        full_rows = [
            ("S1", "PROC1", "PROCEDURE", 1, "PROCEDURE proc1 AS"),
            ("S1", "PROC1", "PROCEDURE", 2, "  INSERT INTO DIM_CUST (id) VALUES (1)"),
        ]
        cursor = MagicMock()
        cursor.execute = MagicMock()
        cursor.__iter__ = MagicMock(side_effect=[iter(search_rows), iter(full_rows)])

        results = sp._oracle_search_procedures(
            cursor,
            table_name="DIM_CUST",
            text_content=None,
            schema=None,
            object_types=["PROCEDURE"],
            use_regex=False,
            limit_objects=10,
            limit_lines_per_object=0,
        )
        assert len(results) == 1
        assert results[0]["schema"] == "S1" and results[0]["name"] == "PROC1"
        assert results[0]["match_count"] == 1
        assert results[0]["matching_line_numbers"] == [2]
        assert len(results[0]["lines"]) == 2
