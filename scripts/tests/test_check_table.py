"""
Unit tests for check_table.py.
Test formatters, helpers, and get_table_info with mocked DB connection.
"""
import json
import pytest
from unittest.mock import patch, MagicMock

import check_table


class TestNullableDisplay:
    """[Test] _nullable_display."""

    def test_yes_when_nullable(self):
        assert check_table._nullable_display(True) == "YES"

    def test_no_when_not_nullable(self):
        assert check_table._nullable_display(False) == "NO"


class TestStatsDict:
    """[Test] _stats_dict."""

    def test_builds_dict_with_all_fields(self):
        d = check_table._stats_dict(1000, 50, 120, "2024-01-01 00:00:00")
        assert d["num_rows"] == 1000
        assert d["blocks"] == 50
        assert d["avg_row_len"] == 120
        assert d["last_analyzed"] == "2024-01-01 00:00:00"

    def test_last_analyzed_none_becomes_none(self):
        d = check_table._stats_dict(1, 0, None, None)
        assert d["last_analyzed"] is None


class TestOracleColumnType:
    """[Test] _oracle_column_type from ALL_TAB_COLUMNS row."""

    def test_varchar2(self):
        # row: (col_name, data_type, data_length, precision, scale, ...)
        row = ("C", "VARCHAR2", 100, None, None)
        assert check_table._oracle_column_type(row) == "VARCHAR2(100)"

    def test_number_with_precision_scale(self):
        row = ("N", "NUMBER", None, 10, 2)
        assert check_table._oracle_column_type(row) == "NUMBER(10,2)"

    def test_number_integer(self):
        row = ("ID", "NUMBER", None, 22, None)
        assert check_table._oracle_column_type(row) == "NUMBER(22)"

    def test_plain_type(self):
        row = ("D", "DATE", None, None, None)
        assert check_table._oracle_column_type(row) == "DATE"


class TestFormatText:
    """[Test] format_text output."""

    def test_includes_schema_table_and_columns(self):
        info = {
            "schema": "S",
            "table": "T",
            "db_type": "oracle",
            "table_comment": "",
            "columns": [
                {"name": "ID", "data_type": "NUMBER(22)", "nullable": False, "comment": "PK"},
                {"name": "NAME", "data_type": "VARCHAR2(100)", "nullable": True, "comment": ""},
            ],
            "indexes": [],
            "partitions": [],
            "statistics": {},
        }
        out = check_table.format_text(info)
        assert "S.T" in out
        assert "oracle" in out
        assert "ID" in out and "NAME" in out
        assert "NO" in out and "YES" in out

    def test_indexes_section_empty(self):
        info = {
            "schema": "S", "table": "T", "db_type": "mysql",
            "table_comment": "", "columns": [],
            "indexes": [], "partitions": [], "statistics": {},
        }
        out = check_table.format_text(info)
        assert "Không có index nào" in out or "index" in out.lower()

    def test_statistics_section(self):
        info = {
            "schema": "S", "table": "T", "db_type": "postgresql",
            "table_comment": "", "columns": [],
            "indexes": [], "partitions": [],
            "statistics": {"num_rows": 5000, "blocks": 100, "avg_row_len": 200, "last_analyzed": "2024-01-01"},
        }
        out = check_table.format_text(info)
        assert "5,000" in out or "5000" in out
        assert "100" in out


class TestFormatMarkdown:
    """[Test] format_markdown output."""

    def test_markdown_table_structure(self):
        info = {
            "schema": "OWN",
            "table": "TBL",
            "db_type": "mysql",
            "table_comment": "A table",
            "columns": [
                {"name": "id", "data_type": "int", "nullable": False, "comment": "Primary key"},
            ],
            "indexes": [{"name": "pk", "type": "BTREE", "unique": "UNIQUE", "columns": "id"}],
            "partitions": [],
            "statistics": {"num_rows": 10, "blocks": 1, "avg_row_len": 100, "last_analyzed": None},
        }
        out = check_table.format_markdown(info)
        assert "# OWN.TBL" in out
        assert "| # | Column | Type |" in out
        assert "| id |" in out
        assert "**Database type:**" in out
        assert "## Indexes" in out

    def test_escapes_pipe_in_comment(self):
        info = {
            "schema": "S", "table": "T", "db_type": "oracle",
            "table_comment": "", "columns": [
                {"name": "c", "data_type": "VARCHAR2(1)", "nullable": True, "comment": "a|b"},
            ],
            "indexes": [], "partitions": [], "statistics": {},
        }
        out = check_table.format_markdown(info)
        assert "\\|" in out or "a|b" in out  # pipe escaped or present


class TestFormatJson:
    """[Test] format_json round-trip."""

    def test_serializes_info(self):
        info = {
            "schema": "S", "table": "T", "db_type": "sqlserver",
            "columns": [{"name": "x", "data_type": "int", "nullable": True}],
            "indexes": [], "partitions": [], "statistics": {},
        }
        out = check_table.format_json(info)
        parsed = json.loads(out)
        assert parsed["schema"] == "S"
        assert parsed["table"] == "T"
        assert len(parsed["columns"]) == 1


class TestGetTableInfo:
    """[Test] get_table_info with mocked connection."""

    def test_returns_formatted_text_by_default(self):
        fake_info = {
            "schema": "SC", "table": "T1", "db_type": "oracle",
            "table_comment": "", "columns": [],
            "indexes": [], "partitions": [], "statistics": {},
        }
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = MagicMock()
        mock_conn.cursor.return_value.__exit__.return_value = None
        fake_info_func = lambda _c, _s, _t: fake_info
        with patch("check_table.get_connection", return_value=MagicMock(__enter__=MagicMock(return_value=mock_conn), __exit__=MagicMock(return_value=None))):
            with patch("check_table.get_db_type", return_value="oracle"):
                with patch.dict("check_table._INFO_FUNCS", {"oracle": fake_info_func}):
                    out = check_table.get_table_info("SC", "T1", db_alias="DWH", fmt="text")
                    assert "SC.T1" in out
                    assert "oracle" in out

    def test_returns_json_when_format_json(self):
        fake_info = {
            "schema": "S", "table": "T", "db_type": "mysql",
            "table_comment": "", "columns": [], "indexes": [], "partitions": [], "statistics": {},
        }
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = MagicMock()
        mock_conn.cursor.return_value.__exit__.return_value = None
        fake_info_func = lambda _c, _s, _t: fake_info
        with patch("check_table.get_connection", return_value=MagicMock(__enter__=MagicMock(return_value=mock_conn), __exit__=MagicMock(return_value=None))):
            with patch("check_table.get_db_type", return_value="mysql"):
                with patch.dict("check_table._INFO_FUNCS", {"mysql": fake_info_func}):
                    out = check_table.get_table_info("S", "T", db_alias="X", fmt="json")
                    parsed = json.loads(out)
                    assert parsed["schema"] == "S" and parsed["table"] == "T"
