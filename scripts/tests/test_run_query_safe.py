"""
Unit tests for run_query_safe.py.
Test _wrap_with_limit, formatters, and run_query (non-SELECT raises).
"""
import json
import pytest
from unittest.mock import patch, MagicMock

import run_query_safe


class TestWrapWithLimit:
    """[Test] _wrap_with_limit in run_query_safe (per-dialect)."""

    def test_oracle(self):
        out = run_query_safe._wrap_with_limit("SELECT * FROM t", 10, "oracle")
        assert "ROWNUM <= 10" in out
        assert "SELECT * FROM t" in out

    def test_mysql_and_postgresql(self):
        for db in ("mysql", "postgresql"):
            out = run_query_safe._wrap_with_limit("SELECT * FROM t", 5, db)
            assert "LIMIT 5" in out
            assert "_limited" in out

    def test_sqlserver(self):
        out = run_query_safe._wrap_with_limit("SELECT * FROM t", 3, "sqlserver")
        assert "TOP 3" in out

    def test_strips_semicolon(self):
        out = run_query_safe._wrap_with_limit("SELECT 1;", 1, "mysql")
        assert out.endswith("LIMIT 1")


class TestFormatText:
    """[Test] format_text result dict -> string."""

    def test_no_columns_message(self):
        result = {"columns": [], "rows": [], "row_count": 0, "execution_time_ms": 1.0}
        out = run_query_safe.format_text(result)
        assert "no columns" in out.lower() or "Query executed" in out

    def test_header_and_rows(self):
        result = {
            "columns": ["A", "B"],
            "rows": [[1, "x"], [2, "y"]],
            "row_count": 2,
            "execution_time_ms": 10.5,
            "truncated": False,
        }
        out = run_query_safe.format_text(result)
        assert "A" in out and "B" in out
        assert "1" in out and "2" in out
        assert "2 rows" in out
        assert "10.5" in out or "10" in out

    def test_truncated_footer(self):
        result = {
            "columns": ["X"],
            "rows": [[i] for i in range(100)],
            "row_count": 100,
            "execution_time_ms": 0,
            "truncated": True,
            "row_limit": 100,
        }
        out = run_query_safe.format_text(result)
        assert "giới hạn" in out.lower() or "limit" in out.lower() or "100" in out

    def test_null_displayed_as_null(self):
        result = {
            "columns": ["V"],
            "rows": [[None]],
            "row_count": 1,
            "execution_time_ms": 0,
            "truncated": False,
        }
        out = run_query_safe.format_text(result)
        assert "NULL" in out


class TestFormatJson:
    """[Test] format_json serializes result."""

    def test_round_trip(self):
        result = {
            "columns": ["a"],
            "rows": [[1]],
            "row_count": 1,
            "execution_time_ms": 5.0,
            "truncated": False,
        }
        out = run_query_safe.format_json(result)
        parsed = json.loads(out)
        assert parsed["columns"] == ["a"]
        assert parsed["row_count"] == 1


class TestFormatMarkdown:
    """[Test] format_markdown table and footer."""

    def test_markdown_table(self):
        result = {
            "columns": ["Col1", "Col2"],
            "rows": [["a", "b"]],
            "row_count": 1,
            "execution_time_ms": 2.0,
            "truncated": False,
        }
        out = run_query_safe.format_markdown(result)
        assert "| Col1 | Col2 |" in out
        assert "| a | b |" in out or "a" in out
        assert "1 rows" in out or "rows" in out


class TestRunQuery:
    """[Test] run_query: error path (non-SELECT raises)."""

    def test_raises_on_insert(self):
        with patch("run_query_safe.get_connection"):
            with patch("run_query_safe.get_db_type", return_value="oracle"):
                with pytest.raises(ValueError) as exc:
                    run_query_safe.run_query("INSERT INTO t VALUES (1)", db_alias="DWH")
                assert "SELECT" in str(exc.value) or "chặn" in str(exc.value)

    def test_raises_on_delete(self):
        with patch("run_query_safe.get_connection"):
            with pytest.raises(ValueError):
                run_query_safe.run_query("DELETE FROM t", db_alias="DWH")
