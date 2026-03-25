"""
Unit tests for search_views.py.
Uses mocked DB cursor/connection to validate parsing and formatting logic.
"""

import pytest
from unittest.mock import patch, MagicMock

import search_views as sv


class TestLobToStr:
    def test_none_returns_empty(self):
        assert sv._lob_to_str(None) == ""

    def test_string_returns_same(self):
        assert sv._lob_to_str("hello") == "hello"

    def test_object_with_read(self):
        class FakeLob:
            def read(self):
                return "abc"

        assert sv._lob_to_str(FakeLob()) == "abc"


class TestSearchViewsEntryPoint:
    def test_raises_value_error_for_non_oracle(self):
        with patch.object(sv, "get_db_type", return_value="postgresql"):
            with pytest.raises(ValueError) as exc:
                sv.search_views(view_name="VW_X", db_alias="X")
            assert "Oracle" in str(exc.value)

    def test_oracle_fetch_by_name_path(self):
        mock_cursor = MagicMock()
        mock_cursor.execute = MagicMock()
        mock_cursor.fetchone = MagicMock(return_value=None)
        mock_cursor.__iter__ = lambda self: iter(
            [
                ("OWNER1", "VW_FOO", "SELECT * FROM T1"),
            ]
        )

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch.object(sv, "get_db_type", return_value="oracle"), patch.object(sv, "get_connection", return_value=mock_conn):
            results = sv.search_views(object_name="OWNER1.VW_FOO", db_alias="DWH")

        assert len(results) == 1
        assert results[0]["schema"] == "OWNER1"
        assert results[0]["name"] == "VW_FOO"
        assert results[0]["type"] == "VIEW"
        assert "SELECT * FROM T1" in results[0]["text"]


class TestOracleFetchViewByName:
    def test_fetch_groups_rows(self):
        cursor = MagicMock()
        cursor.execute = MagicMock()
        cursor.__iter__ = lambda self: iter(
            [
                ("S1", "V1", "SELECT 1 FROM DUAL"),
                ("S2", "V1", "SELECT 2 FROM DUAL"),
            ]
        )
        results = sv._oracle_fetch_view_by_name(cursor, view_name="V1", owner=None, show_query=False)
        assert len(results) == 2
        assert results[0]["schema"] == "S1"
        assert results[1]["schema"] == "S2"
        assert results[0]["text"].startswith("SELECT 1")


class TestFormatters:
    def test_format_text_includes_definition(self):
        results = [{"schema": "S", "name": "V", "type": "VIEW", "text": "SELECT 1 FROM DUAL"}]
        out = sv.format_text(results)
        assert "Found 1 view(s)" in out
        assert "S.V" in out
        assert "SELECT 1 FROM DUAL" in out

