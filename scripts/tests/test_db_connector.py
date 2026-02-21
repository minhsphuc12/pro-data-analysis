"""
Unit tests for db_connector.py.
Mock external deps (os.environ, DB drivers); test get_db_type, is_select_only,
_wrap_with_limit, get_param_style, list_available_connections.
"""
import pytest
from unittest.mock import patch, MagicMock

import db_connector


class TestGetDbType:
    """[Test] get_db_type: happy path and error cases."""

    def test_returns_oracle_when_dwh_type_not_set_legacy_fallback(self):
        with patch.dict("os.environ", {}, clear=False):
            # Legacy: DWH alias without TYPE defaults to oracle
            assert db_connector.get_db_type("DWH") == "oracle"

    def test_returns_lowercase_type_from_env(self):
        with patch.dict("os.environ", {"MYDB_TYPE": "PostgreSQL"}, clear=False):
            assert db_connector.get_db_type("MYDB") == "postgresql"

    def test_raises_when_type_not_set_for_non_dwh(self):
        with patch.dict("os.environ", {}, clear=False):
            with pytest.raises(ValueError) as exc:
                db_connector.get_db_type("SOURCE")
            assert "SOURCE_TYPE" in str(exc.value) and "not set" in str(exc.value)

    def test_raises_when_type_invalid(self):
        with patch.dict("os.environ", {"X_TYPE": "mongodb"}, clear=False):
            with pytest.raises(ValueError) as exc:
                db_connector.get_db_type("X")
            assert "not supported" in str(exc.value).lower() or "mongodb" in str(exc.value)

    @pytest.mark.parametrize("db_type", ["oracle", "mysql", "postgresql", "sqlserver"])
    def test_accepts_all_supported_types(self, db_type):
        with patch.dict("os.environ", {"A_TYPE": db_type}, clear=False):
            assert db_connector.get_db_type("A") == db_type


class TestIsSelectOnly:
    """[Test] is_select_only: allow SELECT/WITH, block DML/DDL."""

    def test_allows_select(self):
        assert db_connector.is_select_only("SELECT * FROM t") is True
        assert db_connector.is_select_only("  select id from foo  ") is True

    def test_allows_with(self):
        assert db_connector.is_select_only("WITH x AS (SELECT 1) SELECT * FROM x") is True

    def test_allows_explain(self):
        assert db_connector.is_select_only("EXPLAIN SELECT * FROM t") is True

    def test_blocks_insert_update_delete(self):
        assert db_connector.is_select_only("INSERT INTO t VALUES (1)") is False
        assert db_connector.is_select_only("UPDATE t SET x=1") is False
        assert db_connector.is_select_only("DELETE FROM t") is False

    def test_blocks_ddl(self):
        assert db_connector.is_select_only("DROP TABLE t") is False
        assert db_connector.is_select_only("ALTER TABLE t ADD c INT") is False
        assert db_connector.is_select_only("CREATE TABLE t (id INT)") is False
        assert db_connector.is_select_only("TRUNCATE TABLE t") is False

    def test_blocks_dangerous_keywords_anywhere(self):
        assert db_connector.is_select_only("SELECT * FROM t; DELETE FROM t") is False

    def test_ignores_line_comments(self):
        assert db_connector.is_select_only("-- hello\nSELECT 1") is True

    def test_ignores_block_comments(self):
        assert db_connector.is_select_only("/* comment */ SELECT 1") is True


class TestWrapWithLimit:
    """[Test] _wrap_with_limit for each DB dialect."""

    def test_oracle_rownum(self):
        sql = "SELECT * FROM my_table"
        out = db_connector._wrap_with_limit(sql, 10, "oracle")
        assert "ROWNUM <= 10" in out
        assert "SELECT * FROM (SELECT * FROM my_table)" in out or out.startswith("SELECT * FROM (")

    def test_mysql_limit(self):
        sql = "SELECT * FROM t"
        out = db_connector._wrap_with_limit(sql, 5, "mysql")
        assert "LIMIT 5" in out
        assert "_limited" in out

    def test_postgresql_limit(self):
        sql = "SELECT * FROM t"
        out = db_connector._wrap_with_limit(sql, 20, "postgresql")
        assert "LIMIT 20" in out

    def test_sqlserver_top(self):
        sql = "SELECT * FROM t"
        out = db_connector._wrap_with_limit(sql, 15, "sqlserver")
        assert "TOP 15" in out

    def test_strips_trailing_semicolon(self):
        out = db_connector._wrap_with_limit("SELECT 1;", 1, "mysql")
        assert ";" not in out.split("LIMIT")[0]


class TestGetParamStyle:
    """[Test] get_param_style returns correct placeholder per DB."""

    @pytest.mark.parametrize("db_type,expected", [
        ("oracle", ":param"),
        ("mysql", "%s"),
        ("postgresql", "%s"),
        ("sqlserver", "?"),
    ])
    def test_returns_expected_style(self, db_type, expected):
        assert db_connector.get_param_style(db_type) == expected


class TestListAvailableConnections:
    """[Test] list_available_connections from env."""

    def test_returns_sorted_list_of_configured_aliases(self):
        env = {
            "DWH_TYPE": "oracle",
            "DWH_DSN": "x",
            "DWH_USERNAME": "u",
            "DWH_PASSWORD": "p",
            "SOURCE_TYPE": "mysql",
            "SOURCE_HOST": "h",
            "SOURCE_USERNAME": "u",
            "SOURCE_PASSWORD": "p",
        }
        with patch.dict("os.environ", env, clear=False):
            conns = db_connector.list_available_connections()
            aliases = [c["alias"] for c in conns]
            assert "DWH" in aliases
            assert "SOURCE" in aliases
            assert conns == sorted(conns, key=lambda x: x["alias"])

    def test_skips_invalid_type_gracefully(self):
        env = {"BAD_TYPE": "invalid", "BAD_HOST": "h"}
        with patch.dict("os.environ", env, clear=False):
            conns = db_connector.list_available_connections()
            # BAD may be skipped because get_db_type raises
            assert isinstance(conns, list)
