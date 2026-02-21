"""
Unit tests for explain_query.py.
Test _analyze_oracle_plan (pure function) for issue detection.
"""
import pytest

import explain_query


class TestAnalyzeOraclePlan:
    """[Test] _analyze_oracle_plan detects full scan, cartesian, cost, etc."""

    def test_detects_full_table_scan(self):
        plan_lines = [
            "| Id  | Operation         | Name |",
            "|   0 | SELECT STATEMENT |      |",
            "|   1 |  TABLE ACCESS FULL| MY_TABLE |",
        ]
        issues = explain_query._analyze_oracle_plan(plan_lines)
        full_scan = [i for i in issues if i["type"] == "FULL_TABLE_SCAN"]
        assert len(full_scan) >= 1
        assert "MY_TABLE" in full_scan[0]["message"] or "Full Table Scan" in full_scan[0]["message"]

    def test_detects_cartesian_product(self):
        plan_lines = ["MERGE JOIN CARTESIAN"]
        issues = explain_query._analyze_oracle_plan(plan_lines)
        cart = [i for i in issues if i["type"] == "CARTESIAN_PRODUCT"]
        assert len(cart) == 1
        assert cart[0]["severity"] == "CRITICAL"

    def test_detects_hash_join_info(self):
        plan_lines = ["HASH JOIN"]
        issues = explain_query._analyze_oracle_plan(plan_lines)
        hj = [i for i in issues if i["type"] == "HASH_JOIN"]
        assert len(hj) == 1
        assert hj[0]["severity"] == "INFO"

    def test_detects_high_cost(self):
        plan_lines = [
            "| 1 |  | 200000 |",
            "| 2 |  | 10 |",
        ]
        issues = explain_query._analyze_oracle_plan(plan_lines)
        cost_issues = [i for i in issues if i["type"] == "HIGH_COST"]
        assert len(cost_issues) >= 1
        assert "200000" in cost_issues[0]["message"] or "cao" in cost_issues[0]["message"]

    def test_returns_ok_when_no_issues(self):
        plan_lines = ["| 1 | INDEX RANGE SCAN | PK_IDX |"]
        issues = explain_query._analyze_oracle_plan(plan_lines)
        ok_issues = [i for i in issues if i["type"] == "NO_ISSUES"]
        assert len(ok_issues) == 1
