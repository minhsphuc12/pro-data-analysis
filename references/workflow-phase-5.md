# Phase 5: Query Testing (Unit Tests)

Test in two stages:

## 5a. EXPLAIN PLAN (always first)

```bash
python @scripts/explain_query.py --db DWH --file query.sql
```

Check for:
- [ ] No full table scans on large fact tables
- [ ] Partition pruning is happening
- [ ] Index usage on join columns
- [ ] No cartesian products
- [ ] Reasonable cost estimate

## 5b. Safe Execution (after EXPLAIN passes)

```bash
python @scripts/run_query_safe.py --db DWH --file query.sql --limit 100 --timeout 30
```

Verify:
- [ ] Results make business sense (spot-check values)
- [ ] Column names and types are correct
- [ ] No unexpected NULLs
- [ ] Row count is in expected range

```bash
# Check total row count
python @scripts/run_query_safe.py --db DWH --file query.sql --count-only
```

If issues found, iterate back to Phase 4.
