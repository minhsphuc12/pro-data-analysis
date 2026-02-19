# Phase 6: Optimization

Based on EXPLAIN PLAN analysis:
1. **Partition pruning**: Ensure partition key is in WHERE clause
2. **Index awareness**: Filter and JOIN on indexed columns
3. **Join order**: Fact table scanned once, dimensions lookup
4. **Avoid repeated scans**: Use CTEs or temp results
5. **Oracle hints** (if needed): `/*+ PARALLEL(t,4) */`, `/*+ INDEX(t idx_name) */`
6. Re-run EXPLAIN PLAN and safe execution after changes

Load optimization reference: `references/optimization.md`
