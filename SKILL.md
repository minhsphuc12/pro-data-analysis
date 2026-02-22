---
name: pro-data-analyst
description: >
  Use for any data analytics task: answering business questions with SQL, finding relevant tables,
  understanding DWH schema, building reports/queries, optimizing query performance. Works with
  Oracle DWH, MySQL, PostgreSQL, SQL Server. Follows a structured 7-phase workflow from requirement 
  analysis through data discovery, query design, testing, optimization, to documented output.
license: MIT
metadata:
  version: "2.4.0"
  domain: data-analytics
  triggers: >
    data analysis, SQL query, business report, find tables, DWH query, data warehouse,
    column meaning, table search, query optimization, EXPLAIN plan, revenue report,
    customer analysis, join tables, data discovery, ETL, star schema, KPI, metrics,
    Oracle query, SQL Server query, database question, data exploration, aggregate data
  role: analyst
  scope: end-to-end
  output-format: sql + documentation
---

**Key differentiator**: You do NOT jump straight to writing SQL. You first understand the data
landscape by searching documentation, database comments, and prior queries before designing
any solution.

## When to Use This Skill

- Answering business questions that require querying databases
- Finding which tables and columns contain specific business data
- Building reports or analytical queries
- Optimizing slow-running queries
- Understanding DWH and sources database schema and data relationships
- Any task involving SQL against enterprise databases

## Human-in-the-Loop Checkpoints

The workflow has **4 mandatory checkpoints** (unless the user enables skip mode). At each checkpoint, STOP and wait for user confirmation before proceeding. Details on skip mode, partial skip, and checkpoint rules: **`references/checkpoints.md`**.

## 7-Phase Workflow

**Bắt buộc thực hiện đúng thứ tự 7 phase. Không bỏ phase.**

| Phase | Mô tả ngắn | Chi tiết |
|-------|------------|----------|
| 1 | Requirement analysis & goal setting — brief, business glossary | `references/workflow-phase-1.md` |
| 2 | Data discovery — docs, schema, prior queries, deep inspection | `references/workflow-phase-2.md` |
| 3 | Data mapping & documentation | `references/workflow-phase-3.md` |
| 4 | Query design & reasoning (CTEs, PII, template) | `references/workflow-phase-4.md` |
| 5 | Query testing — EXPLAIN PLAN, safe execution | `references/workflow-phase-5.md` |
| 6 | Optimization (partition, index, hints) | `references/workflow-phase-6.md` |
| 7 | Save & document — query, report, knowledge distillation, security | `references/workflow-phase-7.md` |

After Phase 1 → **[CHECKPOINT 1]**; after Phase 2 → **[CHECKPOINT 2]**; after Phase 3 → **[CHECKPOINT 3]**; after Phase 4 → **[CHECKPOINT 4]**. The content for each checkpoint can be found in the corresponding workflow phase file. When working on phase N, load **`references/workflow-phase-N.md`** to get all steps and the checkpoint.

## Reference Guide

Load detailed guidance based on context:

| Topic | Reference | Load When |
|-------|-----------|-----------|
| Checkpoints & skip mode | `references/checkpoints.md` | Checkpoint rules, skip mode, partial skip |
| Workflow Phase 1–7 + checkpoints | `references/workflow-phase-N.md` | Executing Phase N (N = 1..7) |
| Query Patterns | `references/query-patterns.md` | JOINs, CTEs, subqueries, recursive queries |
| Window Functions | `references/window-functions.md` | ROW_NUMBER, RANK, LAG/LEAD, analytics |
| Optimization | `references/optimization.md` | EXPLAIN plans, indexes, statistics, tuning |
| Database Design | `references/database-design.md` | Normalization, keys, constraints, schemas |
| Dialect Differences | `references/dialect-differences.md` | Oracle vs MySQL vs PostgreSQL specifics |
| DWH Patterns | `references/dwh-patterns.md` | Star schema, SCD, ETL, fact/dimension patterns |
| Table/join knowledge | `single-table/{db}_{schema}_{table}.md`, `multiple-tables/{db}_{t1}_{t2}….md` | Before querying a table or join, check if a knowledge file exists and load it for context |

## Scripts Reference

| Script | Purpose | Key Usage |
|--------|---------|-----------|
| `@scripts/check_table.py` | Inspect table structure + comments | `python @scripts/check_table.py SCHEMA TABLE --db DWH` |
| `@scripts/search_schema.py` | Search DB metadata by name/comment | `python @scripts/search_schema.py -k "keyword" --db DWH` |
| `@scripts/search_documents.py` | Search Excel docs (DWH + source meta) | `python @scripts/search_documents.py -k "keyword" --folder documents/` |
| `@scripts/search_glossary.py` | Search business glossary (terms, definitions, DWH mapping) | `python @scripts/search_glossary.py -k "keyword" --folder documents/` |
| `@scripts/explain_query.py` | Run EXPLAIN PLAN | `python @scripts/explain_query.py --db DWH --file q.sql` |
| `@scripts/run_query_safe.py` | Execute with safety limits | `python @scripts/run_query_safe.py --db DWH --file q.sql` |
| `@scripts/find_relationships.py` | Find FK / join paths | `python @scripts/find_relationships.py -s SCHEMA -t TABLE` |
| `@scripts/sample_data.py` | Sample data + profiling | `python @scripts/sample_data.py -s SCHEMA -t TABLE --profile` |

## Database Connections

Connections configured via environment variables:
```
{ALIAS}_TYPE=oracle|mysql|postgresql|sqlserver
{ALIAS}_USERNAME=...
{ALIAS}_PASSWORD=...
{ALIAS}_DSN=... (Oracle) or {ALIAS}_HOST=... + {ALIAS}_PORT=... + {ALIAS}_DATABASE=... (MySQL/PostgreSQL/SQL Server)
{ALIAS}_DRIVER=... (SQL Server only, optional, default: {ODBC Driver 17 for SQL Server})
```
Default alias: `DWH` (Oracle datawarehouse)

**SQL Server specific:**
- Port default: 1433
- Requires pyodbc package: `pip install pyodbc`
- DRIVER can be customized via {ALIAS}_DRIVER environment variable

## Folder Structure

**Quy ước đặt tên theo task:** Mọi file hoặc thư mục tạo theo task-name **bắt buộc** dùng tiền tố ngày `{YYYY-MM-DD}_` để dễ sắp xếp và tìm (ví dụ: `2025-02-22_revenue-by-region/`, `2025-02-22_revenue-by-region-brief.md`).

```
documents/      -> Excel metadata (DWH + source systems). Chuẩn khai thác:
  dwh-meta-tables.xlsx, dwh-meta-columns.xlsx  -> DWH (bảng/cột tích hợp)
  [source]-meta-tables.xlsx, [source]-meta-columns.xlsx -> Từ điển từng hệ thống nguồn (source-a, source-b, ...)
  *glossary*.xlsx, *bg-*.xlsx (business glossary) -> Thuật ngữ, định nghĩa, cách tính, Bảng/Trường DWH, SQL tính toán (dùng trong Phase 1)
queries/        -> Existing SQL queries (reference for patterns)
queries/agent-written/  -> Output: queries written by this agent (naming: {YYYY-MM-DD}_{task-name}.sql)
references/     -> SQL and DWH reference guides
scripts/        -> Python tools for database inspection and query testing
single-table/   -> Knowledge base: one file per table. Naming: {source_db}_{schema}_{table}.md. Accumulate learnings across sessions.
multiple-tables/ -> Knowledge base: one file per set of joined tables. Naming: {source_db}_{table1}_{table2}[_{table3}…].md. Accumulate learnings across sessions.
```

## Constraints

### MUST DO
- Follow the 7-phase workflow in order
- **STOP at every [CHECKPOINT] and wait for user confirmation before proceeding**
- Present clear summaries at each checkpoint with structured questions
- Incorporate user feedback / domain knowledge when provided at checkpoints
- Re-present and re-confirm if user corrections are significant
- Create task brief before data discovery (Phase 1)
- Use date prefix **{YYYY-MM-DD}_** for any file or folder named with task-name (e.g. `{YYYY-MM-DD}_{task-name}/`, `{YYYY-MM-DD}_{task-name}-brief.md`) so outputs are easy to sort and find
- In Phase 1, consult business glossary for key terms/KPIs to enrich the brief (definitions, calculation, DWH candidates) before Phase 2
- Search BOTH documents/ (DWH + source docs when relevant) AND database metadata for data discovery (Phase 2)
- Document data mapping before writing query (Phase 3)
- Write CTEs with inline comments explaining reasoning (Phase 4)
- Run EXPLAIN PLAN before executing query (Phase 5)
- Wrap test execution with safety limits (Phase 5)
- Save output query to `queries/agent-written/` with header comment (Phase 7)
- When the job finishes successfully, distill session learnings into knowledge-base files in `single-table/` and/or `multiple-tables/` (one file per object; if file exists, read then merge/append with date and task context); never include real data samples, PII, internal identifiers, or confidential business data in those files (see Phase 7 Security — knowledge base content)
- Handle NULLs explicitly in all comparisons
- Apply partition pruning on partitioned tables
- Use column comments to understand business meaning of data
- **PII in queries**: Before executing any SQL, ensure no column that is or may be PII (e.g. name, email, phone, ID number, address; or columns marked CDE/PII in metadata) appears in the SELECT list as a direct column. If analytics need PII, use only aggregations (e.g. `COUNT(*)`, `COUNT(DISTINCT col)`).

### MUST NOT DO
- **Proceed past any [CHECKPOINT] without explicit user approval**
- **Assume tables/columns/filters are correct without user confirmation**
- **Ignore user-provided domain knowledge or corrections at checkpoints**
- **Execute SQL that selects columns that are or may be PII** (e.g. name, email, phone, national ID, address, or columns marked CDE/PII in metadata) **as direct result columns**. PII may appear only inside aggregation functions (e.g. `COUNT(email)`, `COUNT(DISTINCT customer_id)`); raw PII must not be returned in the result set.
- Jump straight to writing SQL without understanding data first
- Skip EXPLAIN PLAN analysis
- Using logic DWH when user explicitly ask not to
- Execute queries without row limits during testing
- Run INSERT/UPDATE/DELETE/DDL through the test scripts
- Use SELECT * in final queries
- Ignore column comments when they exist
- Leave queries undocumented
- Hardcode database credentials in scripts
- Skip data quality checks (nulls, edge cases)
- Write queries without considering data volume

## Knowledge Reference

Star schema, fact tables, dimension tables, SCD types, CTEs, window functions,
recursive queries, EXPLAIN/ANALYZE, covering indexes, query hints, partitioning,
materialized views, OLAP patterns, slowly changing dimensions, ETL patterns,
partition pruning, parallel execution, Oracle hints, data profiling
