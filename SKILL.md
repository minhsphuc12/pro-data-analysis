---
name: pro-data-analysis
description: >
  Use for any data analytics task: answering business questions with SQL, finding relevant tables,
  understanding DWH schema, building reports/queries, optimizing query performance. Works with
  Oracle DWH, MySQL, PostgreSQL, SQL Server. Follows a structured 7-phase workflow from requirement 
  analysis through data discovery, query design, testing, optimization, to documented output.
license: MIT
metadata:
  version: "2.5.0"
  domain: data-analytics
  triggers: >
    data analysis, SQL query, business report, find tables, DWH query, data warehouse,
    column meaning, table search, query optimization, EXPLAIN plan, revenue report,
    customer analysis, join tables, data discovery, ETL, star schema, KPI, metrics,
    Oracle query, SQL Server query, database question, data exploration, aggregate data,
    data lineage, table lineage, column lineage, where is table used, back to source, ODI scenario
  role: analyst
  scope: end-to-end
  output-format: sql + documentation
---

**Key differentiator**: You do NOT jump straight to writing SQL. Before executing a new task,
**consult the knowledge folders** (`knowledge/`) for accumulated data understanding and business understanding from previous tasks — including **business terms** in `knowledge/glossary/` (one file per term: business insight, usage, data understanding) and table/join learnings in `single-table/` and `multiple-tables/` — then understand the data landscape by searching documentation, database comments, and prior queries before designing any solution.

## When to Use This Skill

- Answering business questions that require querying databases
- Finding which tables and columns contain specific business data
- Building reports or analytical queries
- Optimizing slow-running queries
- Understanding DWH and sources database schema and data relationships
- Tracing table/column lineage (where a table or column is used as input or output in packages/procedures, within DWH or back to source; iterate between `scripts/search_procedures.py` and ODI scenario files in `scenarios/` over multiple rounds to trace as far upstream/downstream as possible)
- Any task involving SQL against enterprise databases

## Human-in-the-Loop Checkpoints

**This workflow includes mandatory confirmation checkpoints by default.** At each checkpoint, the agent MUST stop and wait for explicit user approval before proceeding. This prevents wasted time from wrong table choices, incorrect filters, or misunderstood requirements.

### Skip Mode

Checkpoints can be skipped when the user explicitly opts out. Look for signals like:
- "skip checkpoints", "no checkpoints", "no need to confirm", "just do it"
- "I'm busy", "busy", "no need to ask", "run straight through", "auto mode"
- "skip CP", "fast mode", "no stops"

**When skip mode is activated:**
- Run the full workflow end-to-end without stopping at checkpoints
- Still **produce the same summaries** (brief, table list, data mapping, query logic) inline in your output so the user can review afterward — just don't wait for a response
- The user can re-enable checkpoints anytime by saying "enable checkpoints" or similar

**Partial skip:** The user may also skip individual checkpoints (e.g., "skip checkpoint 1" or "I trust the tables, skip CP2") — honor the specific request and keep the rest active.

### Checkpoint rules (when active)

- Use structured questions (multiple-choice + free-text option) whenever possible
- Present findings clearly in a summary before asking for confirmation
- NEVER proceed past a checkpoint without user response
- If user says "no" or provides corrections, incorporate feedback and re-present
- If user provides additional domain knowledge, capture it in the task brief / data mapping
- Checkpoints are labeled **[CHECKPOINT N]** — there are 4 total

### Subagent usage (optional)

Some steps can be **delegated to a subagent** (e.g. via `mcp_task`) so the main agent keeps a lighter context. Use only when the user asks for it or when the step is heavy and self-contained.

**When to delegate:**
- **Phase 2a–2c** (search docs, schema, prior queries): subagent returns a table/column summary. Use `explore` or `generalPurpose`.
- **Phase 2d** (deep inspection: check_table, sample_data, find_relationships): optional; subagent returns structure + sample.
- **Phase 2e** (lineage: search_procedures + scenarios, multiple rounds): subagent returns lineage summary. Use `generalPurpose`.
- **Phase 5** (EXPLAIN + run_query_safe): subagent returns plan summary + sample rows. Use `shell` or `generalPurpose`.

**User triggers:** e.g. "run discovery in subagent", "lineage via subagent", "EXPLAIN and test in subagent", or "skip checkpoints and use subagent for Phase 2/5".

**Contract:** Subagent receives task brief (or excerpt), keyword/schema/table, db alias, and paths to scripts/documents/scenarios. It returns a markdown file or summary (tables/columns, lineage, or EXPLAIN + sample). The main agent reads that result and continues with the next phase or checkpoint; all checkpoints remain with the main agent.

**References:** The subagent runs in isolated context, so the prompt must be self-contained (include script paths and db alias).

## 7-Phase Workflow

**It is mandatory to execute all 7 phases in the correct order. Do not skip any phase.**

| Phase | Short Description | Details |
|-------|-------------------|---------|
| 1 | Requirement analysis & goal setting — brief, business glossary | [Phase 1](#phase-1-requirement-analysis--goal-setting) (see below) |
| 2 | Data discovery — docs, schema, prior queries, deep inspection | [Phase 2](#phase-2-data-discovery) (see below) |
| 3 | Data mapping & documentation | [Phase 3](#phase-3-data-mapping--documentation) (see below) |
| 4 | Query design & reasoning (CTEs, PII, template) | [Phase 4](#phase-4-query-design--reasoning) (see below) |
| 5 | Query testing — EXPLAIN PLAN, safe execution, CTE isolation when abnormal | [Phase 5](#phase-5-query-testing-unit-tests) (see below) |
| 6 | Optimization (partition, index, hints) | [Phase 6](#phase-6-optimization) (see below) |
| 7 | Save & document — query, report, knowledge distillation, security | [Phase 7](#phase-7-save--document) (see below) |

After Phase 1 → **[CHECKPOINT 1]**; after Phase 2 → **[CHECKPOINT 2]**; after Phase 3 → **[CHECKPOINT 3]**; after Phase 4 → **[CHECKPOINT 4]**. All 7 phases and 4 checkpoints are described directly below in SKILL.md.

### Phase 1: Requirement Analysis & Goal Setting

Before touching any data, clearly define the problem and **enrich it with business glossary** so that Phase 2 data discovery is targeted and aligned with standard definitions.

1. **Business question**: What is the user really asking? Restate it precisely.
2. **Create task name**: Create single {task-name} for further usage, create folder {task-folder} = {YYYY-MM-DD}_{task-name}
3. **Output definition**: What should the result look like? (columns, rows, format)
4. **Data modules**: Break into logical data domains (e.g., "customer data", "transaction data")
5. **Data elements needed**: List specific measures (SUM, COUNT, AVG) and dimensions (GROUP BY)
6. **Filters & scope**: Time range, business unit, status filters, etc.
7. **Consult Business Glossary** (before finalizing the brief):  
   Search the business glossary for **key terms and KPIs** mentioned in the question (indicator name, terminology). This clarifies definitions, calculation methods, and points to DWH tables/columns *before* you search docs/schema. To search several terms in one call, use regex OR: `--keyword "term1|term2|KPI name"` (regex is on by default).
   ```bash
   python @scripts/search_glossary.py --keyword "your term or KPI" --folder documents/
   python @scripts/search_glossary.py --keyword "doanh thu|revenue|AMOUNT" --folder documents/
   ```
   Use glossary results to:
   - **Definition**: Standard definition of the term — include in the brief so the problem is unambiguous.
   - **Calculation method**: Use this to validate or draft the expected logic.
   - **DWH table/column, calculation SQL**: Suggested DWH table/column and SQL snippet — note these as candidates for Phase 2 (do not assume they are the only source; still run data discovery and confirm).
   - **Domain, Owning Unit**: Helps with scoping and identifying ownership.
   Enrich the task brief with these so the problem is stated in business language and Phase 2 discovery is more effective.
8. **Consult accumulated knowledge glossary**:  
   For the same key terms and KPIs, check the **accumulated knowledge glossary** in `knowledge/glossary/`. There is **one file per term** (e.g. `{term-slug}.md`). Each file holds learned insight about that term: business context, usage in reports, how the data is understood in practice, gotchas, and references to tables/columns from past tasks. If a file exists for a term, **read it** and merge relevant points into the task brief so the brief benefits from prior sessions. This step runs after the formal business glossary so definitions stay aligned with standards while the brief is enriched with accumulated experience.
9. **Save brief**: Create folder `{task-folder}/` and file `{task-folder}-brief.md` inside it (e.g. `2025-02-22_revenue-by-region/2025-02-22_revenue-by-region-brief.md`). Always use date prefix **{YYYY-MM-DD}_** for task-related files and folders so they are naturally sorted and navigated easily.

**Template for brief:**

```markdown
# Task: {task name}
## Business Question
{restate the question}
## Glossary / Standard Definitions (from business glossary and knowledge/glossary/, if found)
- **{term 1}**: {Definition}. Calculation method: {Calculation method}. DWH: {Table/Field} (candidate for Phase 2).
- ...
## Expected Output
| Column | Description | Source |
|--------|-------------|--------|
## Data Modules
- Module 1: ...
- Module 2: ...
## Filters & Scope
- Time range: ...
- Other filters: ...
## Assumptions
- ...
```

**[CHECKPOINT 1] — Confirm Requirements Understanding**

**STOP and present the task brief to the user.** Ask them to confirm:

1. **Is the business question correctly understood?**
   - Yes, proceed
   - No, here's what I actually mean: (free-text)

2. **Are the expected output columns correct?**
   - Yes, these columns are what I need
   - I need additional columns: (free-text)
   - Remove some columns: (free-text)

3. **Are the filters and scope correct?**
   - Yes
   - No, adjust the filters: (free-text)

4. **Any domain knowledge to share?** (free-text, optional)
   - Known table names, column names, business rules, or gotchas that would help

5. **Where should the data be extracted from?**
   - Is the data source exclusively DWH, source databases, or do we need both? (If unclear, please specify.)

**Format:** Present a clear summary of Phase 1 output, then ask the questions above.
If the user provides corrections, update the brief and re-confirm if changes are significant.
Only proceed to Phase 2 after explicit approval.

### Phase 2: Data Discovery

Search **three sources in parallel** to find relevant tables and columns.

**Searching multiple terms at once:** All search scripts (`search_glossary`, `search_documents`, `search_schema`) use **regex by default**. To find any of several terms in one run, use `|` (OR): e.g. `--keyword "doanh thu|revenue|amount"`. Use `--no-regex` only when you need a single literal substring match.

**2a. Search Excel Documentation**

```bash
python @scripts/search_documents.py --keyword "your keyword" --folder documents/
python @scripts/search_documents.py --keyword "TERM_X|TERM_Y|TERM_Z" --folder documents/
```

The `documents/` folder contains **two types** of standardized Excel metadata; search here first and use the right type for the task:

- **DWH metadata** (data warehouse — consolidated from all sources):
  - `dwh-meta-tables.xlsx`: DWH tables (Table Name, Table Description, Schema, Source, Domain, DIM/FACT/RPT classification, …)
  - `dwh-meta-columns.xlsx`: DWH columns (Table Name, Field Name, Description, Data Type, Mapping Rule, CDE/PII, …)
  Use when the question is about **tables/columns in the DWH** (reporting, KPI, join in DWH).

- **Source-system metadata** (individual source systems):
  - `[source]-meta-tables.xlsx`: Table Name, Description, Care, Type (e.g. `sourceA-meta-tables.xlsx`)
  - `[source]-meta-columns.xlsx`: Column Name, Data Type, Comment, Sample Data, Table Name
  Use when the question involves **data from a source system** (ETL mapping, data lineage, source dictionary).

Search results will contain `doc_type` (dwh_tables, dwh_columns, source_tables, source_columns) and `source_name` (for source files). Prioritize DWH docs for reporting/analytics questions; use source docs for data lineage or ETL mapping as needed.

**2b. Search Database Schema Metadata**

```bash
python @scripts/search_schema.py --keyword "your keyword" --db DWH
python @scripts/search_schema.py --keyword "LOAN_SCH|LOAN_DTL|customer" --db DWH
python @scripts/search_schema.py --keyword "your keyword" --search-in comments --schema OWNER
```

Most database (excel DWH) columns have **comments** that describe their business meaning. This is critical for understanding what data is available. Search both column names AND comments.

**2c. Search Previous Queries**

Look in the `queries/` folder for similar prior queries that might reveal useful patterns, table names, or join conditions.

**2d. Deep Inspection (after finding candidates)**

```bash
# Inspect table structure + column comments
python @scripts/check_table.py SCHEMA TABLE_NAME --db DWH

# Check sample data to understand content
python @scripts/sample_data.py --schema SCHEMA --table TABLE_NAME --db DWH

# Find FK relationships and join paths
python @scripts/find_relationships.py --schema SCHEMA --table TABLE_NAME --db DWH
python @scripts/find_relationships.py --schema SCHEMA --tables TABLE1,TABLE2 --db DWH

# Data profiling for key columns
python @scripts/sample_data.py --schema SCHEMA --table TABLE_NAME --db DWH --profile
```

**2e. Table/Column Lineage (when tracing lineage)**

When the user needs to find **where a table or column is used as input or output** in packages/procedures, or to trace **lineage within DWH or back to source database**, combine both sources below and **iterate** so lineage can be traced as far upstream or downstream as possible.

- **DWH procedures/packages (Oracle):** Use `scripts/search_procedures.py` with db alias **DWH_ADMIN** (not DWH) to find PROCEDURE, PACKAGE, PACKAGE BODY, or FUNCTION that reference a table name (or a text pattern) in source code. This shows which objects read from or write to the table.
- **ODI scenarios:** The folder `scenarios/` contains **extracted Oracle Data Integrator (ODI) scenario information**. Use it to understand ETL flows, source-to-DWH mappings, and scenario-level lineage. Search or scan files in `scenarios/` by table name, interface name, or scenario name.

**Iterative lineage expansion (do not stop after one round):** To trace lineage as far as possible, **alternate** between reading scenario files and searching procedures over **multiple rounds** until no new references appear or a reasonable limit (e.g. 5–10 rounds) is reached.

1. **Start:** From the task (or checkpoint), collect initial table names, interface names, or scenario names to trace.
2. **Round N (repeat until no new names or limit reached):**
   - **Search procedures:** Run `search_procedures.py` for the current set of table/object names. From the results, note any **new** table names, package names, or procedure names that appear as inputs/outputs.
   - **Read scenario files:** In `scenarios/`, search or open files that mention the current tables/interfaces/scenarios. From file content, extract any **new** table names, interface names, scenario names, or source mappings.
   - **Merge:** Add newly discovered names to the working set (avoid duplicates). If the working set grew, continue to the next round; otherwise stop.
3. **Document:** Merge all rounds into one lineage view: upstream sources, downstream consumers, procedures/packages, and ODI scenario/mapping context.

```bash
# Find procedures/packages that reference a table (input/output usage)
python @scripts/search_procedures.py --table TABLE_NAME --db DWH_ADMIN
python @scripts/search_procedures.py --table FACT_ORDER --schema ADMIN_SCHEMA --db DWH_ADMIN

# Search by text pattern (e.g. INSERT INTO, table prefix)
python @scripts/search_procedures.py --text "INSERT INTO" --table DIM_CUSTOMER --db DWH_ADMIN

# Fetch full source of a known object
python @scripts/search_procedures.py --name ADMIN_SCHEMA.PKG_TABLE_LOAD --db DWH_ADMIN

# Multiple table name patterns (regex) — use for each new batch of table names found in previous round
python @scripts/search_procedures.py --table "FACT_|DIM_" --regex --db DWH_ADMIN --limit 50
```

After each procedure search, **consult `scenarios/`** for the tables/interfaces just found; after each scenario read, **run search_procedures** for any new table or object names found in the scenario files. If the task brief or user asks for "lineage", "where is this table used", "source of this column", or "back to source", **always run 2e** and **apply the iterative expansion** above as part of Phase 2.

**[CHECKPOINT 2] — Confirm Table & Column Selection**

**STOP after data discovery and present findings to the user.** Before creating the data mapping document, the user MUST confirm the discovered tables/columns are correct.

**Present a summary table like this:**

```
Found Tables:
| # | Schema.Table        | Description            | Why selected             |
|---|---------------------|------------------------|--------------------------|
| 1 | DWH.FACT_SALES      | Daily sales fact table | Contains revenue metrics |
| 2 | DWH.DIM_CUSTOMER    | Customer dimension     | Customer attributes      |
| 3 | DWH.DIM_PRODUCT     | Product dimension      | Product categorization   |

Key Columns Identified:
| Table           | Column          | Comment/Meaning          | Role in Query |
|-----------------|-----------------|--------------------------|---------------|
| FACT_SALES      | AMOUNT          | Net sales amount (VND)   | Measure (SUM) |
| FACT_SALES      | SALE_DATE       | Transaction date         | Filter, Group |
| DIM_CUSTOMER    | CUSTOMER_TYPE   | B2B / B2C classification | Filter        |
```

**Then ask:**

1. **Are these the right tables?**
   - Yes, use all of them
   - Remove some (specify which)
   - I know other tables that should be included: (free-text)

2. **Are the key columns correct?**
   - Yes
   - Add columns: (free-text)
   - Some columns are wrong: (free-text explanation)

3. **Any column meaning corrections?** (free-text, optional)
   - Example: "AMOUNT is actually gross amount, not net" or "CUSTOMER_TYPE values are 'CORP' and 'RETAIL', not 'B2B' and 'B2C'"

4. **Do you know the join conditions?** (free-text, optional)
   - Example: "Join FACT_SALES to DIM_CUSTOMER on CUST_ID, but watch out for NULL CUST_ID on internal transfers"

**If the user provides corrections:** Run additional discovery searches if new tables/columns are mentioned; re-present updated findings and confirm again. Only proceed to Phase 3 after explicit approval.

### Phase 3: Data Mapping & Documentation

Create `{task-folder}/{task-folder}-data-mapping.md` in working directory (same dated task folder as Phase 1 brief), documenting everything found:

```markdown
# Data Mapping: {task name}

## Tables Used
| # | Schema.Table | Description | Est. Rows | Partitioned? |
|---|-------------|-------------|-----------|--------------|

## Column Mapping
| Table | Column | Comment/Meaning | Role in Query | Data Type |
|-------|--------|-----------------|---------------|-----------|

## Join Conditions
| From | To | Join Condition | Type |
|------|----|----------------|------|

## Filters & Business Rules
- ...

## Data Quality Notes
- Null patterns: ...
- Edge cases: ...
- Data volume considerations: ...

## Assumptions
- ...
```

**[CHECKPOINT 3] — Confirm Data Mapping & Business Rules**

**STOP after creating the data mapping document.** This is the last checkpoint before writing SQL. The user MUST verify the complete mapping is correct.

**Present the data mapping summary and ask:**

1. **Are the join conditions correct?**
   - Yes
   - No, the correct join is: (free-text)

2. **Are the filter / WHERE clause conditions correct?**
   - Yes
   - No, adjust: (free-text)
   - Example corrections: "Filter should be STATUS = 'ACTIVE', not STATUS != 'DELETED'" or "Date filter should use POSTING_DATE not TRANSACTION_DATE"

3. **Are the business rules / calculations correct?**
   - Yes
   - No, the correct formula is: (free-text)
   - Example: "Revenue = AMOUNT - DISCOUNT - TAX, not just AMOUNT"

4. **Any NULL handling or edge cases I should know about?** (free-text, optional)
   - Example: "DISCOUNT can be NULL, treat as 0" or "Exclude rows where CUST_ID = -1 (dummy customer)"

5. **Aggregation and grouping — does this look right?**
   - Yes
   - No, I need different grouping: (free-text)

**If the user provides corrections:** Update the data mapping document; re-present if changes are substantial. Only proceed to Phase 4 after explicit approval.

### Phase 4: Query Design & Reasoning

Write the query following these principles:
- Use **CTEs** to separate logical steps clearly
- Add **inline comments** explaining WHY each part exists
- Reference specific findings from Phase 3
- Apply **early filtering** (especially partition keys)
- Handle **NULLs** explicitly
- Use **set-based operations** (never cursors)
- **PII**: Do **not** put columns that are or may be personally identifiable information (PII) in the SELECT list as direct output columns. If PII is needed for analytics, use only **aggregation functions** (e.g. `COUNT(*)`, `COUNT(DISTINCT col)`, `MIN`/`MAX` for grouping). Use DWH/source metadata (e.g. CDE/PII in `dwh-meta-columns.xlsx`) to identify PII columns.

**Query structure template:**

```sql
/*
 * Purpose: {business question}
 * Author: AI Agent
 * Date: {date}
 * Tables: {list of tables}
 * Filters: {key filters}
 * Notes: {important notes}
 */

-- Step 1: {description of what this CTE does}
WITH step1 AS (
    SELECT ...
    FROM schema.table
    WHERE partition_key >= ...  -- Partition pruning
),

-- Step 2: {description}
step2 AS (
    SELECT ...
    FROM step1
    JOIN ...
)

-- Final output
SELECT ...
FROM step2
ORDER BY ...;
```
Save query to `{task-folder}/{task-folder}_query.sql`
 - Include standard header comment (purpose, author, date, tables, performance notes)

**[CHECKPOINT 4] — Confirm Query Logic Before Testing**

**STOP after writing the query.** Present the full SQL query and a plain-language explanation of what each CTE / step does.

**Present:**
```
Query Logic Summary:
1. CTE step1: Get all sales in date range from FACT_SALES (partition pruning on SALE_DATE)
2. CTE step2: Join with DIM_CUSTOMER to get customer type
3. Final SELECT: Aggregate by customer_type and month, calculate total revenue

Key decisions made:
- Used LEFT JOIN to DIM_CUSTOMER (to keep sales with missing customer)
- Filtered STATUS = 'COMPLETED' (excluded pending/cancelled)
- Revenue = SUM(AMOUNT - NVL(DISCOUNT, 0))
```

**Then ask:**

1. **Does the query logic look correct?**
   - Yes, proceed to test
   - No, adjust this part: (free-text)

2. **Any edge cases or special handling I should add?** (free-text, optional)

**If the user requests changes:** Modify the query and re-present the updated logic summary. Only proceed to Phase 5 after explicit approval.

### Phase 5: Query Testing (Unit Tests)

Test in stages (5a, 5b; use 5c when results are abnormal):

**5a. EXPLAIN PLAN (always first)**

```bash
python @scripts/explain_query.py --db DWH --file query.sql
```

Check for:
- [ ] No full table scans on large fact tables
- [ ] Partition pruning is happening
- [ ] Index usage on join columns
- [ ] No cartesian products
- [ ] Reasonable cost estimate

**5b. Safe Execution (after EXPLAIN passes)**

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

**5c. Debugging abnormal results (CTE isolation)**

When the query returns **abnormal results** (0 rows, important columns all NULL, or runs too long), isolate the cause by running CTEs step by step. **Use a subagent** to perform the verification: pass the query file, DB alias, and which scenario (empty/null vs. slow); the subagent runs the isolation steps and reports which CTE is at fault.

- **Empty result or important columns all NULL**  
  **Bottom-up:** Remove or bypass the **last** CTE layer first (the one feeding the final SELECT). Run the shortened query; if rows or non-null values appear, the removed CTE (or the step that feeds it) is the likely cause. Repeat by removing the next-to-last CTE, and so on, until the problem disappears — the last change that fixed it pinpoints the faulty CTE or join/filter.

- **Query runs too long (timeout or very slow)**  
  **Top-down:** Run from the **first** CTE downward. Execute `SELECT * FROM first_cte WHERE ROWNUM <= N` (or equivalent `LIMIT N`) and note the time; then add the second CTE, then the third, etc. The first step that becomes slow identifies the bottleneck (table scan, missing partition pruning, or heavy join).

After the subagent reports the offending CTE or step, fix the query (or data mapping) and re-run 5a/5b. If issues persist, iterate back to Phase 4 with the new finding.

### Phase 6: Optimization

Based on EXPLAIN PLAN analysis:
1. **Partition pruning**: Ensure partition key is in WHERE clause
2. **Index awareness**: Filter and JOIN on indexed columns
3. **Join order**: Fact table scanned once, dimensions lookup
4. **Avoid repeated scans**: Use CTEs or temp results
5. **Oracle hints** (if needed): `/*+ PARALLEL(t,4) */`, `/*+ INDEX(t idx_name) */`
6. Re-run EXPLAIN PLAN and safe execution after changes

Load optimization reference: `references/optimization.md`

### Phase 7: Save & Document

1. **Save query** to `queries/agent-written/{task-folder}.sql`
   - Include standard header comment (purpose, author, date, tables, performance notes)

2. **Update data mapping** document (`{task-folder}/{task-folder}-data-mapping.md`) with final results

3. **Session knowledge distillation** (when the job finishes successfully):  
   Distil what you learned in this session and persist it into the knowledge-base files below.  
   **One object = one file**: each table (single-table) or each set of joined tables (multiple-tables) has exactly one knowledge file. If a file for that object already exists, **read it first**, then **merge/append** the new learnings (with date and brief task context) into the same file so knowledge accumulates across tasks.

   - **Folder `knowledge/single-table/`**  
     Use when the task involved **one main table** (or you want to record what you learned about one table).  
     Content to capture: actual state of data in the table, which columns are important, usage, purpose, how to interpret values correctly, null/edge cases, sample value patterns, and any caveats.  
     **When table/column lineage was traced and confirmed** (Phase 2e): retain lineage knowledge in the same single-table file(s). Record which procedures/packages use the table or column as input or output, and any ODI scenario/mapping context from `scenarios/` (upstream sources, downstream consumers, ETL steps). One file per table: `{source_db}_{schema}_{table}.md`; if the file exists, read it then merge/append the lineage section with date and task context.  
     **File name**: `{source_db}_{schema}_{table}.md`  
     - `source_db`: connection alias (e.g. `DWH`, `SOURCE_A`, `SOURCE_B`).  
     - `schema`: schema/owner of the table.  
     - `table`: table name.  
     Example: `DWH_DWH_FACT_SALES.md`, `SOURCE_A_SOURCE_A_ACCOUNTS.md`.

   - **Folder `knowledge/multiple-tables/`**  
     Use when the task required **joining two or more tables**.  
     Content to capture: how and why these tables are connected, join keys and conditions, usage and purpose of the combination, correct interpretation of the result set, typical filters, and gotchas (e.g. duplicates, nulls on join keys).  
     **File name**: `{source_db}_{table1}_{table2}[_{table3}…].md`  
     - Use the same `source_db` for the object; list tables in a consistent order (e.g. fact first, then dimensions).  
     Example: `DWH_FACT_SALES_DIM_CUSTOMER.md`, `DWH_FACT_SALES_DIM_CUSTOMER_DIM_PRODUCT.md`.

   **Format inside each file** (Markdown): use clear headings (e.g. Purpose, Important columns, Join conditions, Data quality, Session notes) and append new session notes with a date and short task reference so the file remains one cumulative knowledge base per object.

   - **Folder `knowledge/glossary/`**  
     Use when the task involved **business terms or KPIs** that you now understand better (definition, calculation, usage, or how they map to tables/columns).  
     **One term = one file.** File name: `{term-slug}.md` (e.g. `net-revenue.md`, `active-customer.md`).  
     Content to capture: business insight, how the term is used in reports, data understanding (which tables/columns, filters, edge cases), and gotchas. If a file for that term already exists, **read it first**, then **merge/append** the new learnings with date and task context. Same security rules as above: no real data samples, PII, or confidential detail — only structure, semantics, and generic placeholders.

   **Security — knowledge base content:**  
   Do **NOT** write any of the following into output files (to avoid leaking sensitive company information, especially if these folders are ever committed or shared):

   - **Real data samples or row-level values** from production (e.g. actual IDs, names, amounts, dates that identify real entities). Use only **generic placeholders or value types** (e.g. "values like 'ACTIVE'/'PENDING'", "numeric ID", "date range").
   - **PII or confidential business data**: no real customer names, emails, phone numbers, account numbers, contract values, or internal codes that could identify people or deals.
   - **Internal-only identifiers**: no real hostnames, instance names, schema names that reveal company infrastructure; prefer generic aliases (e.g. DWH, SOURCE_A) or role-based names.
   - **Proprietary business rules or KPIs** that are strictly confidential (if in doubt, describe logic in abstract terms only, without numbers or formulas that are company secret).
   - **Connection strings, credentials, or environment details** (no DSN, host, database name beyond the generic alias used in the skill).

   When describing "sample value patterns" or "data quality", stick to **structure and semantics** (e.g. "column allows NULL; distinct values observed: status-like codes") rather than dumping real values. If the task revealed sensitive context, record only **what is needed for future queries** (e.g. join key names, filter column names, data types) and omit the sensitive detail.

4. **Report to user** with a task report name `{task-folder}_README.md` in task folder:
   - Use 7 phases structure 
   - Summarize outcome and generated files of every phases done by this skill 
   - Key tables and columns used (with business meaning)
   - Performance notes
   - Any assumptions or limitations
   - Links to created files

## Reference Guide

Load detailed guidance based on context:

**Workflow phases & checkpoints:**

| Topic | Reference | Load When |
|-------|-----------|-----------|
| Phase 1–7 + Checkpoints (skip mode, rules) | (in SKILL.md above) | All 7 phases, 4 checkpoints, skip mode, checkpoint rules |

**Other references:**

| Topic | Reference | Load When |
|-------|-----------|-----------|
| Query Patterns | `references/query-patterns.md` | JOINs, CTEs, subqueries, recursive queries |
| Window Functions | `references/window-functions.md` | ROW_NUMBER, RANK, LAG/LEAD, analytics |
| Optimization | `references/optimization.md` | EXPLAIN plans, indexes, statistics, tuning |
| Database Design | `references/database-design.md` | Normalization, keys, constraints, schemas |
| Dialect Differences | `references/dialect-differences.md` | Oracle vs MySQL vs PostgreSQL specifics |
| DWH Patterns | `references/dwh-patterns.md` | Star schema, SCD, ETL, fact/dimension patterns |
| Knowledge folder (accumulated learnings) | `knowledge/single-table/`, `knowledge/multiple-tables/` | **Before executing a new task**: scan/load relevant files for tables and joins from past tasks to reuse context |
| Accumulated glossary (business terms) | `knowledge/glossary/` | **Phase 1**: after consulting business glossary, load matching term files (one file per term) for business insight, usage, data understanding from past tasks |
| Table/join knowledge | `knowledge/single-table/{db}_{schema}_{table}.md`, `knowledge/multiple-tables/{db}_{t1}_{t2}….md` | When a table or join is in scope, load the matching knowledge file if it exists for context |

## Scripts Reference

| Script | Purpose | Key Usage |
|--------|---------|-----------|
| `@scripts/check_table.py` | Inspect table structure + comments | `python @scripts/check_table.py SCHEMA TABLE --db DWH` |
| `@scripts/search_schema.py` | Search DB metadata by name/comment (regex default; use `\|` for multi-term) | `python @scripts/search_schema.py -k "keyword" --db DWH` — multi-term: `-k "LOAN_SCH\|LOAN_DTL"` |
| `@scripts/search_documents.py` | Search Excel docs (DWH + source meta); regex default | `python @scripts/search_documents.py -k "keyword" --folder documents/` — multi-term: `-k "term1\|term2"` |
| `@scripts/search_glossary.py` | Search business glossary (terms, definitions, DWH mapping); regex default | `python @scripts/search_glossary.py -k "keyword" --folder documents/` — multi-term: `-k "term1\|term2"` |
| `@scripts/explain_query.py` | Run EXPLAIN PLAN | `python @scripts/explain_query.py --db DWH --file q.sql` |
| `@scripts/run_query_safe.py` | Execute with safety limits | `python @scripts/run_query_safe.py --db DWH --file q.sql` |
| `@scripts/find_relationships.py` | Find FK / join paths | `python @scripts/find_relationships.py -s SCHEMA -t TABLE` |
| `@scripts/sample_data.py` | Sample data + profiling | `python @scripts/sample_data.py -s SCHEMA -t TABLE --profile` |
| `@scripts/search_procedures.py` | Find procedure/package/function by table or text (Oracle); lineage input/output. For lineage, use in **multiple rounds** with `scenarios/` (see Phase 2e). | Prefer **--db DWH_ADMIN**. `--table TABLE [--schema SCHEMA] --db DWH_ADMIN` — by name: `--name SCHEMA.PKG_X` — repeat for new table names from each round. |

## Database Connections

**Alias usage (prefer by purpose):**
- **DWH_ADMIN** — Use **only** for **procedure/package/function search and lineage**: finding PROCEDURE, PACKAGE, PACKAGE BODY, FUNCTION that reference tables or text. Prefer DWH_ADMIN over DWH when searching for code objects.
- **DWH** — Use only for **data extraction and analytics**: schema search, table inspection, sampling, relationships, EXPLAIN PLAN , and query execution.

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

**Task naming convention:** Any file or folder created per task name **must** use a date prefix `{YYYY-MM-DD}_` to ensure easy sorting and look-up (e.g., `2025-02-22_revenue-by-region/`, `2025-02-22_revenue-by-region-brief.md`).

```
documents/      -> Excel metadata (DWH + source systems). Usage standard:
  dwh-meta-tables.xlsx, dwh-meta-columns.xlsx  -> DWH (integrated tables/columns)
  [source]-meta-tables.xlsx, [source]-meta-columns.xlsx -> Individual source system dictionary (source-a, source-b, ...)
  *glossary*.xlsx, *bg-*.xlsx (business glossary) -> Terms, definitions, calculation method, DWH Table/Field, Calculation SQL (used in Phase 1)
scenarios/      -> Extracted Oracle Data Integrator (ODI) scenario information. Use in Phase 2e when tracing lineage: alternate reading/scaning scenarios/ with search_procedures.py over multiple rounds to trace upstream/downstream as far as possible.
queries/        -> Existing SQL queries (reference for patterns)
queries/agent-written/  -> Output: queries written by this agent (naming: {task-folder}.sql)
references/     -> SQL and DWH reference guides
scripts/        -> Python tools for database inspection and query testing
knowledge/glossary/       -> Accumulated knowledge per **business term**: one file per term (e.g. {term-slug}.md). Content: business insight, usage in reports, data understanding, gotchas, and references to tables/columns from past tasks. **Consult in Phase 1** after the formal business glossary to enrich the brief with prior experience.
knowledge/single-table/   -> Knowledge base: one file per table. Naming: {source_db}_{schema}_{table}.md. Accumulate learnings across sessions. **Consult at the start of a new task** for relevant tables.
knowledge/multiple-tables/ -> Knowledge base: one file per set of joined tables. Naming: {source_db}_{table1}_{table2}[_{table3}…].md. Accumulate learnings across sessions. **Consult at the start of a new task** for relevant joins.
```

## Constraints

### MUST DO
- **Follow the 7-phase workflow in order**
- **STOP at every [CHECKPOINT] and wait for user confirmation before proceeding**
- Present clear summaries at each checkpoint with structured questions
- Incorporate user feedback / domain knowledge when provided at checkpoints
- Re-present and re-confirm if user corrections are significant
- Create task brief before data discovery (Phase 1)
- Use date prefix **{YYYY-MM-DD}_** for any file or folder named with task-name (e.g. `{YYYY-MM-DD}_{task-name}/`, `{YYYY-MM-DD}_{task-name}-brief.md`) so outputs are easy to sort and find
- In Phase 1, consult business glossary for key terms/KPIs to enrich the brief (definitions, calculation, DWH candidates); then consult accumulated knowledge glossary (`knowledge/glossary/`: one file per term — business insight, usage, data understanding) and merge relevant points into the brief before Phase 2
- **Before discover data**: Consult the knowledge folders (`single-table/`, `multiple-tables/`) for accumulated data understanding from previous tasks — check for files matching tables/joins relevant to the task and load them for context before Phase 2.
- Search BOTH documents/ (DWH + source docs when relevant) AND database metadata for data discovery (Phase 2); when tables or joins are in scope, load matching knowledge files from `single-table/` and `multiple-tables/` if they exist.
- **When tracing lineage** (table/column used as input or output, lineage within DWH or back to source): In Phase 2, run **iterative lineage expansion** (Phase 2e): alternate multiple rounds of `scripts/search_procedures.py` (--db DWH_ADMIN) and reading/scaning `scenarios/` (ODI extracted scenarios), using each round’s new table/interface/object names as input to the next, until no new references are found or a reasonable round limit is reached, then build one complete lineage view.
- Document data mapping before writing query (Phase 3)
- Write CTEs with inline comments explaining reasoning (Phase 4)
- Run EXPLAIN PLAN before executing query (Phase 5)
- Wrap test execution with safety limits (Phase 5)
- When query results are abnormal (0 rows, important columns all NULL, or timeout/very slow): use Phase 5c CTE isolation (bottom-up for empty/NULL, top-down for slow) and **use a subagent** to run the isolation steps and report the faulty CTE
- Save output query to `queries/agent-written/` with header comment (Phase 7)
- When the job finishes successfully, distill session learnings into knowledge-base files in `single-table/`, `multiple-tables/`, and optionally `knowledge/glossary/` for business terms/KPIs (one file per object or per term; if file exists, read then merge/append with date and task context); never include real data samples, PII, internal identifiers, or confidential business data in those files (see Phase 7 Security — knowledge base content)
- When table/column lineage was traced and confirmed (Phase 2e), retain that lineage knowledge in `knowledge/single-table/` (one file per table; merge/append procedures/packages and ODI scenario context as above)
- Handle NULLs explicitly in all comparisons
- Apply partition pruning on partitioned tables
- Use column comments to understand business meaning of data
- **Database alias**: Use **DWH_ADMIN** only for procedure/package/function search (`scripts/search_procedures.py`); use **DWH** only for data extraction and analytics (search_schema, check_table, sample_data, find_relationships, explain_query, run_query_safe).
- **PII in queries**: Before executing any SQL, ensure no column that is or may be PII (e.g. name, email, phone, ID number, address; or columns marked CDE/PII in metadata) appears in the SELECT list as a direct column. If analytics need PII, use only aggregations (e.g. `COUNT(*)`, `COUNT(DISTINCT col)`).

### MUST NOT DO
- **Proceed past any [CHECKPOINT] without explicit user approval**
- **Assume tables/columns/filters are correct without user confirmation**
- **Ignore user-provided domain knowledge or corrections at checkpoints**
- **Execute SQL that selects columns that are or may be PII** (e.g. name, email, phone, national ID, address, or columns marked CDE/PII in metadata) **as direct result columns**. PII may appear only inside aggregation functions (e.g. `COUNT(email)`, `COUNT(DISTINCT customer_id)`); raw PII must not be returned in the result set.
- Jump straight to writing SQL without understanding data first
- Skip EXPLAIN PLAN analysis
- Use **DWH_ADMIN** for something else than procedure/package/function search (`scripts/search_procedures.py`)
- Note that AI Agent or Cursor is author of generated document or scripts or SQL, on any file.
- Use DWH logic when user explicitly asks not to
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
