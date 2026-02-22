# Phase 7: Save & Document

1. **Save query** to `queries/agent-written/{YYYY-MM-DD}_{task-name}.sql`
   - Include standard header comment (purpose, author, date, tables, performance notes)

2. **Update data mapping** document (`{YYYY-MM-DD}_{task-name}/{YYYY-MM-DD}_{task-name}-data-mapping.md`) with final results

3. **Report to user** with:
   - Final query
   - Key tables and columns used (with business meaning)
   - Performance notes
   - Any assumptions or limitations

4. **Session knowledge distillation** (when the job finishes successfully):  
   Distil what you learned in this session and persist it into the knowledge-base files below.  
   **One object = one file**: each table (single-table) or each set of joined tables (multiple-tables) has exactly one knowledge file. If a file for that object already exists, **read it first**, then **merge/append** the new learnings (with date and brief task context) into the same file so knowledge accumulates across tasks.

   - **Folder `single-table/`**  
     Use when the task involved **one main table** (or you want to record what you learned about one table).  
     Content to capture: actual state of data in the table, which columns are important, usage, purpose, how to interpret values correctly, null/edge cases, sample value patterns, and any caveats.  
     **File name**: `{source_db}_{schema}_{table}.md`  
     - `source_db`: connection alias (e.g. `DWH`, `SOURCE_A`, `SOURCE_B`).  
     - `schema`: schema/owner of the table.  
     - `table`: table name.  
     Example: `DWH_DWH_FACT_SALES.md`, `SOURCE_A_SOURCE_A_ACCOUNTS.md`.

   - **Folder `multiple-tables/`**  
     Use when the task required **joining two or more tables**.  
     Content to capture: how and why these tables are connected, join keys and conditions, usage and purpose of the combination, correct interpretation of the result set, typical filters, and gotchas (e.g. duplicates, nulls on join keys).  
     **File name**: `{source_db}_{table1}_{table2}[_{table3}…].md`  
     - Use the same `source_db` for the object; list tables in a consistent order (e.g. fact first, then dimensions).  
     Example: `DWH_FACT_SALES_DIM_CUSTOMER.md`, `DWH_FACT_SALES_DIM_CUSTOMER_DIM_PRODUCT.md`.

   **Format inside each file** (Markdown): use clear headings (e.g. Purpose, Important columns, Join conditions, Data quality, Session notes) and append new session notes with a date and short task reference so the file remains one cumulative knowledge base per object.

   **Security — knowledge base content:**  
   Do **NOT** write any of the following into `single-table/` or `multiple-tables/` files (to avoid leaking sensitive company information, especially if these folders are ever committed or shared):

   - **Real data samples or row-level values** from production (e.g. actual IDs, names, amounts, dates that identify real entities). Use only **generic placeholders or value types** (e.g. "values like 'ACTIVE'/'PENDING'", "numeric ID", "date range").
   - **PII or confidential business data**: no real customer names, emails, phone numbers, account numbers, contract values, or internal codes that could identify people or deals.
   - **Internal-only identifiers**: no real hostnames, instance names, schema names that reveal company infrastructure; prefer generic aliases (e.g. DWH, SOURCE_A) or role-based names.
   - **Proprietary business rules or KPIs** that are strictly confidential (if in doubt, describe logic in abstract terms only, without numbers or formulas that are company secret).
   - **Connection strings, credentials, or environment details** (no DSN, host, database name beyond the generic alias used in the skill).

   When describing "sample value patterns" or "data quality", stick to **structure and semantics** (e.g. "column allows NULL; distinct values observed: status-like codes") rather than dumping real values. If the task revealed sensitive context, record only **what is needed for future queries** (e.g. join key names, filter column names, data types) and omit the sensitive detail.
