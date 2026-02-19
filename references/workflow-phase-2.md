# Phase 2: Data Discovery

Search **three sources in parallel** to find relevant tables and columns:

## 2a. Search Excel Documentation

```bash
python @scripts/search_documents.py --keyword "your keyword" --folder documents/
```

The `documents/` folder contains **two types** of standardized Excel metadata; search here first and use the right type for the task:

- **DWH metadata** (data warehouse — consolidated from all sources):
  - `dwh-meta-tables.xlsx`: bảng DWH (Tên Bảng, Mô tả bảng, Schema, Source, Domain, Phân loại DIM/FACT/RPT, …)
  - `dwh-meta-columns.xlsx`: cột DWH (Tên Bảng, Tên Trường, Mô tả, Kiểu dữ liệu, Mapping Rule, CDE/PII, …)
  Use when the question is about **tables/columns in the DWH** (reporting, KPI, join trong DWH).

- **Source-system metadata** (từng hệ thống nguồn riêng lẻ):
  - `[source]-meta-tables.xlsx`: Table Name, Description, Care, Type (ví dụ `sourceA-meta-tables.xlsx`)
  - `[source]-meta-columns.xlsx`: Column Name, Data Type, Comment, Sample Data, Table Name
  Use when the question involves **data từ hệ thống nguồn** (ETL mapping, nguồn gốc dữ liệu, từ điển source).

Kết quả search có `doc_type` (dwh_tables, dwh_columns, source_tables, source_columns) và `source_name` (với file source). Ưu tiên DWH docs cho câu hỏi báo cáo/analytics; khai thác source docs khi cần tra nguồn hoặc mapping ETL.

## 2b. Search Database Schema Metadata

```bash
python @scripts/search_schema.py --keyword "your keyword" --db DWH
python @scripts/search_schema.py --keyword "your keyword" --search-in comments --schema OWNER
```

Most database (excel DWH) columns have **comments** that describe their business meaning. This is critical
for understanding what data is available. Search both column names AND comments.

## 2c. Search Previous Queries

Look in the `queries/` folder for similar prior queries that might reveal useful patterns,
table names, or join conditions.

## 2d. Deep Inspection (after finding candidates)

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

---

## [CHECKPOINT 2] — Confirm Table & Column Selection

**STOP after data discovery and present findings to the user.** Before creating the
data mapping document, the user MUST confirm the discovered tables/columns are correct.

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
   - Example: "AMOUNT is actually gross amount, not net" or "CUSTOMER_TYPE values are
     'CORP' and 'RETAIL', not 'B2B' and 'B2C'"

4. **Do you know the join conditions?** (free-text, optional)
   - Example: "Join FACT_SALES to DIM_CUSTOMER on CUST_ID, but watch out for NULL CUST_ID
     on internal transfers"

**If the user provides corrections:**
- Run additional discovery searches if new tables/columns are mentioned
- Re-present updated findings and confirm again
- Only proceed to Phase 3 after explicit approval
