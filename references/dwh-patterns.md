# Data Warehouse Patterns & Best Practices

## Star Schema

### Structure
- **Fact table**: Central table with measures (amounts, counts, quantities) + foreign keys to dimensions
- **Dimension table**: Descriptive attributes (who, what, where, when, how)

### Common Fact Types
- **Transaction fact**: One row per event (e.g., each sale)
- **Periodic snapshot**: One row per time period (e.g., monthly balance)
- **Accumulating snapshot**: One row per lifecycle (e.g., order fulfillment stages)

### Query Pattern
```sql
-- Star schema: fact + dimensions join
SELECT
    d_time.year_month,
    d_product.category_name,
    d_branch.branch_name,
    SUM(f.amount) AS total_amount,
    COUNT(*) AS transaction_count
FROM fact_transactions f
JOIN dim_time d_time ON f.time_key = d_time.time_key
JOIN dim_product d_product ON f.product_key = d_product.product_key
JOIN dim_branch d_branch ON f.branch_key = d_branch.branch_key
WHERE d_time.year = 2024
GROUP BY d_time.year_month, d_product.category_name, d_branch.branch_name;
```

### Best Practices
- Always filter by time dimension first (partition pruning)
- Join fact to dimensions, not dimension to dimension through fact
- Use surrogate keys (integer) for dimension keys, not natural keys
- Keep fact tables narrow: only keys + measures

---

## Slowly Changing Dimensions (SCD)

### Type 1: Overwrite
- Simply update the dimension record
- No history preserved
- Use when: historical accuracy not important

### Type 2: Add New Row
- Insert new row with new surrogate key
- Track with `effective_date`, `expiry_date`, `is_current` flag
- Use when: need full history

```sql
-- SCD Type 2: Get current dimension value
SELECT * FROM dim_customer
WHERE is_current = 'Y';

-- SCD Type 2: Get dimension value at a specific date
SELECT * FROM dim_customer
WHERE :report_date BETWEEN effective_date AND NVL(expiry_date, DATE '9999-12-31');

-- Join fact with SCD Type 2 dimension (point-in-time)
SELECT f.*, d.*
FROM fact_transactions f
JOIN dim_customer d ON f.customer_key = d.customer_key;
-- Note: if fact stores surrogate key, it already points to the correct version
```

### Type 3: Add Column
- Add `previous_value` column
- Only tracks one level of history
- Use when: only need "before and after"

---

## Common DWH Query Patterns

### Year-over-Year Comparison
```sql
WITH current_year AS (
    SELECT product_key, SUM(amount) AS cy_amount
    FROM fact_sales f
    JOIN dim_time d ON f.time_key = d.time_key
    WHERE d.year = 2024
    GROUP BY product_key
),
prior_year AS (
    SELECT product_key, SUM(amount) AS py_amount
    FROM fact_sales f
    JOIN dim_time d ON f.time_key = d.time_key
    WHERE d.year = 2023
    GROUP BY product_key
)
SELECT
    cy.product_key,
    cy.cy_amount,
    py.py_amount,
    ROUND((cy.cy_amount - py.py_amount) / NULLIF(py.py_amount, 0) * 100, 2) AS yoy_growth_pct
FROM current_year cy
LEFT JOIN prior_year py ON cy.product_key = py.product_key;
```

### Running Total / Cumulative Sum
```sql
SELECT
    d.year_month,
    SUM(f.amount) AS monthly_amount,
    SUM(SUM(f.amount)) OVER (
        PARTITION BY EXTRACT(YEAR FROM d.calendar_date)
        ORDER BY d.year_month
    ) AS ytd_cumulative
FROM fact_sales f
JOIN dim_time d ON f.time_key = d.time_key
GROUP BY d.year_month, EXTRACT(YEAR FROM d.calendar_date);
```

### Ranking / Top-N
```sql
SELECT * FROM (
    SELECT
        d.branch_name,
        SUM(f.amount) AS total,
        ROW_NUMBER() OVER (ORDER BY SUM(f.amount) DESC) AS rank
    FROM fact_sales f
    JOIN dim_branch d ON f.branch_key = d.branch_key
    GROUP BY d.branch_name
) WHERE rank <= 10;
```

### Pivot / Cross-Tab
```sql
-- Oracle PIVOT
SELECT * FROM (
    SELECT d_time.quarter, d_product.category, f.amount
    FROM fact_sales f
    JOIN dim_time d_time ON f.time_key = d_time.time_key
    JOIN dim_product d_product ON f.product_key = d_product.product_key
)
PIVOT (
    SUM(amount) FOR quarter IN ('Q1' AS q1, 'Q2' AS q2, 'Q3' AS q3, 'Q4' AS q4)
);
```

### Moving Average
```sql
SELECT
    d.calendar_date,
    f.amount,
    AVG(f.amount) OVER (
        ORDER BY d.calendar_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7d
FROM fact_daily_sales f
JOIN dim_time d ON f.time_key = d.time_key;
```

---

## ETL / Data Loading Patterns

### Incremental Load (CDC Pattern)
```sql
-- Load only new/changed records since last ETL run
INSERT INTO stg_customers
SELECT * FROM source_customers@dblink
WHERE last_modified_date > (
    SELECT MAX(etl_load_date) FROM etl_control WHERE table_name = 'CUSTOMERS'
);
```

### Merge / Upsert (Oracle)
```sql
MERGE INTO dim_customer tgt
USING stg_customer src ON (tgt.customer_id = src.customer_id)
WHEN MATCHED THEN
    UPDATE SET tgt.name = src.name, tgt.updated_date = SYSDATE
WHEN NOT MATCHED THEN
    INSERT (customer_key, customer_id, name, created_date)
    VALUES (dim_customer_seq.NEXTVAL, src.customer_id, src.name, SYSDATE);
```

---

## Oracle-Specific DWH Features

### Partition Pruning
```sql
-- Always include partition key in WHERE clause
-- If table is partitioned by TRANSACTION_DATE:
SELECT * FROM fact_transactions
WHERE transaction_date >= DATE '2024-01-01'
  AND transaction_date < DATE '2024-02-01';
-- This ensures Oracle reads only the relevant partition
```

### Parallel Query
```sql
-- Enable parallel for large scans
SELECT /*+ PARALLEL(f, 4) */ ...
FROM fact_transactions f
WHERE ...;
```

### Materialized View for Pre-Aggregation
```sql
CREATE MATERIALIZED VIEW mv_monthly_sales
BUILD IMMEDIATE
REFRESH FAST ON DEMAND
AS
SELECT d.year_month, d.product_key,
       SUM(f.amount) AS total_amount, COUNT(*) AS cnt
FROM fact_sales f
JOIN dim_time d ON f.time_key = d.time_key
GROUP BY d.year_month, d.product_key;
```

### Analytic Functions for DWH
```sql
-- Ratio to total
SELECT
    branch_name,
    total_amount,
    ROUND(total_amount / SUM(total_amount) OVER () * 100, 2) AS pct_of_total
FROM (
    SELECT d.branch_name, SUM(f.amount) AS total_amount
    FROM fact_sales f
    JOIN dim_branch d ON f.branch_key = d.branch_key
    GROUP BY d.branch_name
);

-- NTILE for distribution analysis
SELECT
    customer_key,
    total_spend,
    NTILE(10) OVER (ORDER BY total_spend DESC) AS decile
FROM (
    SELECT customer_key, SUM(amount) AS total_spend
    FROM fact_sales
    GROUP BY customer_key
);
```

---

## Performance Checklist for DWH Queries

1. **Partition pruning**: Filter includes partition key?
2. **Index usage**: JOIN keys and frequent filter columns indexed?
3. **Avoid full table scan on large facts**: Always filter by date range
4. **Pre-aggregate**: Use materialized views for frequently-run summaries
5. **Minimize data movement**: Filter early, aggregate early
6. **Star transformation**: Oracle can use star transformation for star joins
7. **Parallel execution**: Consider for large batch/report queries
8. **Statistics current**: Check LAST_ANALYZED on tables and indexes
