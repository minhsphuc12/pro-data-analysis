# Phase 4: Query Design & Reasoning

Write the query following these principles:
- Use **CTEs** to separate logical steps clearly
- Add **inline comments** explaining WHY each part exists
- Reference specific findings from Phase 3
- Apply **early filtering** (especially partition keys)
- Handle **NULLs** explicitly
- Use **set-based operations** (never cursors)
- **PII**: Do **not** put columns that are or may be personally identifiable information (PII) in the SELECT list as direct output columns. If PII is needed for analytics, use only **aggregation functions** (e.g. `COUNT(*)`, `COUNT(DISTINCT col)`, `MIN`/`MAX` for grouping). Use DWH/source metadata (e.g. CDE/PII in `dwh-meta-columns.xlsx`) to identify PII columns.

## Query structure template

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

---

## [CHECKPOINT 4] — Confirm Query Logic Before Testing

**STOP after writing the query.** Present the full SQL query and a plain-language
explanation of what each CTE / step does.

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

**If the user requests changes:**
- Modify the query and re-present the updated logic summary
- Only proceed to Phase 5 after explicit approval
