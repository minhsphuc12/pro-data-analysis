# Phase 3: Data Mapping & Documentation

Create `{task-name}/{task-name}-data-mapping.md` in working directory documenting everything found:

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

---

## [CHECKPOINT 3] — Confirm Data Mapping & Business Rules

**STOP after creating the data mapping document.** This is the last checkpoint before
writing SQL. The user MUST verify the complete mapping is correct.

**Present the data mapping summary and ask:**

1. **Are the join conditions correct?**
   - Yes
   - No, the correct join is: (free-text)

2. **Are the filter / WHERE clause conditions correct?**
   - Yes
   - No, adjust: (free-text)
   - Example corrections: "Filter should be STATUS = 'ACTIVE', not STATUS != 'DELETED'"
     or "Date filter should use POSTING_DATE not TRANSACTION_DATE"

3. **Are the business rules / calculations correct?**
   - Yes
   - No, the correct formula is: (free-text)
   - Example: "Revenue = AMOUNT - DISCOUNT - TAX, not just AMOUNT"

4. **Any NULL handling or edge cases I should know about?** (free-text, optional)
   - Example: "DISCOUNT can be NULL, treat as 0" or "Exclude rows where CUST_ID = -1
     (dummy customer)"

5. **Aggregation and grouping — does this look right?**
   - Yes
   - No, I need different grouping: (free-text)

**If the user provides corrections:**
- Update the data mapping document
- Re-present if changes are substantial
- Only proceed to Phase 4 after explicit approval
