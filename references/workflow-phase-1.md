# Phase 1: Requirement Analysis & Goal Setting

Before touching any data, clearly define the problem and **enrich it with business glossary** so that Phase 2 data discovery is targeted and aligned with standard definitions.

1. **Business question**: What is the user really asking? Restate it precisely.
2. **Output definition**: What should the result look like? (columns, rows, format)
3. **Data modules**: Break into logical data domains (e.g., "customer data", "transaction data")
4. **Data elements needed**: List specific measures (SUM, COUNT, AVG) and dimensions (GROUP BY)
5. **Filters & scope**: Time range, business unit, status filters, etc.
6. **Consult Business Glossary** (before finalizing the brief):  
   Search the business glossary for **key terms and KPIs** mentioned in the question (indicator name, terminology). This clarifies definitions, calculation methods, and points to DWH tables/columns *before* you search docs/schema.
   ```bash
   python @scripts/search_glossary.py --keyword "your term or KPI" --folder documents/
   ```
   Use glossary results to:
   - **Definition**: Standard definition of the term — include in the brief so the problem is unambiguous.
   - **Calculation method**: Use this to validate or draft the expected logic.
   - **DWH table/column, calculation SQL**: Suggested DWH table/column and SQL snippet — note these as candidates for Phase 2 (do not assume they are the only source; still run data discovery and confirm).
   - **Domain, Owning Unit**: Helps with scoping and identifying ownership.
   Enrich the task brief with these so the problem is stated in business language and Phase 2 discovery is more effective.
7. **Save brief**: Create folder `{YYYY-MM-DD}_{task-name}/` and file `{YYYY-MM-DD}_{task-name}-brief.md` inside it (e.g. `2025-02-22_revenue-by-region/2025-02-22_revenue-by-region-brief.md`). Always use date prefix **{YYYY-MM-DD}_** for task-name files and folders so they sort and navigate easily.

## Template for brief

```markdown
# Task: {task name}
## Business Question
{restate the question}
## Glossary / Standard Definitions (from business glossary, if found)
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

---

## [CHECKPOINT 1] — Confirm Requirements Understanding

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
