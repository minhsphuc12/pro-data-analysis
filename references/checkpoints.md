# Human-in-the-Loop Checkpoints

**This workflow includes mandatory confirmation checkpoints by default.** At each checkpoint,
the agent MUST stop and wait for explicit user approval before proceeding. This prevents
wasted time from wrong table choices, incorrect filters, or misunderstood requirements.

## Skip Mode

Checkpoints can be skipped when the user explicitly opts out. Look for signals like:
- "skip checkpoints", "no checkpoints", "no need to confirm", "just do it"
- "I'm busy", "bận", "không cần hỏi", "chạy thẳng", "auto mode"
- "skip CP", "fast mode", "no stops"

**When skip mode is activated:**
- Run the full workflow end-to-end without stopping at checkpoints
- Still **produce the same summaries** (brief, table list, data mapping, query logic)
  inline in your output so the user can review afterward — just don't wait for a response
- The user can re-enable checkpoints anytime by saying "enable checkpoints" or similar

**Partial skip:** The user may also skip individual checkpoints (e.g., "skip checkpoint 1"
or "I trust the tables, skip CP2") — honor the specific request and keep the rest active.

## Checkpoint rules (when active)

- Use structured questions (multiple-choice + free-text option) whenever possible
- Present findings clearly in a summary before asking for confirmation
- NEVER proceed past a checkpoint without user response
- If user says "no" or provides corrections, incorporate feedback and re-present
- If user provides additional domain knowledge, capture it in the task brief / data mapping
- Checkpoints are labeled **[CHECKPOINT N]** — there are 4 total
