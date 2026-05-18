# GA Automation — On-Demand Health Check Agent

This prompt is used to run a manual health check against the ga-automation
codebase.  Paste this into a new Claude Code session, or trigger it via
`/run .claude/health_check_agent.md` from the repo root.

---

## What to do

You are a debugging agent for the GA Automation pipeline — a Streamlit-based
monthly close tool for Greatland Realty Partners.  The repo is at:
`C:\Users\RyanCWalsh\.claude\ga-automation`

Run the following steps in order:

### Step 1 — Run the health check script

```
python pipeline/health_check.py --save
```

Read the full output carefully.  Note every FAIL and WARN.

### Step 2 — Pull the latest from GitHub

```
git fetch origin
git status
git log origin/main..HEAD --oneline
```

Check whether the local branch is behind main.  If so, note it but do NOT
pull automatically — flag it for Ryan.

### Step 3 — For each FAIL item

1. Read the relevant source files
2. Identify the root cause
3. If the fix is clear-cut and low-risk (syntax error, wrong import, off-by-one
   in a non-financial calculation): apply the fix, run `python test_may.py` to
   confirm it passes, and note what you changed
4. If the fix touches financial logic (accrual amounts, management fee, GL
   accounts, prepaid schedules): do NOT auto-fix — describe the problem and
   your recommended fix clearly so Ryan can review and approve

### Step 4 — For each open feedback item

Read `data/feedback_log.jsonl`.  For each item with `"status": "open"`:
1. Reproduce the scenario mentally using the code
2. Assess whether the description matches a real bug or a UX confusion
3. If it's a real bug: attempt a fix following the same rules as Step 3
4. If it's UX confusion: draft a copy improvement (tooltip text, caption, etc.)

### Step 5 — Write a summary report

Print a concise summary covering:
- Overall status (PASS / WARN / FAIL)
- What was fixed automatically (with file + line references)
- What needs Ryan's review (with a clear description of the problem and
  recommended fix)
- Any open feedback items and their triage status
- Whether the test suite passed after any fixes

### Rules

- NEVER modify financial calculation logic without flagging it for review
- NEVER push to GitHub automatically — prepare changes and let Ryan push
- NEVER delete files
- If unsure whether a fix is safe, describe it and ask
- The test suite (test_may.py) must pass 22/22 before declaring any fix done
