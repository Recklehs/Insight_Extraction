# Dashboard LLM Model Setting Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a custom dashboard setting that lets users update the active blog-analysis LLM model with free text input plus historical dropdown suggestions.

**Architecture:** Keep Airflow Variable `blog_llm_model` as the source of truth. Extend the dashboard backend with read/write helpers for the Airflow API, expose the current model and historical candidates on `/blogs`, and add a dedicated POST route to save the model. Suggestions come from distinct `processing_logs.llm_model` values plus the current resolved model.

**Tech Stack:** FastAPI, Jinja2 templates, SQLAlchemy, unittest/TestClient, Airflow API

---

### Task 1: Document and test the settings behavior

**Files:**
- Modify: `tests/test_dashboard_analysis_views.py`

**Step 1: Write the failing test**

Add tests that assert:
- `/blogs` shows the current model value and a historical model candidate.
- `POST /settings/llm-model` trims whitespace, stores the submitted value, and redirects with a success message.
- Blank submission redirects with an error without storing anything.
- Storage failure redirects with an error.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_dashboard_analysis_views.py -q`
Expected: FAIL because the route context, helpers, and UI do not exist yet.

**Step 3: Write minimal implementation**

Only after seeing the failure, add the backend and template changes required for those tests.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_dashboard_analysis_views.py -q`
Expected: PASS

### Task 2: Add backend helpers and routes

**Files:**
- Modify: `dashboard/app/main.py`

**Step 1: Add read helpers**

Implement helper functions to:
- resolve fallback model value from environment/default,
- fetch the Airflow variable value,
- upsert the Airflow variable value,
- build the distinct historical candidate list from `ProcessingLog`.

**Step 2: Extend `/blogs`**

Update the `/blogs` route to include:
- `current_llm_model`
- `llm_model_candidates`

**Step 3: Add save route**

Add `POST /settings/llm-model` that:
- trims the input,
- rejects blank values,
- writes the variable through Airflow,
- redirects with flash messages.

**Step 4: Run focused tests**

Run: `pytest tests/test_dashboard_analysis_views.py -q`
Expected: PASS once backend changes satisfy the tests.

### Task 3: Add the dashboard form

**Files:**
- Modify: `dashboard/app/templates/blogs.html`

**Step 1: Add settings panel**

Render a panel above the blog-add form that includes:
- explanatory copy,
- a text input prefilled with the current model,
- a `datalist` populated from `llm_model_candidates`,
- a submit button.

**Step 2: Keep UX simple**

Use existing flash messaging, avoid adding new navigation or JS.

**Step 3: Re-run tests**

Run: `pytest tests/test_dashboard_analysis_views.py -q`
Expected: PASS

### Task 4: Verify final behavior

**Files:**
- Verify only

**Step 1: Run focused verification**

Run: `pytest tests/test_dashboard_analysis_views.py -q`
Expected: PASS with all dashboard tests green.

**Step 2: Record known baseline issue**

Run: `pytest -q`
Expected: existing collection failure in `tests/test_naver_blog_rss_checker.py` caused by the current Airflow/structlog environment mismatch, unrelated to this feature.
