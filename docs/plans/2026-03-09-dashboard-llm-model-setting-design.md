# Dashboard LLM Model Setting Design

## Goal

Allow users to view and update the blog analysis LLM model directly from the custom dashboard, while keeping Airflow Variable `blog_llm_model` as the runtime source of truth.

## Current Context

- The blog processor resolves the model name from Airflow Variable `blog_llm_model`, with fallback to `BLOG_LLM_MODEL` and then `gemini-3-flash-preview`.
- The custom dashboard already reads local PostgreSQL data and calls the Airflow API for manual DAG re-analysis.
- The dashboard currently has no settings surface for the active LLM model.

## Approved Approach

Use Airflow Variable `blog_llm_model` as the only writable setting store. The dashboard reads the current value from Airflow and renders it on `/blogs`. The dashboard writes updates back to Airflow through a dedicated POST endpoint. Suggested model options come from historical `processing_logs.llm_model` values plus the current configured value.

## UI Design

- Add an LLM settings panel near the top of the `/blogs` page.
- Use a text input with an attached `datalist`.
- Users can type any model name.
- If a model has appeared before in `processing_logs`, it appears as a dropdown suggestion.
- The current active model is prefilled in the input.
- Saving redirects back to `/blogs` with a success or error flash message.

## Data Flow

### Read path

1. `/blogs` loads blog list data as before.
2. The route also fetches the current model value from Airflow Variable `blog_llm_model`.
3. If the variable is missing or Airflow read fails, the route falls back to `BLOG_LLM_MODEL` or the processor default.
4. The route queries distinct non-empty `processing_logs.llm_model` values.
5. The route merges the current value into that set, deduplicates, sorts, and passes the result to the template.

### Write path

1. The user submits a model name to a new POST route.
2. The server trims whitespace and rejects an empty value.
3. The server upserts Airflow Variable `blog_llm_model` through the Airflow API.
4. On success, the dashboard redirects to `/blogs?message=...`.
5. On failure, the dashboard redirects to `/blogs?error=...`.

## Error Handling

- Blank input is rejected before any Airflow call.
- Airflow write failures are surfaced to the user as flash errors.
- Airflow read failures do not block the page; the dashboard falls back to the existing default resolution chain.
- Candidate list generation ignores blank and whitespace-only values from historical logs.

## Testing Strategy

- Verify `/blogs` renders the current model value and suggestion candidates.
- Verify successful model update trims input and calls the Airflow write helper.
- Verify blank input shows an error and does not call Airflow.
- Verify Airflow write failures redirect with an error message.

## Non-Goals

- No schema changes.
- No `.env` editing from the dashboard.
- No per-blog or per-post model override.
