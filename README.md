# Point an MLflow experiment at an existing OTel trace table

Make OTel span data that **already exists in a Unity Catalog Delta table** show up in an **MLflow
experiment** — the Traces UI, `mlflow.search_traces`, and MLflow GenAI evaluation — without
regenerating it from an instrumented app. The table MLflow ends up bound to is always the current
**`otel.schemaVersion = v2`** schema.

Works for any UC span table: MLflow-created traces, Zerobus OTLP ingestion output, the redacted
output of a PII pipeline, or traces an external / non-Databricks app exported into UC.

**Scope.** This repo starts from the point where **OTel span data already exists in a Unity
Catalog Delta table**. Getting it there — instrumenting an app, OTLP/Zerobus ingest, an ETL job, a
redaction pipeline — is upstream and **out of scope**. The source must be **span-shaped** (one row
per span, with `trace_id` / `span_id` / `attributes`) and live in **UC**.

## The two schema versions

Databricks defines exactly two UC trace-table schema versions (there is no v3; this is unrelated to
OpenTelemetry's own `schema_url` semantic-convention versioning, which lives in the `*_schema_url`
columns):

| | **v1** (legacy) | **v2** (current, recommended) |
|---|---|---|
| Trace location | 2-part `catalog.schema` | 3-part `catalog.schema.table_prefix` |
| Table names | fixed `mlflow_experiment_trace_otel_*` | `{prefix}_otel_*` |
| `attributes` | `MAP<STRING,STRING>` | `VARIANT` |
| Annotations | tags/assessments as log events | dedicated `_otel_annotations` table |

This repo always targets **v2**. Zerobus OTLP ingestion already stamps `otel.schemaVersion=v2`; if a
table has no such property, the notebook infers the version from its shape.

## How binding works (and why v2)

You **cannot** point an experiment at a pre-existing table just by naming it — MLflow's backend
only accepts tables it recognizes. Two APIs matter:

- **`mlflow.set_experiment(experiment_name=..., trace_location=UnityCatalog(cat, schema, prefix))`**
  — the current (v2) path. It **adopts** a pre-existing, conformant `{prefix}_otel_*` **v2** table
  set in place and builds the `_trace_metadata` / `_trace_unified` views on top. For a non-existent
  destination it creates the empty v2 tables.
- `mlflow.tracing.set_experiment_trace_location(...)` — the older **v1** (MAP) path. This repo no
  longer uses it.

> **No materialized view for the destination.** The v2 adoption API validates that the spans object
> is a physical table and **rejects a materialized view** (`Failed to validate table
> compatibility`). So the convert paths below always land in a physical table; only an *already-v2
> table set* is adopted without a copy.

## What the notebook does

[`bind_experiment_to_existing_traces.py`](bind_experiment_to_existing_traces.py) auto-detects the
source and routes to one of three paths — all ending on a v2 table:

| Detected source | Path | How it reaches v2 |
|---|---|---|
| **Conformant v2** — `otel.schemaVersion=v2` (or VARIANT attrs + `record_id/time/date/service_name`), named `{p}_otel_spans`, with sibling `_otel_logs`/`_otel_metrics` | **A — adopt in place** | `set_experiment(trace_location=UnityCatalog(...))` on the source schema. No copy. |
| **Standard v1** — `mlflow_experiment_trace_otel_spans` + `_otel_logs` | **B — official migrator** | Create a v2 destination, then `databricks.migrations.v1_to_v2.V1ToV2SqlMigration(...).run()` (MAP→VARIANT spans + log-events→annotations). |
| **Anything else** — renamed v1, external exports, non-standard layouts | **C — generic reshape** | Reshape the source into the v2 span schema (MAP/JSON/VARIANT → VARIANT, synthesize `record_id/time/date/service_name`), `INSERT OVERWRITE` a fresh v2 table, adopt it. |

## Quick start

1. Import the repo into a Databricks Git folder and open the notebook.
2. Fill the widgets: **source spans table**, **target catalog/schema/prefix** (for paths B/C),
   **experiment name**, **serverless SQL warehouse ID**.
3. **Run All** — it detects the version, reaches v2, binds, verifies, and prints the Traces UI link.

Run it **in the workspace** (in-workspace creds).

## Adopting external / non-MLflow traces

The reshape (Path C) adapts any column layout. Whether **request/response** render in the Traces UI
depends on the span's attribute keys, which MLflow's `_unified` view derives by `COALESCE`-ing over
recognized conventions:

| Column | Recognized keys (first non-null wins) |
|---|---|
| `request`  | `mlflow.spanInputs` · `input.value` (OpenInference) · `gen_ai.input.messages` · `gen_ai.tool.call.arguments` · `gcp.vertex.agent.llm_request` |
| `response` | `mlflow.spanOutputs` · `output.value` · `gen_ai.output.messages` · `gen_ai.tool.call.result` · `gcp.vertex.agent.llm_response` · `gcp.vertex.agent.tool_response` |

MLflow, OpenInference, OTel-GenAI, and Vertex sources render with no extra work. For **custom
keys**, alias them to a recognized key in the Path C reshape; spans always render regardless.

## Notes & caveats

- **A standard v1 table under a non-standard name** can still use the official migrator (which
  infers `mlflow_experiment_trace_otel_*` names) by aliasing it via views in a scratch schema and
  pointing Path B there; otherwise it falls to Path C (spans only — tags/assessments aren't carried
  into annotations).
- **Destination is always v2 / VARIANT.** Path A adopts in place (no copy); B/C write a v2 physical
  table and are idempotent (re-run / schedule to refresh).
- **events / links** are preserved when the source carries them (nested attrs → VARIANT), else
  defaulted to empty arrays.
- **Requires** `mlflow[databricks] >= 3.14.0`, `databricks-agents >= 1.10.1` (Path B), and a
  serverless SQL warehouse. `search_traces` from a local/CLI token can 401 on the trace-read API —
  run in-workspace or verify via the `_unified` view (the notebook does both).

## Files

| File | Purpose |
|---|---|
| `bind_experiment_to_existing_traces.py` | Guided notebook — detect version, reach v2, bind, verify |
| `SCHEMA_REFERENCE.md` | The v1 and v2 OTel span schemas + the reshape/migration mapping |
| `README.md` | This file |

## Provenance

Validated end-to-end in a Databricks workspace: v2 adopt-in-place, official `V1ToV2SqlMigration`
(standard v1 and view-aliased renamed v1), and the generic reshape — against MLflow-created,
PII-redacted, and external (OpenInference-convention) sources. The adoption API's rejection of
materialized views for a v2 destination is likewise tested, not assumed.
