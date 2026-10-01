# Point an MLflow experiment at an existing trace table

Make OTel span data that **already exists in Unity Catalog** show up in an **MLflow experiment**
— the Traces UI, `mlflow.search_traces`, and MLflow GenAI evaluation — without regenerating it
from an instrumented app.

Works for any UC span table: MLflow-created "raw" traces, the redacted output of a PII pipeline
(e.g. [otel-pii-redaction](https://github.com/hmoazam/otel-pii-redaction)), or traces an
external / non-Databricks app exported into UC.

**Scope.** This repo starts from the point where **OTel span data already exists in a Unity
Catalog Delta table**. Getting it there — instrumenting an app, OTLP ingest, an ETL job, or a
redaction pipeline — is upstream and **out of scope**; those are named only as example origins.
The source must be **span-shaped** (one row per span, with `trace_id` / `span_id` / `attributes`)
and live in **UC** — a UC Delta table is the only thing MLflow's trace backend can adopt.

## The catch (and the fix)

You **cannot** just name an existing table as an experiment's trace location. MLflow's backend
only accepts tables it recognizes as its own: it checks a Delta table property,
`otel.schemaVersion`, and rejects anything else with

```
ALREADY_EXISTS: ... already exists with incompatible schema version: expected v1, got UNSPECIFIED
```

The working recipe, validated end-to-end:

1. **Reshape** the source into MLflow's exact OTel span schema — notably `attributes` as
   `MAP<STRING,STRING>` and the precise column names/order (see [`SCHEMA_REFERENCE.md`](SCHEMA_REFERENCE.md)).
2. **Stamp** the property: `ALTER TABLE ... SET TBLPROPERTIES ('otel.schemaVersion' = 'v1')`.
3. **Bind** with `mlflow.tracing.set_experiment_trace_location(...)`. MLflow *adopts* your
   tables and builds its own `_metadata` / `_unified` views on top.

> **Two MLflow trace APIs exist — use the right one.**
> `set_experiment(trace_location=UnityCatalog(...))` (creation-time) does **not** adopt
> pre-existing tables, even stamped (server-side "Telemetry profile not found").
> `mlflow.tracing.set_experiment_trace_location(...)` **does**. This repo uses the latter.

## Quick start

1. Open [`bind_experiment_to_existing_traces.py`](bind_experiment_to_existing_traces.py) as a
   notebook in your Databricks workspace (import the repo into a Git folder).
2. Fill the widgets:
   - **Source spans table** — `catalog.schema.table` of your existing spans
   - **Target catalog / schema** — where MLflow's adopted tables will live (created if absent)
   - **Experiment name** — e.g. `/Users/you/existing-traces`
   - **SQL warehouse ID** — a serverless warehouse
3. **Run All.** It reshapes → stamps → binds → verifies, and prints the Traces UI link.

Run it **in the workspace** (in-workspace user creds). A local/CLI token can hit a 401 on the
trace-read API — see caveats.

## How it works

```
existing spans table                MLflow-owned, adopted tables (target schema)
┌─────────────────────────┐        ┌────────────────────────────────────────────┐
│ your_catalog.your_schema│        │ mlflow_experiment_trace_otel_spans   (table) │
│   .your_otel_spans       │        │ mlflow_experiment_trace_otel_logs    (table) │
│  (VARIANT/JSON attrs,    │─reshape│ mlflow_experiment_trace_otel_metrics (table) │
│   any column layout)     │  +stamp│ mlflow_experiment_trace_metadata     (view)  │  ← MLflow builds
│                          │  +bind │ mlflow_experiment_trace_unified      (view)  │  ← MLflow builds
└─────────────────────────┘        └────────────────────────────────────────────┘
                                          ▲ experiment bound here (V4 API)
```

## Adopting external / non-MLflow traces

The reshape (Step 3) adapts **any column layout** — missing columns are defaulted and
`attributes` is coerced to `MAP<STRING,STRING>` whether the source stored it as VARIANT, a JSON
string, or an existing map. So a span table exported into UC by a non-MLflow app binds and lists
fine, regardless of its schema.

Whether the **request / response** columns populate in the Traces UI depends on the span's
**attribute keys**, not on how the table was produced. MLflow's `_unified` view derives them by
`COALESCE`-ing over a fixed set of recognized conventions:

| Column | Recognized attribute keys (first non-null wins) |
|---|---|
| `request`  | `mlflow.spanInputs` · `input.value` (OpenInference) · `gen_ai.input.messages` · `gen_ai.tool.call.arguments` · `gcp.vertex.agent.llm_request` |
| `response` | `mlflow.spanOutputs` · `output.value` · `gen_ai.output.messages` · `gen_ai.tool.call.result` · `gcp.vertex.agent.llm_response` · `gcp.vertex.agent.tool_response` |

If your source uses one of these (MLflow, OpenInference, OTel-GenAI, Vertex), request/response
render with no extra work — validated against an external OpenInference-convention table. If it
uses **custom keys** (e.g. `prompt_text` / `reply_text`), the spans still appear but
request/response come up empty; alias the custom key to a recognized one in the Step 3 reshape,
e.g.:

```sql
-- surface custom keys under names MLflow recognizes (later keys win in map_concat)
map_concat(
  from_json(CAST(s.attributes AS STRING), 'MAP<STRING,STRING>'),
  map('mlflow.spanInputs',  get_json_object(CAST(s.attributes AS STRING), '$.prompt_text'),
      'mlflow.spanOutputs', get_json_object(CAST(s.attributes AS STRING), '$.reply_text'))
) AS attributes
```

## Files

| File | Purpose |
|---|---|
| `bind_experiment_to_existing_traces.py` | Guided Databricks notebook — reshape, stamp, bind, verify |
| `SCHEMA_REFERENCE.md` | The exact target OTel span schema + reshape mapping (incl. events/links) |
| `README.md` | This file |

## Caveats

- **The default is a physical copy.** The notebook copies the reshaped spans into the target via
  `INSERT OVERWRITE` (re-run or schedule it as the source grows). This is the recommended path —
  the experiment stays writable and is decoupled from the source's lifecycle.
  - A **materialized view** named `mlflow_experiment_trace_otel_spans` **can** be adopted instead
    (no data copy), but **read-only**: stamp `otel.schemaVersion=v1` *inline* in
    `CREATE … TBLPROPERTIES(…) AS` (an MV rejects `ALTER … SET TBLPROPERTIES`), and any new traces
    logged to the bound experiment are **silently dropped** (MLflow can't write to an MV). You
    also own `REFRESH`. Use only for read-only adoption of existing traces.
  - A **plain view** does **not** work — it can't carry the `otel.schemaVersion` table property,
    so MLflow won't adopt it.
- **Binding is permanent** per experiment (unset with
  `mlflow.tracing.unset_experiment_trace_location(...)`, or use a new experiment name).
- **events / links** default to empty arrays; preserving them needs a per-source
  reconstruction — see `SCHEMA_REFERENCE.md`.
- **`search_traces` 401 from a local/CLI token** is a client-auth quirk of the trace-read API,
  not a data problem. Run in-workspace, or verify via the `_unified` view (the notebook does
  both).
- Requires `mlflow[databricks] >= 3.14.0` (the V4 trace-location API) and a serverless SQL
  warehouse.

## Provenance

Recipe validated end-to-end in a Databricks workspace against MLflow-generated, PII-redacted, and
external (non-MLflow, OpenInference-convention) UC span tables, and with both a physical-table
copy and a read-only materialized view. The critical unlock — `otel.schemaVersion` is a settable
Delta table property — is what turns "you can't point an experiment at existing tables" into the
flow above.
