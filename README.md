# Point an MLflow experiment at an existing trace table

Make OTel span data that **already exists in Unity Catalog** show up in an **MLflow experiment**
— the Traces UI, `mlflow.search_traces`, and MLflow GenAI evaluation — without regenerating it
from an instrumented app.

Works for any UC span table: MLflow-created "raw" traces, the redacted output of a PII pipeline
(e.g. [otel-pii-redaction](https://github.com/hmoazam/otel-pii-redaction)), or traces an
external / non-Databricks app exported into UC.

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

## Files

| File | Purpose |
|---|---|
| `bind_experiment_to_existing_traces.py` | Guided Databricks notebook — reshape, stamp, bind, verify |
| `SCHEMA_REFERENCE.md` | The exact target OTel span schema + reshape mapping (incl. events/links) |
| `README.md` | This file |

## Caveats

- **It's a copy, not a live view.** MLflow must own the physical tables, so traces are
  duplicated into the target. The notebook uses `INSERT OVERWRITE`, so re-running refreshes;
  schedule it if the source keeps growing. Binding an experiment to a plain view/MV over the
  source is not possible.
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

Recipe validated in a Databricks workspace against real redacted trace tables. The critical
unlock — `otel.schemaVersion` is a settable Delta table property — is what turns "you can't
point an experiment at existing tables" into the flow above.
