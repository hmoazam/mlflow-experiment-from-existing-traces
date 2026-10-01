# OTel span schema reference & conversion mapping

Databricks defines two UC trace-table schema versions, selected by the Delta table property
`otel.schemaVersion`. This repo always targets **v2** (the current schema). This file documents both
and how the notebook converts a source into v2.

## v2 — the target schema (`{prefix}_otel_spans`)

Current schema (Zerobus OTLP ingestion and MLflow's `set_experiment(trace_location=UnityCatalog(...))`
produce it). `attributes` and all nested attribute fields are **`VARIANT`**; four Databricks-specific
columns lead the table.

```sql
CREATE TABLE {prefix}_otel_spans (
  record_id STRING, time TIMESTAMP, date DATE, service_name STRING,   -- v2-only, synthesized
  trace_id STRING, span_id STRING, trace_state STRING, parent_span_id STRING,
  flags INT, name STRING, kind STRING,
  start_time_unix_nano BIGINT, end_time_unix_nano BIGINT,
  attributes VARIANT, dropped_attributes_count INT,
  events ARRAY<STRUCT<time_unix_nano:BIGINT, name:STRING, attributes:VARIANT, dropped_attributes_count:INT>>,
  dropped_events_count INT,
  links ARRAY<STRUCT<trace_id:STRING, span_id:STRING, trace_state:STRING, attributes:VARIANT, dropped_attributes_count:INT, flags:INT>>,
  dropped_links_count INT,
  status STRUCT<message:STRING, code:STRING>,
  resource STRUCT<attributes:VARIANT, dropped_attributes_count:INT>,
  resource_schema_url STRING,
  instrumentation_scope STRUCT<name:STRING, version:STRING, attributes:VARIANT, dropped_attributes_count:INT>,
  span_schema_url STRING
) USING DELTA CLUSTER BY (time, service_name, trace_id)
TBLPROPERTIES ('otel.schemaVersion' = 'v2');
```

MLflow also expects `{prefix}_otel_logs` and `{prefix}_otel_metrics` tables and, on binding, creates
`{prefix}_otel_annotations` plus the `{prefix}_trace_metadata` and `{prefix}_trace_unified` **views**.
Don't pre-create the views.

> The notebook doesn't hardcode this DDL — it calls `set_experiment(trace_location=UnityCatalog(...))`,
> which creates the empty v2 tables for your MLflow version, then populates them. Read column names
> with `DESCRIBE TABLE` (not `spark.table(...).schema`, which can't resolve `VARIANT` over Spark
> Connect), and stop at the clustering/metadata boundary so `CLUSTER BY` columns aren't double-counted.

## v1 — the legacy schema (`mlflow_experiment_trace_otel_spans`)

Older schema from `mlflow.tracing.set_experiment_trace_location`. Same columns as v2 **minus**
`record_id/time/date/service_name`, with `attributes` (and nested attrs) as **`MAP<STRING,STRING>`**,
and no annotations table (tags/assessments/metadata live as events in `_otel_logs`).

## Which API, which version

| | `set_experiment(trace_location=UnityCatalog(cat,schema,prefix))` | `set_experiment_trace_location(UCSchemaLocation(cat,schema))` |
|---|---|---|
| Schema | **v2** (VARIANT, `{prefix}_otel_*`) | v1 (MAP, `mlflow_experiment_trace_otel_*`) |
| Adopts a pre-existing **table** set? | **Yes** — conformant v2 tables are adopted in place | Yes — v1 tables |
| Adopts a **materialized view**? | **No** — rejected (`Failed to validate table compatibility`) | (v1 path; not used here) |

Use the v2 creation API. It adopts an existing conformant v2 table set *and* creates a new one when
absent — validated against pre-existing Zerobus-style tables.

## Converting a source into v2

### Standard v1 → v2: the official migrator

For a standard v1 table set (`mlflow_experiment_trace_otel_spans` + `_otel_logs`), use the supported
helper (`databricks-agents >= 1.10.1`), which does MAP→VARIANT, adds the four v2 columns, and
converts log-events → annotations:

```python
from databricks.migrations.v1_to_v2 import V1ToV2SqlMigration
V1ToV2SqlMigration(
    v1_source_schema="<cat>.<schema>",                 # expects .mlflow_experiment_trace_otel_spans/_logs
    v2_destination_prefix="<cat>.<schema>.<prefix>",   # the v2 tables created by set_experiment(...)
).run()
```

A standard-v1 table under a **non-standard name** can still use it: expose it under the expected names
via views in a scratch schema (no copy), then point the migrator at that schema —

```sql
CREATE VIEW scratch.mlflow_experiment_trace_otel_spans AS SELECT * FROM your.renamed_spans;
CREATE VIEW scratch.mlflow_experiment_trace_otel_logs  AS SELECT * FROM your.renamed_logs;  -- empty-compatible if none
```

### Arbitrary source → v2: generic reshape

For any other layout, map each v2 column from the source (defaulting missing ones) and convert
attributes to VARIANT:

| Target column | Source expression |
|---|---|
| `record_id` / `time` / `date` / `service_name` | `uuid()` · `timestamp_micros(start_time_unix_nano/1000)` · `to_date(time)` · `resource.attributes['service.name']` |
| `attributes` | `parse_json(s.attributes)` if JSON-string, else `parse_json(to_json(s.attributes))` (handles MAP / STRUCT / VARIANT) |
| `resource` / `instrumentation_scope` | `named_struct('attributes', <attrs→VARIANT>, 'dropped_attributes_count', …)` |
| `events` / `links` | `transform(...)` rebuilding each struct with nested `attributes` → VARIANT; else empty typed array |
| scalar cols | pass through, default if absent |

## `{prefix}_trace_unified` (what the Traces UI reads)

Built on binding. `request` / `response` are derived by `COALESCE`-ing over recognized attribute
keys (`mlflow.spanInputs/spanOutputs`, OpenInference `input.value`/`output.value`, OTel `gen_ai.*`,
`gcp.vertex.*`). Querying `SELECT count(*)` on it is an auth-independent way to confirm traces
assemble.
