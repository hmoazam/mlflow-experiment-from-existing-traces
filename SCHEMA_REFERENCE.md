# OTel span schema reference & reshape mapping

The MLflow trace backend accepts only tables that match its OTel span schema **and** carry the
Delta table property `otel.schemaVersion = v1`. This file documents that target schema (as
created by MLflow 3.14 via `mlflow.tracing.set_experiment_trace_location`) and how to reshape a
source span table into it.

> The notebook reads this schema from your workspace at runtime (Step 2), so it always matches
> your MLflow version. This file is for understanding and manual adaptation.

## Target: `mlflow_experiment_trace_otel_spans` (20 columns)

```sql
CREATE TABLE mlflow_experiment_trace_otel_spans (
  trace_id                  STRING,
  span_id                   STRING,
  trace_state               STRING,
  parent_span_id            STRING,
  flags                     INT,
  name                      STRING,
  kind                      STRING,
  start_time_unix_nano      BIGINT,
  end_time_unix_nano        BIGINT,
  attributes                MAP<STRING, STRING>,
  dropped_attributes_count  INT,
  events                    ARRAY<STRUCT<time_unix_nano: BIGINT, name: STRING,
                                         attributes: MAP<STRING, STRING>,
                                         dropped_attributes_count: INT>>,
  dropped_events_count      INT,
  links                     ARRAY<STRUCT<trace_id: STRING, span_id: STRING, trace_state: STRING,
                                         attributes: MAP<STRING, STRING>,
                                         dropped_attributes_count: INT, flags: INT>>,
  dropped_links_count       INT,
  status                    STRUCT<message: STRING, code: STRING>,
  resource                  STRUCT<attributes: MAP<STRING, STRING>, dropped_attributes_count: INT>,
  resource_schema_url       STRING,
  instrumentation_scope     STRUCT<name: STRING, version: STRING,
                                   attributes: MAP<STRING, STRING>, dropped_attributes_count: INT>,
  span_schema_url           STRING
) USING delta
TBLPROPERTIES ('otel.schemaVersion' = 'v1');
```

MLflow also expects (and, on binding, creates) `mlflow_experiment_trace_otel_logs` and
`mlflow_experiment_trace_otel_metrics` physical tables, plus `mlflow_experiment_trace_metadata`
and `mlflow_experiment_trace_unified` **views**. Create the three tables; let MLflow build the
two views.

## Two different MLflow trace schemas — pick the right one

| | `set_experiment(trace_location=UnityCatalog(...))` (V5) | `set_experiment_trace_location(UCSchemaLocation(...))` (V4) |
|---|---|---|
| Table names | `{prefix}_otel_spans` | `mlflow_experiment_trace_otel_spans` (or custom via `_otel_spans_table_name`) |
| `attributes` type | `VARIANT` | **`MAP<STRING,STRING>`** |
| Extra columns | `record_id`, `time`, `date`, `service_name` | none |
| Adopts pre-existing tables? | **No** — fails "Telemetry profile not found" even when stamped | **Yes** — with `otel.schemaVersion=v1` |

**Use the V4 path.** It is the only one that adopts existing tables. The reshape below targets
the V4 MAP schema.

## Reshape mapping (source → V4 target)

For a source in the MLflow "raw" shape or a PII-redaction output (VARIANT/JSON attributes):

| Target column | Source expression |
|---|---|
| scalar cols (`trace_id`, `span_id`, `flags`, timestamps, `name`, `kind`, `*_count`, `*_schema_url`, `trace_state`, `parent_span_id`) | pass through (`s.<col>`), default if absent |
| `attributes` | `from_json(CAST(s.attributes AS STRING), 'MAP<STRING,STRING>')` |
| `resource` | `named_struct('attributes', from_json(CAST(s.resource.attributes AS STRING),'MAP<STRING,STRING>'), 'dropped_attributes_count', s.resource.dropped_attributes_count)` |
| `instrumentation_scope` | `named_struct('name', s.instrumentation_scope.name, 'version', s.instrumentation_scope.version, 'attributes', from_json(CAST(s.instrumentation_scope.attributes AS STRING),'MAP<STRING,STRING>'), 'dropped_attributes_count', s.instrumentation_scope.dropped_attributes_count)` |
| `status` | `named_struct('message', s.status.message, 'code', s.status.code)` |
| `events` | empty typed array by default; reconstruct if needed (see below) |
| `links` | empty typed array by default; reconstruct if needed |

`CAST(... AS STRING)` before `from_json` works whether the source column is `VARIANT` or a JSON
`STRING`. If the source is already `MAP<STRING,STRING>`, pass it through unchanged.

### Preserving events / links

The default drops `events`/`links` to empty arrays because reconstructing `ARRAY<STRUCT<...>>`
(whose nested `attributes` must also become `MAP<STRING,STRING>`) is source-specific. To
preserve them, replace the empty-array expressions with a `transform(...)` that rebuilds each
struct, converting the nested `attributes` to a map. Example sketch for events:

```sql
transform(
  CAST(s.events AS ARRAY<STRUCT<time_unix_nano:BIGINT, name:STRING, attributes:STRING,
                                dropped_attributes_count:INT>>),
  e -> named_struct(
         'time_unix_nano', e.time_unix_nano,
         'name', e.name,
         'attributes', from_json(e.attributes, 'MAP<STRING,STRING>'),
         'dropped_attributes_count', e.dropped_attributes_count)
) AS events
```

(The exact `CAST` target must match your source's events layout — inspect it with
`DESCRIBE TABLE`.)

## `mlflow_experiment_trace_unified` (what the Traces UI reads)

The binding step creates this view over the spans/annotations. Its columns:
`trace_id, client_request_id, request_time, state, execution_duration_ms, request, response,
trace_metadata, tags, spans, assessments`. Querying it (`SELECT count(*)`) is a reliable,
auth-independent way to confirm traces assemble correctly.
