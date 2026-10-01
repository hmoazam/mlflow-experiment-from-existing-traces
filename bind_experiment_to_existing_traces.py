# Databricks notebook source
# MAGIC %md
# MAGIC # Point an MLflow experiment at existing OTel traces (always lands on schema **v2**)
# MAGIC
# MAGIC Given OTel span data that **already exists in a Unity Catalog Delta table**, surface it in an
# MAGIC **MLflow experiment** (Traces UI, `mlflow.search_traces`, evaluation). The table MLflow ends up
# MAGIC bound to is always the current **`otel.schemaVersion = v2`** schema (VARIANT `attributes`,
# MAGIC `{prefix}_otel_*` naming) — what Zerobus OTLP ingestion and MLflow's creation API produce.
# MAGIC
# MAGIC Databricks defines exactly two UC trace-table schema versions:
# MAGIC **v1** (legacy, MAP attributes, fixed `mlflow_experiment_trace_otel_*` names) and
# MAGIC **v2** (current, VARIANT attributes, `{prefix}_otel_*` names, dedicated annotations table).
# MAGIC This notebook auto-detects your source and routes to the right handler:
# MAGIC
# MAGIC | Detected source | Path | How it reaches v2 |
# MAGIC |---|---|---|
# MAGIC | **Conformant v2** (`otel.schemaVersion=v2`, VARIANT attrs, `{p}_otel_spans` + sibling logs/metrics) | **A — adopt in place** | `set_experiment(trace_location=UnityCatalog(...))` on the source schema. No copy. |
# MAGIC | **Standard v1** (`mlflow_experiment_trace_otel_spans` + `_otel_logs`) | **B — official migrator** | Create a v2 destination, then `V1ToV2SqlMigration(...).run()` (MAP→VARIANT, log-events→annotations). |
# MAGIC | **Anything else** (renamed v1, external exports, non-standard layouts) | **C — generic reshape** | Reshape into a fresh v2 table, then adopt it. |
# MAGIC
# MAGIC > **Why not a view / materialized view for the destination?** The v2 creation-time adoption
# MAGIC > API validates the spans object is a physical table and **rejects a materialized view**
# MAGIC > (`Failed to validate table compatibility`). So the convert paths (B, C) always land in a
# MAGIC > physical table. Only an *already-v2 table set* can be adopted without a copy (Path A).
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - `mlflow[databricks] >= 3.14.0`; `databricks-agents >= 1.10.1` (Path B only)
# MAGIC - A serverless SQL warehouse ID; `MODIFY`+`SELECT`+`CREATE TABLE` on the target schema
# MAGIC - Run **in the Databricks workspace**

# COMMAND ----------

# MAGIC %pip install "mlflow[databricks]>=3.14.0" "databricks-agents>=1.10.1"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("source_spans_table", "", "1. Source spans table (catalog.schema.table)")
dbutils.widgets.text("target_catalog", "", "2. Target catalog (convert paths B/C)")
dbutils.widgets.text("target_schema", "", "3. Target schema (created if absent)")
dbutils.widgets.text("target_table_prefix", "traces", "4. Target table prefix ({prefix}_otel_spans)")
dbutils.widgets.text("experiment_name", "", "5. MLflow experiment name (/Users/you/name)")
dbutils.widgets.text("sql_warehouse_id", "", "6. Serverless SQL warehouse ID")

# COMMAND ----------

import re

source_spans_table = dbutils.widgets.get("source_spans_table").strip()
target_catalog = dbutils.widgets.get("target_catalog").strip()
target_schema = dbutils.widgets.get("target_schema").strip()
target_prefix = dbutils.widgets.get("target_table_prefix").strip() or "traces"
experiment_name = dbutils.widgets.get("experiment_name").strip()
sql_warehouse_id = dbutils.widgets.get("sql_warehouse_id").strip()

_FQN = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")
_ID = re.compile(r"^[A-Za-z0-9_]+$")


def _fail(msg):
    dbutils.notebook.exit(f"FAILED: {msg}")


if not _FQN.match(source_spans_table):
    _fail("Source spans table must be fully qualified: catalog.schema.table")
if not experiment_name:
    _fail("Experiment name is required")
if not sql_warehouse_id:
    _fail("SQL warehouse ID is required")

src_catalog, src_schema, src_table = source_spans_table.split(".")
src_schema_fqn = f"{src_catalog}.{src_schema}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Init MLflow

# COMMAND ----------

import os
import mlflow
from mlflow.entities.trace_location import UnityCatalog

mlflow.set_tracking_uri("databricks")
os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = sql_warehouse_id
assert mlflow.__version__ >= "3.14", f"MLflow {mlflow.__version__} too old; need >= 3.14.0."
print(f"MLflow {mlflow.__version__} ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Detect the source schema version and choose a path

# COMMAND ----------

def _tblprop(fqn, key):
    try:
        rows = spark.sql(f"SHOW TBLPROPERTIES {fqn} ('{key}')").collect()
        return rows[0].value if rows else None
    except Exception:
        return None


def _exists(fqn):
    try:
        spark.sql(f"DESCRIBE TABLE {fqn}").collect()
        return True
    except Exception:
        return False


def _describe_cols(fqn):
    """[(name, type_str)] via DESCRIBE — avoids Spark Connect's inability to resolve VARIANT in
    .schema/.columns/.dtypes, and stops at the first metadata/clustering boundary so CLUSTER BY
    columns aren't counted twice."""
    out = []
    for r in spark.sql(f"DESCRIBE TABLE {fqn}").collect():
        c = r.col_name
        if not c or c.startswith("#"):
            break
        out.append((c, (r.data_type or "")))
    return out


src_types = dict(_describe_cols(source_spans_table))
src_cols = set(src_types)
attrs_type = (src_types.get("attributes", "") or "").lower()
schema_version = _tblprop(source_spans_table, "otel.schemaVersion")

is_variant = attrs_type.startswith("variant")
is_map = attrs_type.startswith("map")
has_v2_cols = {"record_id", "time", "date", "service_name"} <= src_cols
named_spans = src_table.endswith("_otel_spans")
src_prefix = src_table[: -len("_otel_spans")] if named_spans else None

# Path A: a conformant, in-place-adoptable v2 table set
path = None
if named_spans and (schema_version == "v2" or (is_variant and has_v2_cols)):
    sib = [f"{src_schema_fqn}.{src_prefix}_otel_{s}" for s in ("logs", "metrics")]
    if all(_exists(s) for s in sib):
        path = "A"
# Path B: standard v1 schema-linked table set (what V1ToV2SqlMigration expects by name)
if path is None and src_table == "mlflow_experiment_trace_otel_spans" \
        and _exists(f"{src_schema_fqn}.mlflow_experiment_trace_otel_logs"):
    path = "B"
# Path C: everything else
if path is None:
    path = "C"

if path != "A":
    for v, lbl in [(target_catalog, "Target catalog"), (target_schema, "Target schema"),
                   (target_prefix, "Target table prefix")]:
        if not _ID.match(v):
            _fail(f"{lbl} must be a bare identifier (required for convert paths B/C)")

print(f"source otel.schemaVersion={schema_version!r} attributes={attrs_type!r} "
      f"has_v2_cols={has_v2_cols} named_spans={named_spans}")
print({"A": "PATH A — adopt the conformant v2 source in place (no copy)",
       "B": "PATH B — official V1ToV2SqlMigration (standard v1 -> v2)",
       "C": "PATH C — generic reshape into a fresh v2 table"}[path])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Create the experiment and the v2 destination
# MAGIC
# MAGIC All paths bind through the creation-time API, which creates the empty `{prefix}_otel_*` v2
# MAGIC tables (for B/C) or adopts the existing ones (A), and builds the `_trace_metadata` /
# MAGIC `_trace_unified` views.

# COMMAND ----------

if path == "A":
    bind_catalog, bind_schema, bind_prefix = src_catalog, src_schema, src_prefix
else:
    bind_catalog, bind_schema, bind_prefix = target_catalog, target_schema, target_prefix
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bind_catalog}.{bind_schema}")

experiment = mlflow.set_experiment(
    experiment_name=experiment_name,
    trace_location=UnityCatalog(catalog_name=bind_catalog, schema_name=bind_schema,
                                table_prefix=bind_prefix),
)
experiment_id = experiment.experiment_id
spans_fqn = experiment.trace_location.full_otel_spans_table_name
print(f"Experiment {experiment_id} bound to {spans_fqn} "
      f"(otel.schemaVersion={_tblprop(spans_fqn, 'otel.schemaVersion')})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Populate the v2 destination (B and C only)

# COMMAND ----------

if path == "B":
    # Official helper: MAP->VARIANT spans + log-events->annotations. Names are inferred as
    # {schema}.mlflow_experiment_trace_otel_{spans,logs}, which this source matches.
    from databricks.migrations.v1_to_v2 import V1ToV2SqlMigration
    V1ToV2SqlMigration(
        v1_source_schema=src_schema_fqn,
        v2_destination_prefix=f"{bind_catalog}.{bind_schema}.{bind_prefix}",
    ).run()
    print("V1ToV2SqlMigration complete.")

elif path == "C":
    # Generic reshape of an arbitrary source into the v2 span schema MLflow just created.
    def col_or(name, default_sql):
        return f"s.{name}" if name in src_cols else default_sql

    def to_variant(expr, coltype):
        # JSON-string -> parse directly; MAP/STRUCT/VARIANT -> round-trip via to_json
        return f"parse_json({expr})" if (coltype or "").lower().startswith("string") \
            else f"parse_json(to_json({expr}))"

    def nested_is_string(container_type):
        return bool(re.search(r"attributes:\s*string", (container_type or "").lower()))

    time_expr = ("timestamp_micros(CAST(s.start_time_unix_nano/1000 AS BIGINT))"
                 if "start_time_unix_nano" in src_cols else "current_timestamp()")
    svc_expr = ("COALESCE(element_at(s.resource.attributes,'service.name'),'')"
                if ("resource" in src_cols and "map" in (src_types.get("resource", "")).lower())
                else "''")
    attrs_expr = to_variant("s.attributes", attrs_type) if "attributes" in src_cols else "parse_json('{}')"

    res_t = src_types.get("resource", "")
    resource_expr = ("named_struct('attributes', "
                     + to_variant("s.resource.attributes", "string" if nested_is_string(res_t) else "map")
                     + ", 'dropped_attributes_count', s.resource.dropped_attributes_count)"
                     if "resource" in src_cols else
                     "named_struct('attributes', parse_json('{}'), 'dropped_attributes_count', 0)")

    is_t = src_types.get("instrumentation_scope", "")
    iscope_expr = ("named_struct('name', s.instrumentation_scope.name, 'version', "
                   "s.instrumentation_scope.version, 'attributes', "
                   + to_variant("s.instrumentation_scope.attributes", "string" if nested_is_string(is_t) else "map")
                   + ", 'dropped_attributes_count', s.instrumentation_scope.dropped_attributes_count)"
                   if "instrumentation_scope" in src_cols else
                   "named_struct('name','','version','','attributes',parse_json('{}'),'dropped_attributes_count',0)")

    EMPTY_EVENTS = ("CAST(array() AS ARRAY<STRUCT<time_unix_nano:BIGINT,name:STRING,"
                    "attributes:VARIANT,dropped_attributes_count:INT>>)")
    EMPTY_LINKS = ("CAST(array() AS ARRAY<STRUCT<trace_id:STRING,span_id:STRING,trace_state:STRING,"
                   "attributes:VARIANT,dropped_attributes_count:INT,flags:INT>>)")
    if "events" in src_cols and "array" in (src_types.get("events", "")).lower():
        n = "string" if nested_is_string(src_types["events"]) else "map"
        events_expr = ("transform(s.events, e -> named_struct('time_unix_nano', e.time_unix_nano, "
                       "'name', e.name, 'attributes', " + to_variant("e.attributes", n)
                       + ", 'dropped_attributes_count', e.dropped_attributes_count))")
    else:
        events_expr = EMPTY_EVENTS
    if "links" in src_cols and "array" in (src_types.get("links", "")).lower():
        n = "string" if nested_is_string(src_types["links"]) else "map"
        links_expr = ("transform(s.links, l -> named_struct('trace_id', l.trace_id, 'span_id', l.span_id, "
                      "'trace_state', l.trace_state, 'attributes', " + to_variant("l.attributes", n)
                      + ", 'dropped_attributes_count', l.dropped_attributes_count, 'flags', l.flags))")
    else:
        links_expr = EMPTY_LINKS

    status_expr = ("named_struct('message', s.status.message, 'code', s.status.code)"
                   if "status" in src_cols else
                   "named_struct('message', CAST(NULL AS STRING), 'code', 'STATUS_CODE_UNSET')")

    expr = {
        "record_id": "uuid()", "time": time_expr, "date": f"to_date({time_expr})",
        "service_name": svc_expr,
        "trace_id": col_or("trace_id", "CAST(NULL AS STRING)"),
        "span_id": col_or("span_id", "CAST(NULL AS STRING)"),
        "trace_state": col_or("trace_state", "''"),
        "parent_span_id": col_or("parent_span_id", "CAST(NULL AS STRING)"),
        "flags": col_or("flags", "0"), "name": col_or("name", "''"),
        "kind": col_or("kind", "'SPAN_KIND_INTERNAL'"),
        "start_time_unix_nano": col_or("start_time_unix_nano", "0"),
        "end_time_unix_nano": col_or("end_time_unix_nano", "0"),
        "attributes": attrs_expr, "dropped_attributes_count": col_or("dropped_attributes_count", "0"),
        "events": events_expr, "dropped_events_count": col_or("dropped_events_count", "0"),
        "links": links_expr, "dropped_links_count": col_or("dropped_links_count", "0"),
        "status": status_expr, "resource": resource_expr,
        "resource_schema_url": col_or("resource_schema_url", "''"),
        "instrumentation_scope": iscope_expr, "span_schema_url": col_or("span_schema_url", "''"),
    }
    # target column order from the table MLflow created (DESCRIBE-based: VARIANT-safe on Connect)
    target_cols = [c for c, _ in _describe_cols(spans_fqn)]
    missing = [c for c in target_cols if c not in expr]
    if missing:
        _fail(f"v2 schema has columns this notebook doesn't map: {missing}")
    select_sql = ",\n  ".join(f"{expr[c]} AS {c}" for c in target_cols)
    reshape_sql = f"INSERT OVERWRITE {spans_fqn}\nSELECT\n  {select_sql}\nFROM {source_spans_table} s"
    print(reshape_sql)
    spark.sql(reshape_sql)
    _n = spark.sql(f"SELECT count(*) c FROM {spans_fqn}").first().c
    print(f"Reshaped {_n} spans into {spans_fqn} (v2 / VARIANT).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Verify

# COMMAND ----------

unified = f"{bind_catalog}.{bind_schema}.{bind_prefix}_trace_unified"
print(f"{unified}: {spark.sql(f'SELECT count(*) c FROM {unified}').first().c} traces assembled.")
display(spark.sql(f"SELECT trace_id, request, response FROM {unified} LIMIT 5"))

try:
    df = mlflow.search_traces(experiment_ids=[experiment_id], max_results=10)
    print(f"mlflow.search_traces returned {len(df)} traces.")
except Exception as e:
    print(f"search_traces note: {e}")

host = spark.conf.get("spark.databricks.workspaceUrl", "")
if host:
    print(f"\nTraces UI: https://{host}/ml/experiments/{experiment_id}?compareRunsMode=TRACES")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes
# MAGIC - **Destination is always v2** (VARIANT). Path A adopts in place (no copy); B/C write a v2
# MAGIC   physical table (re-run to refresh — B's migrator and C's `INSERT OVERWRITE` are idempotent).
# MAGIC - **A standard v1 table under a non-standard name** can still use the official migrator by
# MAGIC   aliasing it to `mlflow_experiment_trace_otel_spans`/`_otel_logs` via views in a scratch
# MAGIC   schema and pointing Path B at that schema; otherwise it falls to Path C (spans only, no
# MAGIC   annotations).
# MAGIC - **request/response** populate when span attributes use a recognized key
# MAGIC   (`mlflow.spanInputs/spanOutputs`, OpenInference `input.value`/`output.value`, OTel
# MAGIC   `gen_ai.*`, `gcp.vertex.*`); otherwise alias custom keys in Step 4's reshape.
# MAGIC - **Materialized views cannot be a v2 destination** — the adoption API rejects them.
