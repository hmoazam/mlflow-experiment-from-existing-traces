# Databricks notebook source
# MAGIC %md
# MAGIC # Point an MLflow experiment at an existing trace table
# MAGIC
# MAGIC Given an **existing** OTel span table in Unity Catalog — MLflow-created raw traces, the
# MAGIC redacted output of a PII pipeline, or traces an external app exported into UC — this
# MAGIC notebook makes those traces show up natively in an **MLflow experiment** (Traces UI,
# MAGIC `mlflow.search_traces`, evaluation, etc.).
# MAGIC
# MAGIC ### Why this is not a one-liner
# MAGIC
# MAGIC You **cannot** point an experiment at a pre-existing table just by naming it. MLflow's
# MAGIC trace backend only accepts tables it recognizes as its own — it checks a Delta table
# MAGIC property, `otel.schemaVersion`, and rejects anything stamped `UNSPECIFIED` with
# MAGIC `ALREADY_EXISTS: ... expected v1, got UNSPECIFIED`. The trick that makes this work:
# MAGIC
# MAGIC 1. **Reshape** your source into MLflow's exact OTel span schema (`attributes` as
# MAGIC    `MAP<STRING,STRING>`, specific column names/order — see `SCHEMA_REFERENCE.md`).
# MAGIC 2. **Stamp** `ALTER TABLE ... SET TBLPROPERTIES ('otel.schemaVersion' = 'v1')`.
# MAGIC 3. **Bind** with `mlflow.tracing.set_experiment_trace_location(...)` — MLflow then
# MAGIC    *adopts* your tables and builds its own `_metadata` / `_unified` views on top.
# MAGIC
# MAGIC > **Use the V4 API, not the creation-time one.** `set_experiment(trace_location=UnityCatalog(...))`
# MAGIC > does **not** adopt pre-existing tables even when stamped (it fails server-side with
# MAGIC > "Telemetry profile not found"). Only `mlflow.tracing.set_experiment_trace_location`
# MAGIC > (shown here) adopts them.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - `mlflow[databricks] >= 3.14.0`
# MAGIC - A serverless SQL warehouse ID
# MAGIC - An existing OTel span table, and `MODIFY` + `SELECT` on the target schema
# MAGIC - Run this **in the Databricks workspace** (in-workspace user creds; avoids a local-auth
# MAGIC   quirk on the trace-read API — see the closing note)

# COMMAND ----------

# MAGIC %pip install "mlflow[databricks]>=3.14.0"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# --- Source: the existing span table you want to surface ---
dbutils.widgets.text("source_spans_table", "", "1. Source spans table (catalog.schema.table)")
dbutils.widgets.text("source_logs_table", "", "2. Source logs table (optional, blank = none)")

# --- Target: where MLflow's adopted tables will live ---
dbutils.widgets.text("target_catalog", "", "3. Target catalog")
dbutils.widgets.text("target_schema", "", "4. Target schema (created if absent)")

# --- Experiment + compute ---
dbutils.widgets.text("experiment_name", "", "5. MLflow experiment name (/Users/you/name)")
dbutils.widgets.text("sql_warehouse_id", "", "6. Serverless SQL warehouse ID")

# COMMAND ----------

import re

source_spans_table = dbutils.widgets.get("source_spans_table").strip()
source_logs_table = dbutils.widgets.get("source_logs_table").strip()
target_catalog = dbutils.widgets.get("target_catalog").strip()
target_schema = dbutils.widgets.get("target_schema").strip()
experiment_name = dbutils.widgets.get("experiment_name").strip()
sql_warehouse_id = dbutils.widgets.get("sql_warehouse_id").strip()

_FQN = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")
_ID = re.compile(r"^[A-Za-z0-9_]+$")


def _fail(msg):
    dbutils.notebook.exit(f"FAILED: {msg}")


if not _FQN.match(source_spans_table):
    _fail("Source spans table must be fully qualified: catalog.schema.table")
if source_logs_table and not _FQN.match(source_logs_table):
    _fail("Source logs table, if set, must be fully qualified: catalog.schema.table")
for v, lbl in [(target_catalog, "Target catalog"), (target_schema, "Target schema")]:
    if not _ID.match(v):
        _fail(f"{lbl} must be a bare identifier")
if not experiment_name:
    _fail("Experiment name is required")
if not sql_warehouse_id:
    _fail("SQL warehouse ID is required")

# MLflow's V4 default physical table names inside the bound schema.
TGT = f"{target_catalog}.{target_schema}"
T_SPANS = f"{TGT}.mlflow_experiment_trace_otel_spans"
T_LOGS = f"{TGT}.mlflow_experiment_trace_otel_logs"
T_METRICS = f"{TGT}.mlflow_experiment_trace_otel_metrics"

print("=== Configuration ===")
print(f"  Source spans:  {source_spans_table}")
print(f"  Source logs:   {source_logs_table or '(none — empty logs table will be created)'}")
print(f"  Target schema: {TGT}")
print(f"  Experiment:    {experiment_name}")
print(f"  Warehouse:     {sql_warehouse_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Init MLflow, verify version

# COMMAND ----------

import os
import mlflow
from mlflow.entities import UCSchemaLocation
from mlflow.tracing import set_experiment_trace_location

mlflow.set_tracking_uri("databricks")
os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = sql_warehouse_id

assert mlflow.__version__ >= "3.14", (
    f"MLflow {mlflow.__version__} too old; need >= 3.14.0. Re-run the %pip cell."
)
print(f"MLflow {mlflow.__version__} ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Capture MLflow's exact target schema (throwaway seed)
# MAGIC
# MAGIC Rather than hard-code DDL that could drift across MLflow versions, we let MLflow tell us
# MAGIC the schema: bind a throwaway experiment to a scratch schema, copy the empty table shapes
# MAGIC it creates, then drop the scratch. This guarantees the target tables match this
# MAGIC workspace's MLflow version exactly.

# COMMAND ----------

import uuid

seed_schema = f"_bind_seed_{uuid.uuid4().hex[:8]}"
seed_fqn = f"{target_catalog}.{seed_schema}"
seed_exp_name = f"{experiment_name}__seed_{uuid.uuid4().hex[:6]}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {seed_fqn}")
_seed_exp = mlflow.set_experiment(experiment_name=seed_exp_name)
set_experiment_trace_location(
    location=UCSchemaLocation(catalog_name=target_catalog, schema_name=seed_schema),
    experiment_id=_seed_exp.experiment_id,
    sql_warehouse_id=sql_warehouse_id,
)

# Build the target tables as empty copies of the seed's physical tables, then stamp v1.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TGT}")
for tgt, name in [
    (T_SPANS, "mlflow_experiment_trace_otel_spans"),
    (T_LOGS, "mlflow_experiment_trace_otel_logs"),
    (T_METRICS, "mlflow_experiment_trace_otel_metrics"),
]:
    spark.sql(f"CREATE OR REPLACE TABLE {tgt} AS SELECT * FROM {seed_fqn}.{name} WHERE 1=0")
    spark.sql(f"ALTER TABLE {tgt} SET TBLPROPERTIES ('otel.schemaVersion' = 'v1')")

# Record the exact target spans column order for the reshape INSERT.
target_span_cols = [r.col_name for r in spark.sql(f"DESCRIBE TABLE {T_SPANS}").collect()
                    if r.col_name and not r.col_name.startswith("#")]
print("Target spans columns:", target_span_cols)

# Drop the scratch seed (do NOT drop the target).
spark.sql(f"DROP SCHEMA IF EXISTS {seed_fqn} CASCADE")
try:
    _e = mlflow.get_experiment_by_name(seed_exp_name)
    if _e:
        mlflow.delete_experiment(_e.experiment_id)
except Exception as e:
    print(f"(seed experiment cleanup warning: {e})")
print("Empty target tables created + stamped otel.schemaVersion=v1; seed dropped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Reshape the source into the OTel span schema
# MAGIC
# MAGIC The tricky part is `attributes` (and the nested `attributes` inside `resource` and
# MAGIC `instrumentation_scope`): the target wants `MAP<STRING,STRING>`. We detect the source
# MAGIC column type and convert accordingly — VARIANT or JSON-STRING → MAP, existing MAP as-is.
# MAGIC
# MAGIC **`events` and `links` default to empty arrays** (see the design note at the bottom):
# MAGIC reconstructing them from an arbitrary source is source-specific. If your spans carry
# MAGIC events/links you need to preserve, edit the two marked expressions.

# COMMAND ----------

# Inspect source columns + types.
src_desc = {r.col_name: r.data_type for r in spark.sql(f"DESCRIBE TABLE {source_spans_table}").collect()
            if r.col_name and not r.col_name.startswith("#")}
src_cols = set(src_desc)


def col_or(name, default_sql):
    """Reference source column if present, else a default literal."""
    return f"s.{name}" if name in src_cols else default_sql


def to_map(expr, coltype):
    """Convert a source attributes-like column to MAP<STRING,STRING>."""
    if coltype and coltype.lower().startswith("map"):
        return expr                              # already a MAP
    # VARIANT or STRING(JSON): CAST to STRING yields JSON text, then parse to MAP.
    return f"from_json(CAST({expr} AS STRING), 'MAP<STRING,STRING>')"


attrs_type = src_desc.get("attributes", "")
# Nested attribute columns may be typed as struct fields; we CAST-then-parse defensively.
EMPTY_EVENTS = ("CAST(array() AS ARRAY<STRUCT<time_unix_nano:BIGINT,name:STRING,"
                "attributes:MAP<STRING,STRING>,dropped_attributes_count:INT>>)")
EMPTY_LINKS = ("CAST(array() AS ARRAY<STRUCT<trace_id:STRING,span_id:STRING,trace_state:STRING,"
               "attributes:MAP<STRING,STRING>,dropped_attributes_count:INT,flags:INT>>)")

# One SELECT expression per target column, in the exact target order.
expr = {
    "trace_id": col_or("trace_id", "CAST(NULL AS STRING)"),
    "span_id": col_or("span_id", "CAST(NULL AS STRING)"),
    "trace_state": col_or("trace_state", "''"),
    "parent_span_id": col_or("parent_span_id", "CAST(NULL AS STRING)"),
    "flags": col_or("flags", "0"),
    "name": col_or("name", "''"),
    "kind": col_or("kind", "'SPAN_KIND_INTERNAL'"),
    "start_time_unix_nano": col_or("start_time_unix_nano", "0"),
    "end_time_unix_nano": col_or("end_time_unix_nano", "0"),
    "attributes": to_map("s.attributes", attrs_type) if "attributes" in src_cols else "map()",
    "dropped_attributes_count": col_or("dropped_attributes_count", "0"),
    "events": EMPTY_EVENTS,   # <-- EDIT if your source has events to preserve
    "dropped_events_count": col_or("dropped_events_count", "0"),
    "links": EMPTY_LINKS,     # <-- EDIT if your source has links to preserve
    "dropped_links_count": col_or("dropped_links_count", "0"),
    "status": ("named_struct('message', s.status.message, 'code', s.status.code)"
               if "status" in src_cols else
               "named_struct('message', CAST(NULL AS STRING), 'code', 'STATUS_CODE_UNSET')"),
    "resource": ("named_struct('attributes', "
                 + to_map("s.resource.attributes", "") +
                 ", 'dropped_attributes_count', s.resource.dropped_attributes_count)"
                 if "resource" in src_cols else
                 "named_struct('attributes', map(), 'dropped_attributes_count', 0)"),
    "resource_schema_url": col_or("resource_schema_url", "''"),
    "instrumentation_scope": (
        "named_struct('name', s.instrumentation_scope.name, 'version', s.instrumentation_scope.version, "
        "'attributes', " + to_map("s.instrumentation_scope.attributes", "") +
        ", 'dropped_attributes_count', s.instrumentation_scope.dropped_attributes_count)"
        if "instrumentation_scope" in src_cols else
        "named_struct('name', '', 'version', '', 'attributes', map(), 'dropped_attributes_count', 0)"),
    "span_schema_url": col_or("span_schema_url", "''"),
}

missing = [c for c in target_span_cols if c not in expr]
if missing:
    _fail(f"Target has columns this notebook doesn't map: {missing}. "
          "Your MLflow version's schema differs — add expressions for these in Step 3.")

select_sql = ",\n  ".join(f"{expr[c]} AS {c}" for c in target_span_cols)
reshape_sql = (
    f"INSERT OVERWRITE {T_SPANS}\n"
    f"SELECT\n  {select_sql}\nFROM {source_spans_table} s"
)
print(reshape_sql)

# COMMAND ----------

# DBTITLE 1,Warn if events/links would be dropped
for arr_col in ("events", "links"):
    if arr_col in src_cols:
        try:
            n = spark.sql(
                f"SELECT count(*) c FROM {source_spans_table} "
                f"WHERE {arr_col} IS NOT NULL AND size(CAST({arr_col} AS ARRAY<STRING>)) > 0"
            ).first().c
        except Exception:
            n = None
        if n:
            print(f"WARNING: {n} source rows have non-empty '{arr_col}'. These are dropped "
                  f"unless you edit the '{arr_col}' expression in Step 3.")

# COMMAND ----------

# DBTITLE 1,Execute the reshape
spark.sql(reshape_sql)
n_spans = spark.sql(f"SELECT count(*) c FROM {T_SPANS}").first().c
print(f"Reshaped {n_spans} spans into {T_SPANS}.")

# Optional: reshape logs the same way is left as an exercise; empty logs is fine for the UI.
if source_logs_table:
    print(f"(Source logs table given: {source_logs_table}. This template leaves logs empty — "
          "add a logs reshape mirroring Step 3 if you need them.)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Create the experiment and bind (adopt the tables)

# COMMAND ----------

existing = mlflow.get_experiment_by_name(experiment_name)
if existing is not None:
    experiment_id = existing.experiment_id
    already_bound = any(
        k.startswith("mlflow.experiment.databricksTrace") for k in (existing.tags or {})
    )
    print(f"Experiment exists ({experiment_id}); trace location "
          f"{'already bound — data refreshed in place' if already_bound else 'not bound — binding now'}.")
    if not already_bound:
        set_experiment_trace_location(
            location=UCSchemaLocation(catalog_name=target_catalog, schema_name=target_schema),
            experiment_id=experiment_id,
            sql_warehouse_id=sql_warehouse_id,
        )
else:
    experiment_id = mlflow.set_experiment(experiment_name=experiment_name).experiment_id
    set_experiment_trace_location(
        location=UCSchemaLocation(catalog_name=target_catalog, schema_name=target_schema),
        experiment_id=experiment_id,
        sql_warehouse_id=sql_warehouse_id,
    )
    print(f"Created + bound experiment {experiment_id}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Verify

# COMMAND ----------

# The _unified view is what the Traces UI reads; querying it confirms traces assemble.
unified = f"{TGT}.mlflow_experiment_trace_unified"
n_unified = spark.sql(f"SELECT count(*) c FROM {unified}").first().c
print(f"{unified}: {n_unified} traces assembled.")
display(spark.sql(f"SELECT trace_id, request, response FROM {unified} LIMIT 5"))

# search_traces works with in-workspace user creds.
try:
    df = mlflow.search_traces(experiment_ids=[experiment_id], max_results=10)
    print(f"mlflow.search_traces returned {len(df)} traces.")
except Exception as e:
    print(f"search_traces note: {e}\n(If this 401s from a local/CLI token, run in-workspace or "
          "verify via the _unified view above and the Traces UI.)")

host = spark.conf.get("spark.databricks.workspaceUrl", "")
if host:
    print(f"\nTraces UI: https://{host}/ml/experiments/{experiment_id}?compareRunsMode=TRACES")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Design notes & caveats
# MAGIC
# MAGIC - **This is a copy, not a live view.** MLflow requires it to own the physical tables, so
# MAGIC   the traces are duplicated into the target. Re-run this notebook (Step 3 uses
# MAGIC   `INSERT OVERWRITE`) to refresh; or schedule it. You cannot bind an experiment to a
# MAGIC   plain view/MV over your source.
# MAGIC - **Binding is permanent.** An experiment's trace location is fixed once set; to change
# MAGIC   it, use `mlflow.tracing.unset_experiment_trace_location(...)` first, or use a new
# MAGIC   experiment name.
# MAGIC - **events / links** are dropped to empty arrays by default. To preserve them, edit the
# MAGIC   two marked expressions in Step 3 (each nested `attributes` must become `MAP<STRING,STRING>`).
# MAGIC - **Schema drift.** Step 2 reads the schema from your MLflow version at runtime, so the
# MAGIC   target always matches. If MLflow adds/renames columns, Step 3 will flag unmapped ones.
# MAGIC - **Do not pre-create** `mlflow_experiment_trace_metadata` or `..._unified` — MLflow
# MAGIC   builds those as views during binding; pre-creating them as tables breaks it.
# MAGIC - See `SCHEMA_REFERENCE.md` for the full target schema and the reshape mapping.
