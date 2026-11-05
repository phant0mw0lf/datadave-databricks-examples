# Databricks notebook source
# MAGIC %md
# MAGIC # pipetree on Databricks - the example pipeline, live
# MAGIC
# MAGIC One notebook for two of the three variants in part 4 of the Pipeline
# MAGIC Engineering series:
# MAGIC
# MAGIC - **A, the package:** one task runs this notebook with `select` empty.
# MAGIC   pipetree builds the dependency tree and runs it - parallel, with retries,
# MAGIC   one run log.
# MAGIC - **B, a job with tasks:** one task per table runs this notebook with
# MAGIC   `select=<table>`. The Databricks job owns the tree; pipetree only merges
# MAGIC   the one table it is given.
# MAGIC
# MAGIC `faults` injects the two problems of part 2 (a throttled source, a
# MAGIC malformed export) so both variants can be watched failing the same way.

# COMMAND ----------


dbutils.widgets.text("catalog", "")  # noqa: F821 - notebook global
dbutils.widgets.text("config_path", "")  # noqa: F821
dbutils.widgets.text("select", "")  # noqa: F821 - one table (variant B) or empty (variant A)
dbutils.widgets.text("init", "false")  # noqa: F821 - true: full reload
dbutils.widgets.text("max_workers", "4")  # noqa: F821
# JSON, table -> fault: {"bronze.orders": {"kind": "throttle", "count": 2,
# "message": "429 Too Many Requests"}, "bronze.customer": {"kind": "fail",
# "message": "malformed export file"}}. Empty: no faults.
dbutils.widgets.text("faults", "")  # noqa: F821

# COMMAND ----------

import json
import logging
import sys
from pathlib import Path

from pipetree import run_pipeline
from pipetree.adapters.fault import FaultInjectingAdapter, FaultSpec
from pipetree.adapters.spark.adapter import SparkAdapter
from pipetree.adapters.spark.runlog_writer import DeltaRunLogWriter
from pipetree.config.loader import load_config
from pipetree.graph.select import split_names
from pipetree.model import PipelineConfig
from pipetree.notebook import LiveGraphView
from pipetree.platform.databricks import DatabricksPlatform

logging.basicConfig(level=logging.INFO, stream=sys.stdout, force=True,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")  # fmt: skip
logging.getLogger("py4j").setLevel(logging.WARNING)

catalog = dbutils.widgets.get("catalog")  # noqa: F821
config_path = Path(dbutils.widgets.get("config_path"))  # noqa: F821
select = split_names(dbutils.widgets.get("select")) or None  # noqa: F821
init = dbutils.widgets.get("init").lower() == "true"  # noqa: F821
max_workers = int(dbutils.widgets.get("max_workers") or "4")  # noqa: F821
faults = {
    table: FaultSpec(**spec)
    for table, spec in json.loads(dbutils.widgets.get("faults") or "{}").items()  # noqa: F821
}

# The pipeline writes plain schema.table names: make them land in `catalog`.
spark.sql(f"USE CATALOG `{catalog}`")  # noqa: F821

config = PipelineConfig.from_validated_raw(load_config(config_path))
platform = DatabricksPlatform(dbutils=dbutils, catalog=catalog)  # noqa: F821
adapter = SparkAdapter(
    spark, systems=config.systems, base_dir=config_path.parent, platform=platform
)
if faults:
    adapter = FaultInjectingAdapter(adapter, faults)

title = f"pipetree - {', '.join(select)}" if select else "pipetree - the whole tree"
digest = run_pipeline(
    config_path,
    adapter=adapter,
    select=select,
    init=init,
    max_workers=max_workers,
    # one row per table in <catalog>._meta.pipeline_run_log - the run log the post compares
    # with the job's run history (B) and the pipeline's event log (C)
    run_log_writer=DeltaRunLogWriter(spark),  # noqa: F821
    observer=LiveGraphView(title=title),
)

# COMMAND ----------

# A failed table fails the task (and with it the job run).
if digest.exit_code != 0:
    raise RuntimeError(
        f"pipetree run {digest.execution_id} FAILED (exit code {digest.exit_code})"
    )
