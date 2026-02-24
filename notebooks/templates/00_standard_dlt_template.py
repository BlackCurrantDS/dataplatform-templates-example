# ============================================================
# 🧱 Fully Dynamic Multi-Source DLT + Gold Layer (dbt)
# ------------------------------------------------------------
# Features:
# - Multi-source Bronze ingestion (API, S3, Redshift, future types)
# - Silver transformation & validation with Great Expectations
# - Operational rules (partitions, VACUUM, ANALYZE) per contract
# - Central monitoring & alerting
# - Optional Gold layer using dbt models
# ============================================================

# make the `shared_modules` directory importable when the notebook is deployed
# via an asset bundle. bundles only push the files to the workspace, they are not
# automatically added to Python's sys.path, so we have to do it manually.
import sys
import os

# Determine notebook directory
if "__file__" in globals():
    # Running as a plain .py file
    notebook_dir = os.path.dirname(os.path.abspath(__file__))
else:
    # Databricks notebook environment
    notebook_dir = os.getcwd()

# If your structure is repo-root/notebooks/..., repo root is parent of notebooks
notebook_root = os.path.dirname(notebook_dir)   # <repo-root>/notebook
print(f"📁 Notebook root directory: {notebook_root}")
repo_root = os.path.dirname(notebook_root)      # <repo-root>
print(f"📁 Repository root directory: {repo_root}")

# 1) Make "shared_modules" importable
#    shared_modules is under <repo-root>/notebook/shared_modules
if notebook_root not in sys.path:
    sys.path.append(notebook_root)

default_config_relative = "configs/dynamic_sources.yaml"


import dlt
import yaml
from shared_modules import pipeline_utils as utils
from pyspark.sql.functions import col, current_timestamp
#from shared_modules.dbt_utils import run_dbt_models  # assume you have reusable dbt run utils

# -------------------------
# 1️⃣ Notebook parameters
# -------------------------
dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("logger_name", "dynamic_multi_source_gold", "Logger Name")
dbutils.widgets.text("run_dbt", "false", "Run Gold Layer dbt models?")

env = dbutils.widgets.get("env")
logger_name = dbutils.widgets.get("logger_name")
run_dbt_flag = dbutils.widgets.get("run_dbt").lower() == "true"
logger = utils.get_logger(logger_name)

# -------------------------
# 2️⃣ Load configuration
# -------------------------
# If the widget value is relative, resolve it relative to repo_root
if not os.path.isabs(default_config_relative):
    config_path = os.path.join(repo_root, default_config_relative)
    print(f"📁 Resolved config path: {config_path}")
else:
    config_path = default_config_relative
    print(f"📁 Using absolute config path: {config_path}")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)[env]

sources = config["sources"]
alert_channel = config.get("alert_channel")
logger.info(f"Loaded {len(sources)} sources for ingestion.")

# -------------------------
# 3️⃣ Bronze Tables
# -------------------------
for src in sources:
    src_name = src["name"]

    def make_bronze_table(src_config):
        @dlt.table(name=f"{src_config['name']}_bronze", comment=f"Bronze table for {src_config['name']}")
        def bronze():
            try:
                # Fully dynamic loader, no if/else
                df = utils.load_source_dynamic(src_config, logger)
                df = df.withColumn("ingest_ts", current_timestamp())
                logger.info(f"✅ Loaded {df.count()} rows for {src_config['name']} Bronze")
                return df
            except Exception as e:
                utils.log_to_monitoring(dataset=src_config['name'], env=env, status="FAILED",
                                        rows_written=0, validation_success=False,
                                        failed_expectations=0, error_message=str(e))
                utils.send_alert(alert_channel, f"🚨 {src_config['name']} Bronze failed", logger)
                raise
        return bronze
    make_bronze_table(src)

# -------------------------
# 4️⃣ Silver Tables with Contract Validation + Operations
# -------------------------
for src in sources:
    src_name = src["name"]
    contract_path = src["contract_path"]

    def make_silver_table(src_config):
        @dlt.table(name=f"{src_config['name']}_silver", comment=f"Silver table for {src_config['name']}")
        @dlt.expect_or_drop("non_null_order_date", "order_date IS NOT NULL")
        def silver():
            try:
                df_raw = dlt.read(f"{src_config['name']}_bronze")
                df_clean = df_raw.dropDuplicates()
                
                # Validate using Great Expectations & contract
                validation = utils.validate_with_contract(df_clean, src_config["contract_path"], logger)
                if not validation["success"]:
                    logger.warning(f"{len(validation['results'])} expectations failed for {src_config['name']}")
                    utils.send_alert(alert_channel, f"🚨 Validation failed for {src_config['name']} Silver", logger)

                df_clean = df_clean.withColumn("pipeline_run_id", col("ingest_ts"))

                # Apply operational rules dynamically from contract
                with open(src_config["contract_path"], "r") as f:
                    contract = yaml.safe_load(f)
                ops = contract.get("operational", {})
                partitions = ops.get("partition_columns", [])
                vacuum_hours = ops.get("vacuum", {}).get("retention_hours", 168)
                if partitions or vacuum_hours:
                    utils.optimize_table(f"silver.{src_config['name']}", partitions=partitions, vacuum_hours=vacuum_hours, logger=logger)

                return df_clean
            except Exception as e:
                utils.log_to_monitoring(dataset=src_config['name'], env=env, status="FAILED",
                                        rows_written=0, validation_success=False,
                                        failed_expectations=len(validation.get("results", [])),
                                        error_message=str(e))
                utils.send_alert(alert_channel, f"🚨 Silver pipeline failed for {src_config['name']}", logger)
                raise
        return silver
    make_silver_table(src)

# -------------------------
# 5️⃣ Gold Layer - dbt Models (Optional)
# -------------------------
"""
if run_dbt_flag:
    try:
        dbt_targets = [src['name'] for src in sources]  # or define specific Gold models
        run_dbt_models(target_models=dbt_targets, env=env, logger=logger)
        logger.info("✅ Gold layer dbt models executed successfully")
    except Exception as e:
        logger.error(f"Gold layer dbt run failed: {str(e)}")
        utils.send_alert(alert_channel, "🚨 Gold layer dbt run failed", logger)
"""
# -------------------------
# 6️⃣ Central Monitoring
# -------------------------
@dlt.table(name=f"{logger_name}_pipeline_monitoring", comment="Central monitoring table")
def monitoring():
    try:
        for src in sources:
            rows_written = dlt.read(f"{src['name']}_silver").count()
            utils.log_to_monitoring(dataset=src['name'], env=env, status="SUCCESS",
                                    rows_written=rows_written, validation_success=True,
                                    failed_expectations=0)
        logger.info("📊 All source pipelines logged to central monitoring table")
        return dlt.read(f"{sources[0]['name']}_silver").limit(0)
    except Exception as e:
        logger.error(f"Monitoring table failed: {str(e)}")
        return dlt.read(f"{sources[0]['name']}_silver").limit(0)