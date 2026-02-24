# ============================================================
# 🧱 Databricks Standard Ingestion Template
# ------------------------------------------------------------
# PURPOSE:
#   - Standardized framework for ingestion & validation pipelines
#   - Enforces configuration, logging, metadata, and data-quality rules
#   - Integrates with Great Expectations and Databricks monitoring
# AUTHOR:   Data Platform Engineering Team
# VERSION:  1.0
# ============================================================
import sys, os
try:
    # grab the workspace path of this notebook and derive the package location
    nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    pkg_dir = os.path.dirname(nb_path).rstrip("/") + "/shared_modules"
    # the local filesystem equivalent is `/Workspace` prefix
    sys.path.append("/Workspace" + pkg_dir)
except Exception:
    # fallback for local runs or tests
    sys.path.append(os.path.join(os.getcwd(), "notebooks", "shared_modules"))
# -------------------------
# 🔧 1. Setup
# -------------------------
from datetime import datetime
import yaml, traceback
from pyspark.sql import SparkSession
from shared_modules import pipeline_utils as utils

# Widgets
dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("logger_name", "api_ingestion", "Logger Name")
dbutils.widgets.text("contract_path", "/Workspace/contracts/sales_orders_contract.yaml", "Contract Path")

env = dbutils.widgets.get("env")
logger_name = dbutils.widgets.get("logger_name")
contract_path = dbutils.widgets.get("contract_path")

spark = SparkSession.builder.getOrCreate()
logger = utils.get_logger(logger_name)
logger.info(f"Started pipeline for {logger_name} in {env}")


# Load config
CONFIG_PATH = "/Workspace/configs"
with open(f"{CONFIG_PATH}/base_config.yaml", "r") as f:
    base_cfg = yaml.safe_load(f)
with open(f"{CONFIG_PATH}/env/{env}.yaml", "r") as f:
    env_cfg = yaml.safe_load(f)
config = {**base_cfg, **env_cfg}

# -------------------------
# 📡 2. Data Ingestion
# -------------------------
try:
    if config.get("data_source_url"):
        df_raw = utils.load_data_from_api(config["data_source_url"], logger)
    else:
        df_raw = utils.load_data_from_path(config["input_path"], logger)

    rows = df_raw.count()
    logger.info(f"✅ Ingested {rows} records from source.")
except Exception as e:
    err_msg = f"Ingestion failed: {str(e)}"
    logger.error(err_msg)
    utils.log_to_monitoring(dataset="unknown", env=env, status="FAILED",
                            rows_written=0, validation_success=False,
                            failed_expectations=0, error_message=err_msg)
    utils.send_alert(config.get("alert_channel"), f"🚨 {logger_name} ingestion failed: {e}", logger)
    raise

# -------------------------
# 🧪 3. Validate with Data Contract
# -------------------------
try:
    validation = utils.validate_with_contract(df_raw, contract_path, logger)
    if not validation["success"]:
        raise Exception("Contract validation failed.")
except Exception as e:
    err_msg = f"Validation failed: {str(e)}"
    logger.error(err_msg)
    utils.log_to_monitoring(dataset="unknown", env=env, status="FAILED",
                            rows_written=0, validation_success=False,
                            failed_expectations=len(validation.get('results', [])),
                            error_message=err_msg)
    utils.send_alert(config.get("alert_channel"), f"🚨 Validation failure for {logger_name}", logger)
    raise


# -------------------------
# 🧹 4. Transform & Write
# -------------------------
try:
    df_clean = df_raw.dropDuplicates().filter("order_date is not null")
    df_clean.write.format("delta").mode("append").save(config["storage_path"])
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {config['target_table']} USING DELTA LOCATION '{config['storage_path']}'""")
    logger.info(f"✅ Data written to {config['target_table']}")
except Exception as e:
    err_msg = f"Write failed: {str(e)}"
    logger.error(err_msg)
    utils.log_to_monitoring(dataset="unknown", env=env, status="FAILED",
                            rows_written=0, validation_success=True,
                            failed_expectations=0, error_message=err_msg)
    utils.send_alert(config.get("alert_channel"), f"🚨 Write failure for {logger_name}", logger)
    raise

# -------------------------
# 📊 5. Central Monitoring & Completion
# -------------------------
rows_written = df_clean.count()
utils.log_to_monitoring(dataset=logger_name,
                        env=env,
                        status="SUCCESS",
                        rows_written=rows_written,
                        validation_success=True,
                        failed_expectations=0,
                        error_message=None)

utils.send_alert(config.get("alert_channel"),
                 f"✅ {logger_name} pipeline completed successfully ({rows_written} rows).",
                 logger)

logger.info(f"🚀 {logger_name} completed successfully.")