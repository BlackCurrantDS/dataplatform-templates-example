# ============================================================
# 📦 Shared Utilities for Databricks Pipelines
# File: /Workspace/shared_modules/pipeline_utils.py
# ============================================================
import logging
import json
from typing import Dict
import yaml
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import great_expectations as gx
from pyspark.sql import DataFrame

spark = SparkSession.builder.getOrCreate()

# -------------------------
# 🧭 Logging
# -------------------------
def get_logger(name="data_pipeline"):
    """Creates and returns a structured logger with the given name."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


# -------------------------
# 📡 Data Loaders
# -------------------------
def load_data_from_api(url: str, logger):
    """Fetch data from REST API and return as Spark DataFrame."""
    logger.info(f"Fetching data from API: {url}")
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        df = spark.createDataFrame(data)
        df = df.withColumn("ingest_ts", current_timestamp())
        return df
    except Exception as e:
        logger.error(f"API load failed: {str(e)}")
        raise


def load_data_from_path(path: str, logger):
    """Read file-based data (JSON, CSV, Parquet) into Spark."""
    logger.info(f"Reading data from path: {path}")
    try:
        if path.endswith(".csv"):
            df = spark.read.option("header", True).csv(path)
        elif path.endswith(".parquet"):
            df = spark.read.parquet(path)
        else:
            df = spark.read.option("multiline", True).json(path)
        df = df.withColumn("ingest_ts", current_timestamp())
        return df
    except Exception as e:
        logger.error(f"File read failed: {str(e)}")
        raise


# -------------------------
# 🧪 Great Expectations Validation
# -------------------------
def validate_with_contract(df, contract_path: str, logger):
    """Run GE expectations defined in the YAML contract on a DataFrame."""
    logger.info(f"Validating data using contract: {contract_path}")
    with open(contract_path, "r") as f:
        contract = yaml.safe_load(f)

    context = gx.get_context()
    suite = context.suites.add(f"{contract['dataset']}_suite")

    for col_def in contract["schema"]:
        name = col_def["name"]
        if col_def.get("required"):
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=name))
        if col_def.get("unique"):
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column=name))
        if col_def.get("allowed_values"):
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeInSet(
                    column=name,
                    value_set=col_def["allowed_values"]
                )
            )

    batch = context.sources.spark.get_batch(dataframe=df)
    results = suite.validate(batch)

    logger.info(f"Validation success: {results['success']}")
    failed = [r for r in results["results"] if not r["success"]]
    for f in failed:
        logger.warning(f"Failed expectation: {f['expectation_config']['expectation_type']}")
    return results


# -------------------------
# 📣 Alerting
# -------------------------
def send_alert(alert_url: str, message: str, logger):
    """Send Slack alert."""
    if not alert_url:
        logger.warning("No alert channel configured.")
        return
    try:
        payload = {"text": message}
        requests.post(alert_url, data=json.dumps(payload))
        logger.info("Alert sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send alert: {str(e)}")


# -------------------------
# 🧾 Centralized Monitoring & Audit
# -------------------------
def log_to_monitoring(
    dataset: str,
    env: str,
    status: str,
    rows_written: int,
    validation_success: bool,
    failed_expectations: int,
    error_message: str = None,
):
    """Write pipeline metrics to a central Delta monitoring table."""
    run_ts = datetime.utcnow().isoformat()
    run_user = (
        spark.sparkContext.getConf().get("spark.databricks.clusterUsageTags.user", "unknown")
    )

    data = [
        {
            "env": env,
            "dataset": dataset,
            "status": status,
            "rows_written": rows_written,
            "validation_success": validation_success,
            "failed_expectations": failed_expectations,
            "error_message": error_message,
            "run_user": run_user,
            "run_ts": run_ts,
        }
    ]
    df = spark.createDataFrame(data)
    df.write.mode("append").format("delta").saveAsTable("platform_monitoring.pipeline_runs")
    print(f"📊 Logged pipeline run to monitoring table ({status})")



def optimize_table(table_name: str, partitions: list = None, vacuum_hours: int = 168, logger=None):
    """
    Applies table maintenance: partitioning, VACUUM, ANALYZE
    """
    try:
        if logger:
            logger.info(f"Starting optimization for table {table_name}")
        
        # Partitioning (if not already partitioned)
        if partitions:
            logger.info(f"Ensuring partitioning on: {partitions}")
            # In Delta, partitioning is set at table creation. If table exists, we cannot repartition without rewriting.
            # Just logging it here; could be enforced via table creation scripts.
        
        # VACUUM
        vacuum_cmd = f"VACUUM {table_name} RETAIN {vacuum_hours} HOURS"
        spark.sql(vacuum_cmd)
        if logger:
            logger.info(f"✅ Vacuum completed: {vacuum_cmd}")
        
        # ANALYZE
        analyze_cmd = f"ANALYZE TABLE {table_name} COMPUTE STATISTICS"
        spark.sql(analyze_cmd)
        if logger:
            logger.info(f"✅ Analyze completed: {analyze_cmd}")
    
    except Exception as e:
        if logger:
            logger.error(f"Table optimization failed for {table_name}: {str(e)}")
        raise

# -------------------------
# Source type → loader mapping
# -------------------------
SOURCE_LOADERS = {
    "api": lambda src_config, logger: load_data_from_api(src_config["data_source_url"], logger),
    "s3": lambda src_config, logger: load_data_from_path(src_config["input_path"], logger),
    "redshift": lambda src_config, logger: load_data_from_path(src_config.get("s3_unload_path"), logger)
    # Add new types here dynamically, e.g., "mongo": load_data_from_mongo
}

# Example: Add MongoDB support
def load_data_from_mongo(uri, logger):
    logger.info(f"Loading data from MongoDB: {uri}")
    df = spark.read.format("mongo").option("uri", uri).load()
    return df

def load_source_dynamic(src_config: Dict, logger: logging.Logger) -> DataFrame:
    """
    Dynamically load a source based on its type using the SOURCE_LOADERS map.
    
    Args:
        src_config (Dict): source config from YAML, must contain 'type' and path/url keys
        logger (logging.Logger): logger instance for logging
    
    Returns:
        pyspark.sql.DataFrame: loaded DataFrame
    
    Raises:
        ValueError: if source type is unknown or required keys are missing
    """
    src_type = src_config.get("type")
    if not src_type:
        raise ValueError(f"Source type not defined in config: {src_config}")

    loader_fn = SOURCE_LOADERS.get(src_type)
    if not loader_fn:
        raise ValueError(f"No loader defined for source type: {src_type}")

    try:
        df = loader_fn(src_config, logger)
        if logger:
            logger.info(f"✅ Successfully loaded {df.count()} rows from source '{src_config.get('name')}' ({src_type})")
        return df

    except KeyError as e:
        err_msg = f"Missing required config key for source '{src_config.get('name')}': {str(e)}"
        if logger:
            logger.error(err_msg)
        raise

    except Exception as e:
        err_msg = f"Failed to load source '{src_config.get('name')}' ({src_type}): {str(e)}"
        if logger:
            logger.error(err_msg)
        raise