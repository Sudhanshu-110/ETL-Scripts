import sys
import json
import math
import time
import logging
import requests
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ---------------- CONFIG ----------------
TABLE_NAME = "analysis.ppe_active_data"

CLEVERTAP_ACCOUNT_ID = "R9K-74Z-576Z"
CLEVERTAP_PASSCODE   = "ERM-BAD-UTUL"
CLEVERTAP_URL = "https://in1.api.clevertap.com/1/upload"

BATCH_SIZE  = 1000
MAX_RETRIES = 3
RETRY_DELAY = 3  # seconds

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

spark = SparkSession.builder.getOrCreate()

# ---------------- FETCH DATA ----------------
def fetch_spark_data():
    """
    Fetch data from Redshift / Delta table into Spark DataFrame
    """
    logging.info(f"Reading data from table: {TABLE_NAME}")
    df = spark.table(TABLE_NAME)

    # Ensure types are correct for CleverTap payload
    df = df.select(
        col("user_id").alias("user_id"),
        col("total_spends").cast("double").alias("total_spends"),
        col("number_of_transactions").cast("int").alias("number_of_transactions"),
        col("emi_prop_decile_count").cast("int").alias("emi_prop_decile_count")
    )
    total = df.count()
    logging.info(f"Fetched {total} rows from {TABLE_NAME}")
    return df, total

# ---------------- CLEVERTAP PROFILE BUILDER ----------------
def build_ct_profile(row):
    """
    Convert Spark Row -> CleverTap profile payload
    """
    return {
        "identity": row["user_id"],
        "type": "profile",
        "profileData": {
            "YES_PTEMI_EligibleSpend": float(row["total_spends"]) if row["total_spends"] else 0.0,
            "YES_PTEMI_EligibleTxns": int(row["number_of_transactions"]) if row["number_of_transactions"] else 0,
            "YES_PTEMI_PropDecile": int(row["emi_prop_decile_count"]) if row["emi_prop_decile_count"] else 0
        }
    }

# ---------------- CLEVERTAP UPLOAD ----------------
def upload_batch_to_clevertap(batch):
    headers = {
        "X-CleverTap-Account-Id": CLEVERTAP_ACCOUNT_ID,
        "X-CleverTap-Passcode": CLEVERTAP_PASSCODE,
        "Content-Type": "application/json"
    }

    payload = {"d": batch}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(CLEVERTAP_URL, headers=headers, data=json.dumps(payload))

            if response.status_code == 200:
                logging.info("Batch uploaded successfully.")
                return True
            else:
                logging.warning(f"Attempt {attempt} failed: {response.text}")

        except Exception as e:
            logging.warning(f"Attempt {attempt} exception: {str(e)}")

        if attempt < MAX_RETRIES:
            logging.info(f"Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)

    logging.error("Batch failed after max retries.")
    return False

# ---------------- MAIN ----------------
def main():
    df, total = fetch_spark_data()

    if total == 0:
        logging.info("No data found. Exiting.")
        return

    total_batches = math.ceil(total / BATCH_SIZE)
    logging.info(f"Uploading {total} rows in {total_batches} batches...")

    df_list = df.collect()  # Collect Spark DataFrame rows to driver for batch processing
    for batch_idx in range(total_batches):
        start = batch_idx * BATCH_SIZE
        end = start + BATCH_SIZE

        batch_rows = df_list[start:end]
        batch_json = [build_ct_profile(r.asDict()) for r in batch_rows]

        logging.info(f"Uploading batch {batch_idx+1}/{total_batches} ({len(batch_json)} profiles)...")
        success = upload_batch_to_clevertap(batch_json)
        if not success:
            logging.error(f"Batch {batch_idx+1} failed. Stopping.")
            sys.exit(1)

    logging.info("All batches uploaded successfully to CleverTap!")

if __name__ == "__main__":
    main()
