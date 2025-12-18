from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import re
from io import BytesIO
import pandas as pd

# ---------------- CONFIG ----------------
S3_BUCKET = "kiwi-cs"
S3_PREFIX = "yes_ppe_data/"
S3_PROCESSED_PREFIX = "yes_ppe_processed/"

MASTER_TABLE = "analysis.ppe_master_data"
DAILY_TABLE = "analysis.ppe_active_data"
MAPPING_TABLE = "temp.user_id_account_num_map"

MAX_FILE_SIZE_MB = 20  # safety guardrail

# ---------------- HELPERS ----------------
def get_latest_excel_from_s3():
    files = dbutils.fs.ls(f"s3://{S3_BUCKET}/{S3_PREFIX}")
    excel_files = [f for f in files if f.path.endswith(".xlsx")]

    if not excel_files:
        print("⚠️ No Excel files found")
        return None

    latest = max(excel_files, key=lambda x: x.modificationTime)
    print(f"📂 Latest Excel file: {latest.path}")
    return latest.path

def extract_date_from_filename(path):
    filename = path.split("/")[-1]
    clean = filename.replace(".xlsx", "")

    patterns = [
        r"(\d{1,2})([A-Za-z]{3,9})",
        r"([A-Za-z]{3,9})(\d{1,2})",
    ]

    for p in patterns:
        m = re.search(p, clean)
        if m:
            try:
                return datetime.strptime(
                    "".join(m.groups()) + str(datetime.now().year),
                    "%d%b%Y"
                ).date()
            except:
                pass

    return datetime.now().date()

# ---------------- STEP 1 ----------------
def clear_and_load_all_users(file_date):
    spark.sql(f"DELETE FROM {DAILY_TABLE}")

    spark.sql(f"""
        INSERT INTO {DAILY_TABLE}
        SELECT
            account_num,
            user_id,
            0.0 AS total_spends,
            0 AS number_of_transactions,
            -1 AS emi_prop_decile_count,
            DATE('{file_date}') AS file_date
        FROM {MAPPING_TABLE}
    """)

    count = spark.sql(
        f"SELECT COUNT(*) FROM {DAILY_TABLE} WHERE file_date = DATE('{file_date}')"
    ).collect()[0][0]

    print(f"✅ Loaded {count} users with default values")

# ---------------- STEP 2 ----------------
def read_and_aggregate_excel(path):
    # Read Excel as binary
    binary_df = spark.read.format("binaryFile").load(path)
    row = binary_df.select("content", "length").limit(1).collect()[0]

    file_size_mb = row["length"] / (1024 * 1024)
    print(f"📦 Excel size: {file_size_mb:.2f} MB")
    if file_size_mb > MAX_FILE_SIZE_MB:
        raise Exception("❌ Excel file too large for pandas-based ingestion")

    # Read all sheets via pandas, force MT_ACCT as string
    xls = pd.ExcelFile(BytesIO(row["content"]))
    df_list = []
    for sheet in xls.sheet_names:
        pdf = pd.read_excel(xls, sheet_name=sheet, dtype={"MT_ACCT": str})
        # Fill numeric columns
        pdf["TXN_AMOUNT"] = pd.to_numeric(pdf["TXN_AMOUNT"], errors="coerce").fillna(0)
        pdf["EMI_PROP_DECILE"] = pd.to_numeric(pdf["EMI_PROP_DECILE"], errors="coerce").fillna(0).astype(int)
        df_list.append(pdf)
    
    pdf_all = pd.concat(df_list, ignore_index=True)

    # Create Spark DataFrame with explicit schema
    schema = StructType([
        StructField("MT_ACCT", StringType()),
        StructField("TXN_AMOUNT", DoubleType()),
        StructField("EMI_PROP_DECILE", IntegerType())
    ])
    df = spark.createDataFrame(pdf_all, schema=schema)

    # Only trim spaces, do NOT remove leading zeros
    df = df.withColumn("MT_ACCT", F.trim(F.col("MT_ACCT")))

    # Aggregate by account
    agg = (
        df.groupBy("MT_ACCT")
          .agg(
              F.sum("TXN_AMOUNT").alias("total_spends"),
              F.count("TXN_AMOUNT").alias("number_of_transactions"),
              F.max("EMI_PROP_DECILE").cast("int").alias("emi_prop_decile_count")
          )
          .withColumnRenamed("MT_ACCT", "account_num")
    )

    print(f"📊 Aggregated {agg.count()} accounts from Excel")
    return agg

# ---------------- STEP 3 ----------------
def update_metrics(agg_df, file_date):
    agg_df.createOrReplaceTempView("ppe_excel_data")

    spark.sql(f"""
        MERGE INTO {DAILY_TABLE} d
        USING ppe_excel_data e
        ON d.account_num = e.account_num
           AND d.file_date = DATE('{file_date}')
        WHEN MATCHED THEN UPDATE SET
            d.total_spends = e.total_spends,
            d.number_of_transactions = e.number_of_transactions,
            d.emi_prop_decile_count = e.emi_prop_decile_count
    """)

    stats = spark.sql(f"""
        SELECT
            COUNT(*) AS total_users,
            SUM(CASE WHEN emi_prop_decile_count >= 0 THEN 1 ELSE 0 END) AS users_in_excel,
            SUM(CASE WHEN emi_prop_decile_count = -1 THEN 1 ELSE 0 END) AS users_not_in_excel
        FROM {DAILY_TABLE}
        WHERE file_date = DATE('{file_date}')
    """).collect()[0]

    print(f"✅ Updated {stats.users_in_excel} users from Excel")

# ---------------- STEP 4 ----------------
def append_to_master(file_date):
    spark.sql(f"""
        INSERT INTO {MASTER_TABLE}
        SELECT *
        FROM {DAILY_TABLE}
        WHERE file_date = DATE('{file_date}')
    """)
    print("✅ Appended data to master table")

# ---------------- MAIN ----------------
def main():
    excel_path = get_latest_excel_from_s3()
    if not excel_path:
        print("📭 No file to process")
        return

    file_date = extract_date_from_filename(excel_path)
    print(f"📅 File date: {file_date}")

    clear_and_load_all_users(file_date)
    agg_df = read_and_aggregate_excel(excel_path)
    update_metrics(agg_df, file_date)
    append_to_master(file_date)

    # Move file to processed
    processed_path = excel_path.replace(S3_PREFIX, S3_PROCESSED_PREFIX)
    dbutils.fs.mv(excel_path, processed_path)
    print(f"📦 File moved to processed: {processed_path}")

main()
