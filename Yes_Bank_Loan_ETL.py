# ============================================================
# YES BANK LOAN FUNNEL INGESTION – DATABRICKS (FINAL)
# ============================================================

# -------------------- CONFIG --------------------

S3_BUCKET = "s3://kiwi-cs"
S3_PREFIX = "loans-data/"
PROCESSED_PREFIX = "processed/"
METADATA_PATH = f"{S3_BUCKET}/metadata/loan_ingestion_metadata.json"

TABLE_MAP = {
    "funnel": "analysis.yes_bank_loan_funnel_data",
    "customer level": "analysis.yes_bank_loan_customer_data"
}

FINAL_FUNNEL_TABLE = "analysis.yes_bank_loans_funnel_final"

# ------------------------------------------------

import re
from datetime import datetime
import pandas as pd
from io import BytesIO

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -------------------- HELPERS --------------------

def normalize_column(col):
    return re.sub(r'\W+', '_', col.strip().lower())

def parse_date_from_filename(name):
    m = re.search(r"(\d{4}-\d{2}-\d{2})", name)
    return datetime.strptime(m.group(1), "%Y-%m-%d").date() if m else None

def read_metadata():
    try:
        return spark.read.json(METADATA_PATH).select("last_processed_date").first()[0]
    except:
        return None

def write_metadata(dt):
    payload = [{
        "last_processed_date": str(dt),
        "last_updated": datetime.utcnow().isoformat(),
        "job": "yes_bank_loan_funnel_databricks"
    }]
    spark.createDataFrame(payload).write.mode("overwrite").json(METADATA_PATH)

def map_columns(pdf):
    col_map = {}
    for c in pdf.columns:
        clean = re.sub(r'\s+', '', c).lower()
        if re.match(r'customerid', clean):
            col_map[c] = 'customer_id'
        elif re.match(r'bookedamount', clean):
            col_map[c] = 'booked_amount'
        elif re.match(r'bookedstatus', clean):
            col_map[c] = 'booked_status'
        elif re.match(r'bookingtimestamp', clean):
            col_map[c] = 'booking_timestamp'
        else:
            col_map[c] = normalize_column(c)
    return pdf.rename(columns=col_map)

def read_excel_from_s3(path):
    binary_df = spark.read.format("binaryFile").load(path)
    content = binary_df.collect()[0]["content"]
    return pd.ExcelFile(BytesIO(content))

def move_s3_file(src_path):
    filename = src_path.split("/")[-1]
    dest_path = f"{S3_BUCKET}/{PROCESSED_PREFIX}{filename}"
    try:
        dbutils.fs.mkdirs(f"{S3_BUCKET}/{PROCESSED_PREFIX}")
    except:
        pass
    dbutils.fs.mv(src_path, dest_path)

# -------------------- FUNNEL CONFIG --------------------

FUNNEL_COLS = [
    "get_otp", "login_otp_sent", "login_otp_validated",
    "loan_account_details_filled", "kfs_agree_cta_clicked",
    "loan_summary_page_view", "loan_summary_page_proceedcta",
    "consent_otp_sent", "consent_otp_validated", "bookings"
]

# -------------------- CUSTOMER SHEET --------------------

def process_customer_sheet(pdf, file_date):
    pdf = map_columns(pdf)
    pdf["file_date"] = str(file_date)

    pdf["loan_id"] = (
        pdf["source"].astype(str).str.strip() + "_" +
        pdf["customer_id"].astype(str).str.strip()
    )

    for col in FUNNEL_COLS:
        if col not in pdf.columns:
            pdf[col] = None

    sdf = spark.createDataFrame(pdf)

    # SAFE handling of 1 / 1.0 / true / yes
    for col in FUNNEL_COLS:
        sdf = sdf.withColumn(
            col,
            F.when(
                F.lower(F.trim(F.col(col).cast("string")))
                .isin("1", "1.0", "true", "yes"),
                F.col("file_date")
            ).otherwise(None)
        )

    return sdf

# -------------------- STATUS RULES --------------------

def apply_status_rules(new_df, target_table):
    if not spark.catalog.tableExists(target_table):
        return new_df

    existing = spark.table(target_table)

    w = Window.partitionBy("loan_id").orderBy(F.col("file_date").desc())

    latest = (
        existing
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .select(
            "loan_id",
            F.upper(F.col("booked_status")).alias("latest_status")
        )
    )

    joined = new_df.join(latest, "loan_id", "left")

    return joined.filter(
        (F.col("latest_status").isNull()) |
        ((F.col("latest_status") == "BOOKED") &
         (F.upper(F.col("booked_status")) == "APPROVED"))
    ).drop("latest_status")

# -------------------- FUNNEL SHEET --------------------

def process_funnel_sheet(pdf, file_date):
    pdf = map_columns(pdf)
    pdf["file_date"] = str(file_date)
    pdf["source"] = pdf["source"].astype(str).str.strip()
    return spark.createDataFrame(pdf)

# -------------------- MERGE FUNCTIONS --------------------

def merge_customer_table(table, df):
    df.createOrReplaceTempView("src")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table}
        USING DELTA
        AS SELECT * FROM src WHERE 1=0
    """)

    spark.sql(f"""
        MERGE INTO {table} t
        USING src s
        ON t.loan_id = s.loan_id
           AND t.file_date = s.file_date
        WHEN NOT MATCHED THEN INSERT *
    """)

def merge_funnel_table(table, df):
    df.createOrReplaceTempView("src")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table}
        USING DELTA
        AS SELECT * FROM src WHERE 1=0
    """)

    spark.sql(f"""
        MERGE INTO {table} t
        USING src s
        ON t.source = s.source
           AND t.file_date = s.file_date
        WHEN NOT MATCHED THEN INSERT *
    """)

# -------------------- FINAL FUNNEL --------------------

def update_final_funnel(stage_table, target_table):

    spark.sql(f"""
        CREATE OR REPLACE TABLE {target_table}
        USING DELTA
        AS
        SELECT
            loan_id,
            max(file_date) as file_date,
            min(get_otp) as get_otp,
            min(login_otp_sent) as login_otp_sent,
            min(login_otp_validated) as login_otp_validated,
            min(loan_account_details_filled) as loan_account_details_filled,
            min(kfs_agree_cta_clicked) as kfs_agree_cta_clicked,
            min(loan_summary_page_view) as loan_summary_page_view,
            min(loan_summary_page_proceedcta) as loan_summary_page_proceedcta,
            min(consent_otp_sent) as consent_otp_sent,
            min(consent_otp_validated) as consent_otp_validated,
            min(bookings) as bookings,
            max(booked_amount) as booked_amount,
            max(booked_status) as booked_status,
            max(booking_timestamp) as booking_timestamp
        FROM {stage_table}
        GROUP BY loan_id
    """)

# -------------------- MAIN --------------------

last_date = read_metadata()
latest_processed = None

files_df = spark.read.format("binaryFile").load(f"{S3_BUCKET}/{S3_PREFIX}*.xlsx")

for row in sorted(files_df.collect(), key=lambda r: r.path):

    path = row.path
    filename = path.split("/")[-1]
    file_date = parse_date_from_filename(filename)

    if last_date and file_date <= datetime.strptime(last_date, "%Y-%m-%d").date():
        continue

    xl = read_excel_from_s3(path)

    for sheet in xl.sheet_names:
        key = sheet.strip().lower()
        pdf = xl.parse(sheet)

        if key == "customer level":
            df = process_customer_sheet(pdf, file_date)
            df = apply_status_rules(df, TABLE_MAP[key])
            merge_customer_table(TABLE_MAP[key], df)

        elif key == "funnel":
            df = process_funnel_sheet(pdf, file_date)
            merge_funnel_table(TABLE_MAP[key], df)

    # 🔹 MOVE FILE ONLY AFTER SUCCESSFUL LOAD
    move_s3_file(path)

    latest_processed = file_date

update_final_funnel(TABLE_MAP["customer level"], FINAL_FUNNEL_TABLE)

if latest_processed:
    write_metadata(latest_processed)

print("✅ YES BANK LOAN FUNNEL JOB COMPLETED SUCCESSFULLY")
