import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pandas as pd

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RetailETL")

def create_spark_session():
    """Initializes a Spark Session with JDBC driver support."""
    return SparkSession.builder \
        .appName("RetailTransactionalIntelligence") \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.6.0.jar") \
        .get_all()

def extract_data(spark, file_path):
    """
    Reads the Excel file into a Spark DataFrame.
    Note: For Excel, we use Pandas as a bridge due to Spark's limited native Excel support.
    """
    logger.info(f"Extracting data from {file_path}")
    
    # Read Excel with Pandas (Online Retail II has multiple sheets usually, we'll take the first or combine)
    # For this project, we'll assume the primary transactional sheet.
    pdf = pd.read_excel(file_path)
    
    # Convert to Spark DataFrame
    df = spark.createDataFrame(pdf)
    logger.info(f"Successfully loaded {df.count()} rows into Spark.")
    return df

def transform_data(df):
    """
    Applies business logic and sanitation:
    1. Rename columns to match Postgres schema.
    2. Format customer_id properly (converting float representation like 12345.0 to 12345).
    3. Identify cancellations (Invoice starts with 'C').
    4. Calculate total_amount.
    5. Filter out operational StockCodes (POST, D, M, etc.).
    6. Separate valid and invalid transactions (Quarantine).
    """
    logger.info("Starting Spark transformations...")
    
    # 1. Rename columns
    df = df.withColumnRenamed("Invoice", "invoice_no") \
           .withColumnRenamed("StockCode", "stock_code") \
           .withColumnRenamed("Description", "description") \
           .withColumnRenamed("Quantity", "quantity") \
           .withColumnRenamed("InvoiceDate", "raw_timestamp") \
           .withColumnRenamed("Price", "unit_price") \
           .withColumnRenamed("Customer ID", "customer_id") \
           .withColumnRenamed("Country", "country")

    # 2. Format customer_id (Double -> Long -> String) to drop trailing .0 decimals from Excel/Pandas
    df = df.withColumn("customer_id", col("customer_id").cast("double").cast("long").cast("string"))

    # 3. Add Business Logic Flags and Types
    df = df.withColumn("is_cancellation", col("invoice_no").startswith("C")) \
           .withColumn("transaction_timestamp", to_timestamp(col("raw_timestamp"))) \
           .withColumn("total_amount", col("quantity") * col("unit_price")) \
           .withColumn("processed_at", current_timestamp())

    # 4. Filter Operational Noise (Postage, Manual Adjustments)
    operational_codes = ["POST", "D", "M", "DOT", "BANK CHARGES", "ADJUST", "ADJUST2"]
    clean_df = df.filter(~col("stock_code").isin(operational_codes))
    
    # 5. Quarantine Logic (Separate valid from invalid)
    # Invalid: Missing or null/empty/NaN customer_id, or null transaction_timestamp
    invalid_df = clean_df.filter(
        col("customer_id").isNull() | 
        col("customer_id").isin("NaN", "", "null") |
        col("transaction_timestamp").isNull()
    )
    valid_df = clean_df.filter(
        col("customer_id").isNotNull() & 
        ~col("customer_id").isin("NaN", "", "null") &
        col("transaction_timestamp").isNotNull()
    )
    
    logger.info(f"Transformation complete. Valid: {valid_df.count()}, Invalid: {invalid_df.count()}")
    return valid_df, invalid_df

def load_data(df, table_name):
    """Writes Spark DataFrame to PostgreSQL using JDBC."""
    db_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:5432/{os.getenv('POSTGRES_DB', 'ecommerce_events')}"
    db_properties = {
        "user": os.getenv("POSTGRES_USER", "etl_user"),
        "password": os.getenv("POSTGRES_PASSWORD", "etl_password"),
        "driver": "org.postgresql.Driver"
    }
    
    logger.info(f"Loading data into {table_name}...")
    df.write.jdbc(url=db_url, table=table_name, mode="append", properties=db_properties)
    logger.info(f"Successfully loaded data into {table_name}.")

def main():
    spark = SparkSession.builder.appName("RetailETL").getOrCreate()
    try:
        # Resolve raw file path dynamically across possible container mount paths
        possible_paths = [
            "/opt/airflow/data/raw/online_retail_II.xlsx",
            "/opt/spark/data/raw/online_retail_II.xlsx",
            "/opt/bitnami/spark/data/raw/online_retail_II.xlsx",
            "./data/raw/online_retail_II.xlsx"
        ]
        raw_file = None
        for path in possible_paths:
            if os.path.exists(path):
                raw_file = path
                logger.info(f"Found input file at: {path}")
                break
        
        if not raw_file:
            raise FileNotFoundError(f"Could not locate online_retail_II.xlsx in any of the expected paths: {possible_paths}")

        # ETL Steps
        raw_df = extract_data(spark, raw_file)
        valid_df, invalid_df = transform_data(raw_df)
        
        # Load valid records to clean_transactions (excluding raw_timestamp)
        columns_to_load = [
            "invoice_no", "stock_code", "description", "quantity", 
            "unit_price", "total_amount", "customer_id", "country", 
            "transaction_timestamp", "is_cancellation", "processed_at"
        ]
        db_valid_df = valid_df.select(*columns_to_load)
        load_data(db_valid_df, "clean_transactions")
        
        # Load quarantined/invalid records to invalid_transactions using JSONB layout
        if invalid_df.count() > 0:
            from pyspark.sql.functions import to_json, struct
            # Convert row columns to JSON to audit the raw data
            audit_df = invalid_df.withColumn(
                "original_data", 
                to_json(struct([col(c) for c in invalid_df.columns if c != "raw_timestamp"]))
            ).withColumn(
                "rejection_reason", 
                lit("Missing customer_id or transaction_timestamp")
            ).select("rejection_reason", "original_data")
            
            load_data(audit_df, "invalid_transactions")
            
    except Exception as e:
        logger.error(f"Spark Job Failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
