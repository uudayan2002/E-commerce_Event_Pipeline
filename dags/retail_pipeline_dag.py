from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'retail_transactional_pipeline',
    default_args=default_args,
    description='Big Data Pipeline for Retail Intelligence using PySpark',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    # 1. Run the PySpark ETL Job
    # This submits the job to the Spark Cluster
    run_spark_etl = SparkSubmitOperator(
        task_id='run_pyspark_retail_etl',
        application='/opt/airflow/scripts/retail_etl.py',
        conn_id='spark_default',
        verbose=True,
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.jars": "/opt/airflow/data/jars/postgresql-42.6.0.jar"
        }
    )

    # 2. Update Daily Sales Summary
    update_daily_summary = PostgresOperator(
        task_id='update_daily_sales_summary',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO daily_sales_summary (sales_date, total_revenue, order_count, unique_customers, cancellation_count)
            SELECT 
                DATE(transaction_timestamp) as sales_date,
                SUM(total_amount) as total_revenue,
                COUNT(DISTINCT invoice_no) as order_count,
                COUNT(DISTINCT customer_id) as unique_customers,
                COUNT(CASE WHEN is_cancellation THEN 1 END) as cancellation_count
            FROM clean_transactions
            GROUP BY DATE(transaction_timestamp)
            ON CONFLICT (sales_date) DO UPDATE SET
                total_revenue = EXCLUDED.total_revenue,
                order_count = EXCLUDED.order_count,
                unique_customers = EXCLUDED.unique_customers,
                cancellation_count = EXCLUDED.cancellation_count,
                last_updated_at = CURRENT_TIMESTAMP;
        """
    )

    # 3. Update Product Metrics
    update_product_metrics = PostgresOperator(
        task_id='update_product_metrics',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO product_metrics (stock_code, description, total_quantity_sold, total_revenue, transaction_count, last_sold_at)
            SELECT 
                stock_code,
                MAX(description) as description,
                SUM(quantity) as total_quantity_sold,
                SUM(total_amount) as total_revenue,
                COUNT(*) as transaction_count,
                MAX(transaction_timestamp) as last_sold_at
            FROM clean_transactions
            GROUP BY stock_code
            ON CONFLICT (stock_code) DO UPDATE SET
                total_quantity_sold = EXCLUDED.total_quantity_sold,
                total_revenue = EXCLUDED.total_revenue,
                transaction_count = EXCLUDED.transaction_count,
                last_sold_at = EXCLUDED.last_sold_at;
        """
    )

    run_spark_etl >> [update_daily_summary, update_product_metrics]
