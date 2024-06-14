from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'banking_data232323',
    default_args=default_args,
    description='A pipeline to process banking data with Spark and store in Hadoop HDFS and PostgreSQL',
    schedule_interval='@daily',
)

def read_and_save_data():
    # Code from bankspark.py
    from bankspark import read_and_save_data
    read_and_save_data()

def transform_data():
    # Initialize Spark session in local mode
    spark = SparkSession.builder \
        .appName("BankingDataTransform") \
        .master("local[*]") \
        .config("spark.jars", "/root/lib/postgresql-42.2.24.jar") \
        .getOrCreate()

    # Read data from Hadoop HDFS
    trans_df = spark.read.parquet("hdfs://localhost:9000/user/hadoop/trans")
    order_df = spark.read.parquet("hdfs://localhost:9000/user/hadoop/order")
    account_df = spark.read.parquet("hdfs://localhost:9000/user/hadoop/account")
    client_df = spark.read.parquet("hdfs://localhost:9000/user/hadoop/client")
    disp_df = spark.read.parquet("hdfs://localhost:9000/user/hadoop/disp")
    district_df = spark.read.parquet("hdfs://localhost:9000/user/hadoop/district")
    loan_df = spark.read.parquet("hdfs://localhost:9000/user/hadoop/loan")

    # Transform data: Join client and disp to get client-account relationship
    client_account_df = client_df.join(disp_df, client_df.client_id == disp_df.client_id) \
        .select(client_df["*"], disp_df.account_id)

    # Join client_account_df dengan account_df, menghindari duplikasi kolom district_id
    client_account_details_df = client_account_df.join(account_df, client_account_df.account_id == account_df.account_id) \
        .select(client_account_df["*"], account_df.statement_freq, account_df.date.alias("account_date"))

    # Additional transformations for other tables and analyses
    trans_count_per_account = trans_df.groupBy("account_id").count().withColumnRenamed("count", "trans_count")
    avg_transaction_amount = trans_df.groupBy("account_id").agg(
        {'amount': 'avg', 'amount': 'sum', 'balance': 'avg'}
    ).withColumnRenamed("avg(amount)", "avg_amount") \
     .withColumnRenamed("sum(amount)", "total_amount") \
     .withColumnRenamed("avg(balance)", "avg_balance")

    # Save the combined DataFrame to PostgreSQL
    # Database connection properties
    db_url = "jdbc:postgresql://localhost:5432/hasil_bank"
    db_properties = {
        "user": "postgres",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }

    client_account_details_df.write.jdbc(url=db_url, table="client_account_details", mode="overwrite", properties=db_properties)
    trans_count_per_account.write.jdbc(url=db_url, table="trans_count_per_account", mode="overwrite", properties=db_properties)
    avg_transaction_amount.write.jdbc(url=db_url, table="avg_transaction_amount", mode="overwrite", properties=db_properties)

    # Stop the Spark session
    spark.stop()

# Define Python tasks
read_and_save_task = PythonOperator(
    task_id='read_and_save_data',
    python_callable=read_and_save_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data', 
    python_callable=transform_data,
    dag=dag,
)

# Set task dependencies
read_and_save_task >> transform_task