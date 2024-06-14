from pyspark.sql import SparkSession

def read_and_save_data():
    # Initialize Spark session in local mode
    spark = SparkSession.builder \
        .appName("BankingDataRead") \
        .master("local[*]") \
        .config("spark.jars", "/root/lib/postgresql-42.2.24.jar") \
        .getOrCreate()

    # Database connection properties
    db_url = "jdbc:postgresql://localhost:5432/postgres"
    db_properties = {
        "user": "postgres",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }

    # Read data into Spark DataFrames
    trans_df = spark.read.jdbc(url=db_url, table="trans", properties=db_properties)
    order_df = spark.read.jdbc(url=db_url, table="order_data", properties=db_properties)
  # Quoted table name
    account_df = spark.read.jdbc(url=db_url, table="account", properties=db_properties)
    client_df = spark.read.jdbc(url=db_url, table="client_data", properties=db_properties)
    disp_df = spark.read.jdbc(url=db_url, table="disp", properties=db_properties)
    district_df = spark.read.jdbc(url=db_url, table="district_data", properties=db_properties)
    loan_df = spark.read.jdbc(url=db_url, table="loan", properties=db_properties)

    # Save each DataFrame to Hadoop HDFS
    trans_df.write.mode("overwrite").parquet("hdfs://localhost:9000/user/hadoop/trans")
    order_df.write.mode("overwrite").parquet("hdfs://localhost:9000/user/hadoop/order")
    account_df.write.mode("overwrite").parquet("hdfs://localhost:9000/user/hadoop/account")
    client_df.write.mode("overwrite").parquet("hdfs://localhost:9000/user/hadoop/client")
    disp_df.write.mode("overwrite").parquet("hdfs://localhost:9000/user/hadoop/disp")
    district_df.write.mode("overwrite").parquet("hdfs://localhost:9000/user/hadoop/district")
    loan_df.write.mode("overwrite").parquet("hdfs://localhost:9000/user/hadoop/loan")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    read_and_save_data()