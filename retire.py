import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from kudu.client import KuduClient, Partitioning
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename='retire_data.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_spark_session():
    spark = SparkSession.builder \
        .appName("RetireDataFromKudu") \
        .getOrCreate()
    return spark

def load_data_from_kudu(spark, kudu_master, kudu_table):
    try:
        df = spark.read \
            .format("org.apache.kudu.spark.kudu") \
            .option("kudu.master", kudu_master) \
            .option("kudu.table", kudu_table) \
            .load()
        logging.info(f"Data loaded successfully from Kudu table: {kudu_table}")
        return df
    except Exception as e:
        logging.error(f"Failed to load data from Kudu: {str(e)}")
        sys.exit(1)

def filter_retired_data(df, timestamp_column):
    try:
        one_year_ago = expr("current_timestamp() - INTERVAL 1 YEAR")
        retired_data = df.filter(col(timestamp_column) < one_year_ago)
        logging.info(f"Data filtered for retirement based on timestamp column: {timestamp_column}")
        return retired_data
    except Exception as e:
        logging.error(f"Failed to filter data: {str(e)}")
        sys.exit(1)

def write_to_ozone_s3(retired_data, ozone_s3_bucket):
    try:
        retired_data.write \
            .mode("append") \
            .parquet(ozone_s3_bucket)
        logging.info(f"Retired data successfully written to Ozone S3 bucket: {ozone_s3_bucket}")
    except Exception as e:
        logging.error(f"Failed to write data to Ozone S3: {str(e)}")
        sys.exit(1)

def delete_from_kudu(kudu_master, kudu_table, timestamp_column):
    try:
        client = KuduClient(kudu_master)
        table = client.table(kudu_table)
        one_year_ago = datetime.now().replace(microsecond=0) - timedelta(days=365)

        session = client.new_session()
        scanner = table.scanner()
        scanner.add_predicate(table.timestamp_column(timestamp_column).lt(one_year_ago))

        for result in scanner:
            session.apply(table.new_delete(result.primary_key()))
        
        session.flush()
        logging.info(f"Retired data deleted from Kudu table: {kudu_table}")
    except Exception as e:
        logging.error(f"Failed to delete data from Kudu: {str(e)}")
        sys.exit(1)

def main():
    kudu_master = "kudu-master:7051"
    kudu_table = "kudu_table"
    timestamp_column = "timestamp_column"
    ozone_s3_bucket = "s3a://ozone-bucket/retired_data/"

    # Create Spark session
    spark = create_spark_session()

    # Load data from Kudu
    df = load_data_from_kudu(spark, kudu_master, kudu_table)

    # Filter data for retirement
    retired_data = filter_retired_data(df, timestamp_column)

    # Write retired data to Ozone S3
    write_to_ozone_s3(retired_data, ozone_s3_bucket)

    # Delete retired data from Kudu
    delete_from_kudu(kudu_master, kudu_table, timestamp_column)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
