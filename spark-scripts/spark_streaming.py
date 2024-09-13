import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DoubleType
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

# Get environment variables
spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")
pg_user = os.getenv("POSTGRES_USER")
pg_password = os.getenv("POSTGRES_PASSWORD")
pg_db = os.getenv("POSTGRES_DW_DB")
pg_host = os.getenv("POSTGRES_CONTAINER_NAME")  
pg_port = os.getenv("POSTGRES_PORT")

# Construct PostgreSQL URL
pg_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

# Define the table name where the data will be stored
pg_table = "purchases_aggregated"

# Spark Configuration
spark_host = f"spark://{spark_hostname}:{spark_port}"

# Set PySpark submit arguments for the required packages
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

# Create Spark Context and Session
sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("KafkaStreaming").setMaster(spark_host))
)
sparkcontext.setLogLevel("WARN")
spark = SparkSession(sparkcontext)

# Define schema for the event
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("item", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("event_timestamp", StringType(), True)  
])

# Read stream from Kafka topic
stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", "10000")
    .load()
)

# Parse the JSON message and convert event_timestamp to timestamp type
parsed_df = stream_df.withColumn("value", F.col("value").cast("string"))  # Convert binary to string
parsed_df = parsed_df.withColumn("data", F.from_json(F.col("value"), schema)).select("data.*")

# === Cleaning Data ===
# Remove duplicates based on event_id
cleaned_df = parsed_df.dropDuplicates(["event_id"])

# Convert 'event_timestamp' to timestamp type using the correct format
cleaned_df = cleaned_df.withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp"), 'yyyy-MM-dd HH:mm:ss'))

# === Handling Late Data ===
# Add a column to flag late events
cleaned_df = cleaned_df.withWatermark("event_timestamp", "1 minute").withColumn(
    "is_late", F.when(F.col("event_timestamp") < F.current_timestamp() - F.expr("INTERVAL 1 MINUTE"), True).otherwise(False)
)

# Aggregate the total purchases over a 10 minute window with price multiplied by quantity
aggregated_df = (
    cleaned_df
    .withWatermark("event_timestamp", "10 minute")  # Set watermark for late data tolerance
    .groupBy(F.window(F.col("event_timestamp"), "10 minute").alias("time_window"))
    .agg(F.round(F.sum(F.col("price") * F.col("quantity")), 2).alias("running_total"))
    .select(F.col("time_window.start").alias("timestamp"), "running_total")
)

# === Analyze Data ===
# Analyze the number of late events
late_event_count_df = cleaned_df.groupBy("is_late").count()

# Function to write each batch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    try:
        # Ensure the schema is correct and processed properly
        batch_df = batch_df.withColumn("batch_id", F.lit(batch_id)) 
        batch_df.show()  

        # Write to PostgreSQL
        batch_df.write \
            .format("jdbc") \
            .option("url", pg_url) \
            .option("dbtable", pg_table) \
            .option("user", pg_user) \
            .option("password", pg_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print(f"Batch {batch_id} saved successfully to PostgreSQL")

    except Exception as e:
        print(f"Error writing to PostgreSQL for batch {batch_id}: {e}")

# Write the streaming aggregation to both console and PostgreSQL every 10 seconds
query = (
    aggregated_df
    .writeStream
    .foreachBatch(write_to_postgres)  # Write each micro-batch to PostgreSQL
    .outputMode("update")  # Update mode for aggregation
    .format("console")  # Write to console for debugging
    .option("checkpointLocation", "/logs/checkpoint-2")
    .trigger(processingTime="10 seconds")  # Trigger every 10 seconds
    .start()
)

# Analyze late events and display to console
late_event_query = (
    late_event_count_df
    .writeStream
    .outputMode("complete")
    .format("console")
    .trigger(processingTime="10 seconds")
    .start()
)

# Wait for the queries to finish
query.awaitTermination()
late_event_query.awaitTermination()
