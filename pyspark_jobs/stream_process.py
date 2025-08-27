from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, MapType

# Define your Spark session
spark = SparkSession.builder \
    .appName("StockKafkaStreamProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Kafka topic config
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "stock_topic"             

# Define schema to parse Alpha Vantage JSON data (outer structure)
schema = StructType([
    StructField("Meta Data", MapType(StringType(), StringType())),
    StructField("Time Series (1min)", MapType(StringType(), StringType()))  # Notice inner MapType replaced with StringType
])

# Define schema for inner details (the string JSON inside the Time Series map)
details_schema = StructType([
    StructField("1. open", StringType()),
    StructField("2. high", StringType()),
    StructField("3. low", StringType()),
    StructField("4. close", StringType()),
    StructField("5. volume", StringType()),
])

# Read stream from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Cast Kafka value from bytes to string
df_string = df.selectExpr("CAST(value AS STRING)")

# Parse JSON column with outer schema
df_json = df_string.select(from_json(col("value"), schema).alias("data"))

# Explode the map (timestamp -> details string)
df_exploded = df_json.selectExpr("explode(data.`Time Series (1min)`) as (timestamp, details)")

# Parse the nested JSON string in details column
df_parsed = df_exploded.withColumn("parsed_details", from_json(col("details"), details_schema))

# Extract individual fields from parsed_details
df_final = df_parsed.select(
    col("timestamp"),
    col("parsed_details.`1. open`").alias("open"),
    col("parsed_details.`2. high`").alias("high"),
    col("parsed_details.`3. low`").alias("low"),
    col("parsed_details.`4. close`").alias("close"),
    col("parsed_details.`5. volume`").alias("volume")
)

# Convert volume to integer (or float if needed) and filter where volume > 100
df_filtered = df_final.withColumn("volume_int", col("volume").cast("int")) \
                      .filter(col("volume_int") > 200) \
                      .drop("volume_int")

# # # Write output to console for testing
# query = df_final.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# query.awaitTermination()

# SQL Server JDBC config
sqlserver_url = "jdbc:sqlserver://localhost:1433;databaseName=msdb;encrypt=false;trustServerCertificate=true"
sqlserver_properties = {
    "user": "sa",
    "password": "NewStrongPassword123!",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

checkpoint_dir = "/tmp/spark_checkpoints/stock_to_sqlserver"

# Write each micro-batch to SQL Server table 'stock_prices'
def write_to_sqlserver(batch_df, batch_id):
    batch_df.write \
        .mode("append") \
        .jdbc(sqlserver_url, "stock_prices", properties=sqlserver_properties)

query = df_filtered.writeStream \
    .foreachBatch(write_to_sqlserver) \
    .option("checkpointLocation", checkpoint_dir) \
    .outputMode("append") \
    .start()

query.awaitTermination()


# spark-submit \
#   --jars /Users/akshit/Developer/jars/mssql-jdbc-12.10.1.jre11.jar \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
#   stream_process.py

#source /Users/akshit/Developer/stock-data-pipeline/venv/bin/activate