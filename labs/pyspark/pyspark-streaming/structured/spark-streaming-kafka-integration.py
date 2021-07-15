#STEP 1: Start Producer
#kafka-console-producer.sh --broker-list localhost:9092 --topic wordcount

#STEP 2: Start Spark Shell or Spark Submit with Kafka dependency to run this program
#pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 spark-streaming-kafka-integration.py

from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StringType, StructField, StructType
from pyspark.sql.functions import *

#Create Spark Session
spark = SparkSession \
    .builder \
    .appName("LoanStreamingAnalysis") \
    .getOrCreate()

#Consume message from Kafka topic and create Dataset
df = spark.readStream.format("kafka")\
      .option("kafka.bootstrap.servers", "localhost:9092")\
      .option("subscribe", "wordcount")\
	  .option("startingOffsets", "latest")\
	  .load()

#Print Schema
df.printSchema()

#Convert binary to string value
lines = df.selectExpr("CAST(value AS STRING)")

#Print Schema
df.printSchema()

words = lines.select(
	# explode turns each item in an array into a separate row
	explode(
		split(lines.value, ' ')
	).alias('word')
)

#Generate running word count
wordCounts = words.groupBy('word').count()

#Print Schema
wordCounts.printSchema()

#Start the stream - prints the output in the console
"""
query = wordCounts.writeStream \
  .outputMode("complete") \
  .format("console") \
  .start() \
  .awaitTermination()
"""

# Prints the output into Kafka topic
query = wordCounts\
    .selectExpr("CAST(word AS STRING) AS key", "CAST(count AS STRING) AS value") \
	.writeStream \
	.outputMode("complete") \
	.format("kafka") \
	.option("kafka.bootstrap.servers","localhost:9092") \
	.option("topic","wordcount-processed") \
	.option("checkpointLocation", "wordcount") \
	.start() \
	.awaitTermination()