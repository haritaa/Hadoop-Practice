#STEP 1: Start Producer
#kafka-console-producer.sh --broker-list localhost:9092 --topic loans
#kafka-console-producer.sh --broker-list localhost:9092 --topic loans < /home/ubuntu/danskeit_pyspark/labs/dataset/loan/loan.json

#STEP 2: Start Spark Shell or Spark Submit with Kafka dependency to run this program
#pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 loan-streaming-analysis-with-kafka.py

from pkg_resources._vendor.pyparsing import col

from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StringType, StructField, StructType
from pyspark.sql.functions import *

#Create Spark Session
spark = SparkSession \
    .builder \
    .appName("LoanStreamingAnalysis") \
    .getOrCreate()

#Consume message from Kafka topic and create Dataset
loansStreamingDF = spark.readStream.format("kafka")\
	                .option("kafka.bootstrap.servers", "localhost:9092")\
                    .option("subscribe", "loans")\
                    .option("kafka.group.id", "loans-consumer")\
	                .option("startingOffsets", "earliest")\
	                .load()

#Print Schema
loansStreamingDF.printSchema()

#Schema definition for consuming message
schema = StructType([ StructField("time", TimestampType(), True),
                      StructField("customer", StringType(), True),
                      StructField("loanId", StringType(), True),
                      StructField("status", StringType(), True)])

#Converting received kafka binary message to json message applying custom schema
loansStreamingJsonDF = loansStreamingDF.select(from_json(col("value").cast("String"), schema).alias("loans"))

#Print Schema
loansStreamingJsonDF.printSchema()

loansFlattenedDF = loansStreamingJsonDF.select("loans.time","loans.customer","loans.loanId","loans.status")

#Print Schema
loansFlattenedDF.printSchema()

"""
ds = loansFlattenedDF \
  .selectExpr("CAST(loanId AS STRING) AS key", "CAST(status AS STRING) AS value") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", "test1") \
  .option("checkpointLocation", "checkpoint123") \
  .start()
"""

#Group the data by window and status and compute the count of each group
loanStatusCountsWindowedDF = loansFlattenedDF\
    .groupBy( \
    window(loansFlattenedDF.time, "60 seconds", "30 seconds"),
    loansFlattenedDF.status
).count().orderBy('window')

#Print Schema
loanStatusCountsWindowedDF.printSchema()

outputDF =  loanStatusCountsWindowedDF\
                .selectExpr("CAST(status AS STRING) AS key", "to_json(struct(*)) AS value")

#Print Schema
outputDF.printSchema()

"""
#Start the stream - prints the output in the console
query = (
  outputDF
    .writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", "false")
    .start()
)
"""

#Start the stream - prints the output in the kafka topic
query = outputDF\
    .writeStream \
    .outputMode("complete") \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("topic","loans-processed") \
    .option("checkpointLocation", "loans") \
    .start()

query.awaitTermination()