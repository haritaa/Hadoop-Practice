from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StringType, StructField, StructType
from pyspark.sql.functions import window

# Path to our loan JSON files
inputPath = "/home/ubuntu/danskeit_hadoop-pyspark/labs/dataset/loan"

spark = SparkSession \
    .builder \
    .appName("LoanStreamingAnalysis") \
    .getOrCreate()

# Explicitly set schema
schema = StructType([ StructField("time", TimestampType(), True),
                      StructField("customer", StringType(), True),
                      StructField("loanId", StringType(), True),
                      StructField("status", StringType(), True)])

# Create streaming data source
loansStreamingDF = (
  spark
    .readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 1)
    .json(inputPath)
)

"""
# Loan Status counts
loanStatusCountsDF = (
  loansStreamingDF
    .groupBy(
      loansStreamingDF.status
    )
    .count()
)
"""

# Group the data by window and status and compute the count of each group
loanStatusCountsWindowedDF = loansStreamingDF.withWatermark("time", "30 seconds") \
    .groupBy( \
    window(loansStreamingDF.time, "10 seconds", "5 seconds"),
    loansStreamingDF.status
).count().orderBy('window')

"""
# Group the data by window and word and compute the count of each group with watermark threshold to handle late data
loanStatusCountsWindowedDF = loansStreamingDF.withWatermark("time", "30 seconds") \
    .groupBy( \
    window(loansStreamingDF.time, "10 seconds", "5 seconds"),
    loansStreamingDF.status
).count().orderBy('window')
"""

# Stream output to console
query = (
  loanStatusCountsWindowedDF
    .writeStream
    .format("console")
    .outputMode("complete")
    .start()
)

"""
#write output to file
query = (
  loanStatusCountsWindowedDF
    .writeStream
    .format("json")
    .option("checkpointLocation", "checkpoint")
    .option("path","output")
    .start()
)
"""

"""
#write output to in-memory table
query = (
  loanStatusCountsWindowedDF
    .writeStream
    .format('memory')
    .queryName('loans')
    .start()
)
"""

query.awaitTermination()