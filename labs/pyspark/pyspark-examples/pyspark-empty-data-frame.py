
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType

spark = SparkSession.builder.appName('pyspark-examples').getOrCreate()

schema = StructType([
  StructField('firstname', StringType(), True),
  StructField('middlename', StringType(), True),
  StructField('lastname', StringType(), True)
  ])
df = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
df.printSchema()

df1 = spark.sparkContext.parallelize([]).toDF(schema)
df1.printSchema()

df2 = spark.createDataFrame([], schema)
df2.printSchema()

df3 = spark.emptyDataFrame()
df3.printSchema()





