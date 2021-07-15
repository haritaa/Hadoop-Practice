"""
	Banking Marketing Data Analysis
"""
from __future__ import print_function

from pyspark.sql import SparkSession

spark = SparkSession \
	.builder \
	.appName("Bank Marketing Data Analysis") \
	.config("spark.driver.cores", "2") \
	.getOrCreate()

# Data Loading
df = spark.read.load("/home/ubuntu/danskeit_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv", format="csv", sep=";", inferSchema="true", header="true")

df.registerTempTable("bank")

#1 Marketing Success Rate
success = spark.sql("select (a.subscribed/b.total)*100 as success_percent from (select count(*) as subscribed from bank where y='yes') a,(select count(*) as total from bank) b").show()

#2 Marketing Failure Rate
failure = spark.sql("select (a.not_subscribed/b.total)*100 as failure_percent from (select count(*) as not_subscribed from bank where y='no') a,(select count(*) as total from bank) b").show()

#3 Max,Min, Mean age of targeted customer

agestats = spark.sql("select min(age), max(age), avg(age) from bank").show()

#4 Avg and Median balance of customers
median = spark.sql("SELECT percentile_approx(balance, 0.5) FROM bank").show()

#5 Check if age matters in the marketing subscription for deposit
age = spark.sql("select age, count(*) as number from bank where y='yes' group by age order by number desc").show()

#6 Check if marital status matters
marital = spark.sql("select marital, count(*) as number from bank where y='yes' group by marital order by number desc ").show()

#7 Check if both matters
age_marital = spark.sql("select age, marital, count(*) as number from bank where y='yes' group by age,marital order by number desc ").show()

spark.stop