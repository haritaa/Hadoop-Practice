"""
A simple example demonstrating basic Spark SQL features.
"""
from __future__ import print_function

from pyspark.sql import SparkSession

#For schema_inference_example
from pyspark.sql import Row

#For programmatic_schema_example
from pyspark.sql.types import *

def basic_df_example(spark):
    # create df
    # spark is an existing SparkSession
    df = spark.read.json("/home/ubuntu/danskeit_hadoop-pyspark/labs/dataset/customer/customer.json")
    # Displays the content of the DataFrame to stdout
    df.show()

    # Print the schema in a tree format
    df.printSchema()

    # Select only the "name" column
    df.select("name").show()

    # Select everybody, but increment the age by 1
    df.select(df['name'], df['age'] + 1).show()

    # Select customer older than 21
    df.filter(df['age'] > 21).show()

    # Count customer by age
    df.groupBy("age").count().show()

    # Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("customer")

    sqlDF = spark.sql("SELECT * FROM customer")
    sqlDF.show()

    # Register the DataFrame as a global temporary view
    df.createGlobalTempView("customer")

    # Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.customer").show()

    # Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.customer").show()


def schema_inference_example(spark):
	# Spark Context
    sc = spark.sparkContext

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("/home/ubuntu/danskeit_hadoop-pyspark/labs/dataset/customer/customer.txt")
    parts = lines.map(lambda l: l.split(","))
    customer = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

    # Infer the schema, and register the DataFrame as a table.
    schemacustomer = spark.createDataFrame(customer)
    schemacustomer.createOrReplaceTempView("customer")

    # SQL can be run over DataFrames that have been registered as a table.
    teenagers = spark.sql("SELECT name FROM customer WHERE age >= 13 AND age <= 19")

    # The results of SQL queries are Dataframe objects.
    # rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
    teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
    for name in teenNames:
        print(name)

		
def programmatic_schema_example(spark):
    # Spark Context
    sc = spark.sparkContext

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("/home/ubuntu/danskeit_hadoop-pyspark/labs/dataset/customer/customer.txt")
    parts = lines.map(lambda l: l.split(","))
	
    # Each line is converted to a tuple.
    customer = parts.map(lambda p: (p[0], p[1].strip()))

    # The schema is encoded in a string.
    schemaString = "name age"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemacustomer = spark.createDataFrame(customer, schema)

    # Creates a temporary view using the DataFrame
    schemacustomer.createOrReplaceTempView("customer")

    # SQL can be run over DataFrames that have been registered as a table.
    results = spark.sql("SELECT name FROM customer")

	# Displays the content of the DataFrame to stdout
    results.show()
  

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.driver.cores", "2") \
        .getOrCreate()

    basic_df_example(spark)
    schema_inference_example(spark)
    programmatic_schema_example(spark)

    spark.stop()