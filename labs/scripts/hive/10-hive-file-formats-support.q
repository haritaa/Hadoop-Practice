# Launch beeline with HiveServer connectivity (Hive Server and Metastore needs to run to connect)
#$ beeline --silent=true -u jdbc:hive2://localhost:10000 username password

# Use Schema
USE hive_training;

## JSON Data Processing

# Create table with json serde
CREATE TABLE IF NOT EXISTS employee_json (eid int, name String, salary String, designation String)
COMMENT 'Employee details'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE;

# Loading data from local file system
LOAD DATA LOCAL INPATH '/home/ubuntu/danskeit_hadoop-pyspark/labs/dataset/empmgmt/employee.json' OVERWRITE INTO TABLE employee_json;

# Writing data to HDFS file system
INSERT OVERWRITE DIRECTORY '/user/training/empmgmt/output' SELECT * FROM employee_json;

# Writing data to local file system
INSERT OVERWRITE LOCAL DIRECTORY '/home/ubuntu/output' SELECT * FROM employee_json;

# Writing data to Hive table
CREATE TABLE emp LIKE employee_json;
INSERT OVERWRITE TABLE emp SELECT * FROM employee_json;

# Writing data in different formats

# Avro Format
CREATE TABLE emp_avro STORED AS AVRO AS SELECT * FROM emp;

# Parquet Format
CREATE TABLE emp_parquet STORED AS PARQUET AS SELECT * FROM emp;

# RC Format
CREATE TABLE emp_rc STORED AS RCFILE AS SELECT * FROM emp;

# ORC Format
CREATE TABLE emp_orc STORED AS ORC AS SELECT * FROM emp;

# Copy data from Avro to Parquet table
INSERT INTO emp_parquet SELECT * FROM emp_avro;

# Copy data from emp_parquet to ORC table
INSERT INTO emp_orc SELECT * FROM emp_parquet;