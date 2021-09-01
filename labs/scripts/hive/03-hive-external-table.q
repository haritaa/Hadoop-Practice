# Launch beeline with HiveServer connectivity (Hive Server and Metastore needs to run to connect)
#$ beeline --silent=true -u jdbc:hive2://localhost:10000 username password

# Use Schema
USE hive_training;

# Create external table
CREATE EXTERNAL TABLE IF NOT EXISTS EMPLOYEE_EXT (eid int, name String, salary String, designation String)
COMMENT 'Employee details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '/user/training/empmgmt/input';

# Describe table
DESCRIBE EMPLOYEE_EXT;

# Describe table with more details
DESCRIBE FORMATTED EMPLOYEE_EXT;

# Load Data into external table
LOAD DATA LOCAL INPATH '/home/ubuntu/danskeit_hadoop-pyspark/labs/dataset/empmgmt/employee.txt' OVERWRITE INTO TABLE EMPLOYEE_EXT;

# Query Data from managed table
SELECT * FROM EMPLOYEE_EXT;
SELECT * FROM EMPLOYEE_EXT WHERE SALARY >= 45000;
SELECT COUNT(*) FROM EMPLOYEE_EXT;
SELECT SUM(SALARY) FROM EMPLOYEE_EXT;

# Drop external table
DROP TABLE IF EXISTS EMPLOYEE_EXT;