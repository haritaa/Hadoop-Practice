# Launch beeline with HiveServer connectivity (Hive Server and Metastore needs to run to connect)
#$ beeline --silent=true -u jdbc:hive2://localhost:10000 username password

# Use Schema
USE hive_training;

# Create managed table
CREATE TABLE IF NOT EXISTS EMPLOYEE (eid int, name String, salary String, designation String)
COMMENT 'Employee details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

# Describe table
DESCRIBE EMPLOYEE;

# Describe table with more details
DESCRIBE FORMATTED EMPLOYEE;

# Load Data into managed table
LOAD DATA LOCAL INPATH '/home/ubuntu/aq_hadoop-pyspark/labs/dataset/empmgmt/employee.txt' OVERWRITE INTO TABLE EMPLOYEE;

# Query Data from managed table
SELECT * FROM EMPLOYEE;
SELECT * FROM EMPLOYEE WHERE SALARY >= 45000;
SELECT COUNT(*) FROM EMPLOYEE;
SELECT SUM(SALARY) FROM EMPLOYEE;

# Drop managed table / schema
DROP TABLE IF EXISTS EMPLOYEE;