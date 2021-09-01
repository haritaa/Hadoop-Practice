# Launch beeline with HiveServer connectivity (Hive Server and Metastore needs to run to connect)
#$ beeline --silent=true -u jdbc:hive2://localhost:10000 username password

# Use Schema
USE hive_training;

# Create employee table
CREATE EXTERNAL TABLE IF NOT EXISTS employee1 (eid int, name String, age int, gender String, salary double, designation String, department String, country int)
COMMENT 'Employee details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/training/empmgmt/input';

# Load Data
LOAD DATA LOCAL INPATH '/home/ubuntu/danskeit_hadoop-pyspark/labs/dataset/empmgmt/employee1.txt' OVERWRITE INTO TABLE employee1;

# Create country table
CREATE EXTERNAL TABLE IF NOT EXISTS country (cid int, code String, name String)
COMMENT 'Country details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '/user/training/country/input';

# Load Data
LOAD DATA LOCAL INPATH '/home/ubuntu/danskeit_hadoop-pyspark/labs/dataset/empmgmt/country.txt' OVERWRITE INTO TABLE country;

## JOIN Queries

#Inner Join
SELECT e.eid, e.name, c.name as country FROM employee1 e JOIN country c ON e.country = c.cid;

#Full Outer Join
SELECT e.eid, e.name, c.name as country FROM employee1 e FULL OUTER JOIN country c ON e.country = c.cid;

#Left Outer Join
SELECT e.eid, e.name, c.name as country FROM employee1 e LEFT OUTER JOIN country c ON e.country = c.cid;

#Right Outer Join
SELECT e.eid, e.name, c.name as country FROM employee1 e RIGHT OUTER JOIN country c ON e.country = c.cid;

# Drop tables
DROP TABLE IF EXISTS employee1;
DROP TABLE IF EXISTS country;
