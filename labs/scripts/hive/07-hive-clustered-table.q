# Launch beeline with HiveServer connectivity (Hive Server and Metastore needs to run to connect)
#$ beeline --silent=true -u jdbc:hive2://localhost:10000 username password

# Use Schema
USE hive_training;

# Create cluster table
CREATE TABLE IF NOT EXISTS EMPLOYEE_CLUS (eid int, name String, salary String, designation String)
COMMENT 'Employee details'
PARTITIONED BY (yoj String) CLUSTERED BY (eid) INTO 3 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

# Insert data into partition table
INSERT INTO TABLE EMPLOYEE_CLUS (eid, name, salary, designation, yoj) VALUES (100, 'Kumar', 50000, 'Admin', '2020');
INSERT INTO TABLE EMPLOYEE_CLUS (eid, name, salary, designation, yoj) VALUES (101, 'Anil', 60000, 'Technical lead', '2019');
INSERT INTO TABLE EMPLOYEE_CLUS (eid, name, salary, designation, yoj) VALUES (102, 'Arvind', 70000, 'Developer', '2020');
INSERT INTO TABLE EMPLOYEE_CLUS (eid, name, salary, designation, yoj) VALUES (103, 'Santhosh', 80000, 'Analyst', '2019');
INSERT INTO TABLE EMPLOYEE_CLUS (eid, name, salary, designation, yoj) VALUES (104, 'Satya', 40000, 'Manager', '2020');
INSERT INTO TABLE EMPLOYEE_CLUS (eid, name, salary, designation, yoj) VALUES (106, 'Vijay', 30000, 'Admin', '2021');

#SELECT Queries
SELECT * FROM EMPLOYEE_CLUS WHERE yoj = '2020';

SELECT * FROM EMPLOYEE_CLUS TABLESAMPLE(BUCKET 2 OUT OF 3);

SELECT * FROM EMPLOYEE_CLUS WHERE salary > 40000 AND designation = 'Admin' AND yoj = '2020';

SELECT designation, count(*) FROM EMPLOYEE_CLUS WHERE yoj = '2020' GROUP BY designation;

# Drop cluster table
DROP TABLE IF EXISTS EMPLOYEE_CLUS;