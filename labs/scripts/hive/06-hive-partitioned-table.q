# Launch beeline with HiveServer connectivity (Hive Server and Metastore needs to run to connect)
#$ beeline --silent=true -u jdbc:hive2://localhost:10000 username password

# Use Schema
USE hive_training;

# Create partition table
CREATE TABLE IF NOT EXISTS EMPLOYEE_PART (eid int, name String, salary String, designation String)
COMMENT 'EMPLOYEE_PART details'
PARTITIONED BY (yoj String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

# Insert data into partition table
INSERT INTO TABLE EMPLOYEE_PART (eid, name, salary, designation, yoj) VALUES (100, 'Kumar', 50000, 'Admin', '2020');
INSERT INTO TABLE EMPLOYEE_PART (eid, name, salary, designation, yoj) VALUES (101, 'Anil', 60000, 'Technical lead', '2019');
INSERT INTO TABLE EMPLOYEE_PART (eid, name, salary, designation, yoj) VALUES (102, 'Arvind', 70000, 'Developer', '2020');
INSERT INTO TABLE EMPLOYEE_PART (eid, name, salary, designation, yoj) VALUES (103, 'Santhosh', 80000, 'Analyst', '2019');
INSERT INTO TABLE EMPLOYEE_PART (eid, name, salary, designation, yoj) VALUES (104, 'Satya', 40000, 'Manager', '2020');
INSERT INTO TABLE EMPLOYEE_PART (eid, name, salary, designation, yoj) VALUES (106, 'Vijay', 30000, 'Admin', '2021');

#SELECT Queries
SELECT * FROM EMPLOYEE_PART WHERE yoj = '2020';

SELECT * FROM EMPLOYEE_PART WHERE salary > 40000 AND designation = 'Admin' AND yoj = '2020';

SELECT designation, count(*) FROM EMPLOYEE_PART WHERE yoj = '2020' GROUP BY designation;

# Drop partition table
DROP TABLE IF EXISTS EMPLOYEE_PART;
