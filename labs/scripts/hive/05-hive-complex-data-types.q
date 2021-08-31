#Launch beeline with HiveServer connectivity (Hive Server and Metastore needs to run to connect)
#$ beeline --silent=true -u jdbc:hive2://localhost:10000 username password

# Create table with complex data types
CREATE TABLE IF NOT EXISTS EMPLOYEE_COMP ( eid int, name String, salary double, designation String, contact array<string>, address MAP<string, string>, dept STRUCT<name:string,bu:string>)
COMMENT 'Employee details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '$'
MAP KEYS TERMINATED BY '#'
STORED AS TEXTFILE;	

# Describe table
DESCRIBE EMPLOYEE_COMP;

# Describe table with more details
DESCRIBE FORMATTED EMPLOYEE_COMP;

# Load data
LOAD DATA LOCAL INPATH '/home/ubuntu/aq_hadoop-pyspark/labs/dataset/empmgmt/employee-comp.txt' OVERWRITE INTO TABLE EMPLOYEE_COMP;

# Query Data
SELECT * FROM EMPLOYEE_COMP;

SELECT name, contact[0], contact[1], contact[2] FROM EMPLOYEE_COMP;

SELECT name, contact[0], address['home'], address['office'], dept.name, dept.bu FROM EMPLOYEE_COMP;

SELECT name, size(contact), size(address), map_keys(address), map_values(address), array_contains(contact, '8765432345'), sort_array(contact)  FROM EMPLOYEE_COMP;