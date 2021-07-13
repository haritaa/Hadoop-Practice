CREATE TABLE IF NOT EXISTS employee_comp ( eid int, name String,
salary double, designation String, contact array<string>, address MAP<string, string>, dept STRUCT<name:string,bu:string>)
COMMENT 'Employee details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '$'
MAP KEYS TERMINATED BY '#'
STORED AS TEXTFILE;	

describe formatted employee_comp;

LOAD DATA LOCAL INPATH '/home/ubuntu/danskeit_hadoop-pyspark/labs/dataset/empmgmt/employee-comp.txt' OVERWRITE INTO TABLE employee_comp;

SELECT * FROM employee_comp;

SELECT name, contact[0], contact[1], contact[2] FROM employee_comp;

SELECT name, contact[0], address['home'], address['office'], dept.name, dept.bu FROM employee_comp;

SELECT name, size(contact), size(address), map_keys(address), map_values(address), array_contains(contact, '8765432345'), sort_array(contact)  FROM employee_comp;