# Launch beeline with HiveServer connectivity (Hive Server and Metastore needs to run to connect)
#$ beeline --silent=true -u jdbc:hive2://localhost:10000 username password

#Use schema
USE hive_training;

#SELECT Queries
SELECT * FROM EMPLOYEE WHERE salary > 50000 AND designation = 'Technical lead';

SELECT * FROM EMPLOYEE WHERE salary > 50000 AND designation LIKE '%lead';

SELECT * FROM EMPLOYEE WHERE salary between 30000 and 50000 AND designation LIKE '%lead';

SELECT * FROM EMPLOYEE WHERE salary IN (50000,60000) AND designation LIKE '%lead';

SELECT * FROM EMPLOYEE WHERE salary NOT IN (50000,60000);

SELECT * FROM EMPLOYEE WHERE EXISTS (SELECT eid FROM EMPLOYEE WHERE salary IN (75000,60000));

SELECT * FROM EMPLOYEE e1 WHERE EXISTS (SELECT eid FROM EMPLOYEE e2 WHERE salary IN (75000,60000) AND e1.eid = e2.eid);

SELECT * FROM EMPLOYEE e1 WHERE NOT EXISTS (SELECT eid FROM EMPLOYEE e2 WHERE salary IN (75000,60000) AND e1.eid = e2.eid);

SELECT designation, count(*) FROM EMPLOYEE GROUP BY designation;

SELECT designation, count(*) FROM EMPLOYEE GROUP BY designation HAVING count(*) > 1;

SELECT * FROM EMPLOYEE LIMIT 5;

SELECT * FROM EMPLOYEE ORDER BY name;

SELECT * FROM EMPLOYEE ORDER BY name desc;

SELECT * FROM EMPLOYEE SORT BY name;

SELECT * FROM EMPLOYEE SORT BY name desc;