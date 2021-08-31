# Launch beeline with HiveServer connectivity (Hive Server and Metastore needs to run to connect)
#$ beeline --silent=true -u jdbc:hive2://localhost:10000 username password

# Use Schema
USE hive_training;

## VIEWs
CREATE VIEW empcountry AS SELECT e.eid, e.name, c.name as country FROM employee1 e JOIN country c ON e.country = c.cid;

SELECT * FROM empcountry;

## Materialized VIEWs
CREATE MATERIALIZED VIEW empcountry_mat AS SELECT e.eid, e.name, c.name as country FROM employee1 e JOIN country c ON e.country = c.cid;

SELECT * FROM empcountry_mat;

# Drop views
DROP VIEW IF EXISTS empcountry;
DROP VIEW IF EXISTS empcountry_mat;