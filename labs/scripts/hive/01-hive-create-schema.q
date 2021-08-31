##Hive client connection
$ beeline

#Launch beeline disabling console logs
$ beeline --silent=true

#Launch beeline with HiveServer connectivity (Hive Server and Metastore needs to run to connect)
$ beeline --silent=true -u jdbc:hive2://localhost:10000 username password

#Connect with Hive Server (Hive Server and Metastore needs to run to connect)
!connect jdbc:hive2://localhost:10000 username password org.apache.hive.jdbc.HiveDriver

#Connect without Hive Server
#!connect jdbc:hive2:// username password org.apache.hive.jdbc.HiveDriver

# Lists databases (schemas)
SHOW schemas;

# Lists tables
SHOW tables;

# Create schema
CREATE SCHEMA hive_training;

# Use schema
USE hive_training;

# Drop schema
DROP SCHEMA hive_training;