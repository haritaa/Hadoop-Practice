#Create table with CSV serde
CREATE TABLE marketing_data (
  age int,
  job string,
  marital string,
  education string,
  default string,
  balance double,
  housing string,
  loan string,
  contact int,
  day string,
  month int,
  duration int,
  campaign int,
  pdays int,
  previous int,
  poutcome string,
  y string )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ";"
)
STORED AS TEXTFILE
TBLPROPERTIES (
    'skip.header.line.count' = '1');

# Create View with type casting
CREATE VIEW marketing_data_view AS 
  SELECT 
    cast(age as int),
    job,
    marital,
    education,
    default,
    cast(balance as double),
    housing,
    loan,
    cast(contact as int),
    day,
    cast(month as int),
    cast(duration as int),
    cast(campaign as int),
    cast(pdays as int),
    cast(previous as int),
    poutcome,
    y    
  FROM marketing_data;

#Describe table/view details
DESCRIBE marketing_data;
DESCRIBE marketing_data_view;

#Load Data
LOAD DATA LOCAL INPATH '/home/ubuntu/danskeit_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv' OVERWRITE INTO TABLE marketing_data;

#Fetch marketing data
SELECT * FROM marketing_data;
SELECT * FROM marketing_data_view;

#1 Marketing Success Rate
select (a.subscribed/b.total)*100 as success_percent from (select count(*) as subscribed from marketing_data where y='yes') a,(select count(*) as total from marketing_data) b

#2 Marketing Failure Rate
select (a.not_subscribed/b.total)*100 as failure_percent from (select count(*) as not_subscribed from marketing_data where y='no') a,(select count(*) as total from marketing_data) b

#3 Max,Min, Mean age of targeted customer
select min(age) as min_age, max(age) as max_age, avg(age) as avg_age from marketing_data

#4 Avg and Median balance of customers
SELECT avg(balance) as avg_balance, percentile_approx(balance, 0.5) as median_balance FROM marketing_data_view

#5 Check if age matters in the marketing subscription for deposit
select age, count(*) as number from marketing_data where y='yes' group by age order by number desc

#6 Check if marital status matters
select marital, count(*) as number from marketing_data where y='yes' group by marital order by number desc

#7 Check if both matters
select age, marital, count(*) as number from marketing_data where y='yes' group by age,marital order by number desc


#Create table with Delimited Row Format
CREATE TABLE marketing_data_delim (
  age int,
  job string,
  marital string,
  education string,
  default string,
  balance double,
  housing string,
  loan string,
  contact int,
  day string,
  month int,
  duration int,
  campaign int,
  pdays int,
  previous int,
  poutcome string,
  y string )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE; 

DESCRIBE marketing_data_delim;

#Drop tables/views
DROP TABLE marketing_data;
DROP TABLE marketing_data_delim;
DROP VIEW marketing_data_view;
