--1:  Creating a Partitioned Table for Prayer Times Data
CREATE DATABASE prayer_times

CREATE EXTERNAL TABLE IF NOT EXISTS prayer_times.prayer_times_2025_partitioned (
  Date STRING,
  Fajr STRING,
  Sunrise STRING,
  Dhuhr STRING,
  Asr STRING,
  Maghrib STRING,
  Isha STRING,
  Imsak STRING
)
PARTITIONED BY (ville STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://bucketadhentunis/'
TBLPROPERTIES ("skip.header.line.count"="1");


--2: Organize Data in S3 for Partitioning: aws s3 mv s3://bucketadhentunis/prayer_times_2025_Sfax.csv s3://bucketadhentunis/ville=Sfax/prayer_times.csv

--3: Load Partitions into Athena
MSCK REPAIR TABLE prayer_times.prayer_times_2025_partitioned;

--4: Verify Partitions
SHOW PARTITIONS prayer_times.prayer_times_2025_partitioned;


select * from prayer_times_2025_partitioned
where ville = 'Sfax'
