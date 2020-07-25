CREATE EXTERNAL TABLE stops_feed (
  stop_id string,
  route_id int,
  waiting_time bigint
)
PARTITIONED BY (date date, hour int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",") 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://[ NAME OF THE BUCKET FOR THE DATA INGESTION FEED ]/'
TBLPROPERTIES ('has_encrypted_data'='false')

###################################################################################################

CREATE EXTERNAL TABLE locations (
  stop_id string,
  route_id int,
  stop_name string,
  lat float,
  lon float,
  location string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",") 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://[ NAME OF THE BUCKET FOR STATIC FILES ]/locations/'
TBLPROPERTIES ('has_encrypted_data'='false')

###################################################################################################

CREATE EXTERNAL TABLE routes (
  route_id int,
  route_name string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ("separatorChar" = ",") 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://[ NAME OF THE BUCKET FOR STATIC FILES ]/routes/'
TBLPROPERTIES ('has_encrypted_data'='false')

