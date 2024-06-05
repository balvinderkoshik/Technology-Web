--/*
--  HIVE SCRIPT  : create_dim_time.hql
--  AUTHOR       : HEMANTH REDDY
--  DATE         : March 22, 2017
--  DESCRIPTION  : Creation of hive dim_time
--*/


USE ${hiveconf:database};
DROP TABLE IF EXISTS  ${hiveconf:namespace}${hiveconf:table_name};
CREATE EXTERNAL TABLE ${hiveconf:database}.${hiveconf:namespace}${hiveconf:table_name}(
time_key bigint,
seconds_since_midnight int,
second_number_in_minute int,
minute_number_in_hour int,
quarter_number_in_hour int,
hour_number_in_day int,
time_in_24hr_day string,
time_in_12hr_day string,
am_pm_indicator string,
last_updated_date TIMESTAMP,
batch_id STRING
)
PARTITIONED BY (status STRING)
STORED AS ORC
LOCATION '${hiveconf:location}'
;
