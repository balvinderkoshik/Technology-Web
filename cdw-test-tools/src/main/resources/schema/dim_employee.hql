--/*
--  HIVE SCRIPT  : create_dim_employee.hql
--  AUTHOR       : NEHA MAHAJAN
--  DATE         : March 21, 2017
--  DESCRIPTION  : Creation of hive gold_dim_employee
--*/

USE ${hiveconf:database};
DROP TABLE IF EXISTS  ${hiveconf:namespace}${hiveconf:table_name};
CREATE EXTERNAL TABLE ${hiveconf:database}.${hiveconf:namespace}${hiveconf:table_name}
(
	employee_key BIGINT,
	employee_id STRING,
	member_key BIGINT,
	employee_status STRING,
	division_cd INT,	
	hire_date DATE,
	termination_date DATE,
	rehire_date DATE,
	discount_pct INT,
	full_part_time_ind STRING,
	location_cd STRING,
	clerk_id STRING,
	home_store_key INT,
	last_updated_date timestamp,
	batch_id STRING
)
PARTITIONED BY (status STRING)
STORED AS ORC
LOCATION '${hiveconf:location}';