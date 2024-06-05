--/*
--  HIVE SCRIPT  : create_dim_tender_type.hql
--  AUTHOR       : Neha Mahajan
--  DATE         : March 21, 2017
--  DESCRIPTION  : Creation of hive gold_dim_tender_type
--*/

USE ${hiveconf:database};
DROP TABLE IF EXISTS  ${hiveconf:namespace}${hiveconf:table_name};
CREATE EXTERNAL TABLE ${hiveconf:database}.${hiveconf:namespace}${hiveconf:table_name}
(
   tender_type_key BIGINT,
   lbi_payment_type_code STRING,
   tender_type_code STRING,
   tender_type_description STRING,
   is_cash STRING,
   is_check STRING,
   is_credit_card STRING,
   is_express_plcc STRING,
   is_gift_card STRING,
   is_bank_card STRING,
   last_updated_date TIMESTAMP,
   batch_id STRING
 )
PARTITIONED BY (status STRING)
STORED AS ORC
LOCATION '${hiveconf:location}';
