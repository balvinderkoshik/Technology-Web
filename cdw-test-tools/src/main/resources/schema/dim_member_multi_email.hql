--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     :   create_member_multi_email.hql                                      #
--# File                                                                       #
--#     :                                                                      #
--# Description                                                                #
--#     :   Creation of hive gold.dim_member_multi_email                       #
--#                                                                            #
--# Date :  March 24, 2017                                                     #
--#                                                                            #
--# Author                                                                     #
--#     :  Aditi Chauhan                                                       #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################

USE ${hiveconf:database};
DROP TABLE IF EXISTS  ${hiveconf:namespace}${hiveconf:table_name};
CREATE EXTERNAL TABLE ${hiveconf:database}.${hiveconf:namespace}${hiveconf:table_name}(
member_multi_email_key bigint,
member_key bigint,
email_address string,
first_pos_consent_date date,
consent_date_retail date,
consent_value_retail string,
consent_date_outlet date,
consent_value_outlet string,
consent_date_next date,
consent_value_next string,
email_consent string,
email_consent_date date,
valid_email string,
best_email_flag string,
best_member string,
is_loyalty_email string,
source_key bigint,
original_source_key bigint,
original_store_key bigint,
active_inactive_flag string,
gender_version string,
frequency string,
affiliate_id bigint,
sub_list_id bigint,
consent_history_id string,
mme_id string,
welcome_email_consent_date date,
MATCH_TYPE_KEY BIGINT,
last_updated_date timestamp,
batch_id string,
rid string)
partitioned by (status string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u0001'
stored as orc
location '${hiveconf:location}'
;


