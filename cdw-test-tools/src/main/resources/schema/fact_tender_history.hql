--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     :   create_fact_tender_hist.hql                                        #
--# File                                                                       #
--#     :                                                                      #
--# Description                                                                #
--#     :   Creation of hive gold.fact_tender_hist	                       #
--#                                                                            #
--# Date :  May 05, 2017                                                       #
--#                                                                            #
--# Author                                                                     #
--#     :  Neha Mahajan                                                        #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################



USE ${hiveconf:database};
DROP TABLE IF EXISTS  ${hiveconf:namespace}${hiveconf:table_name};
CREATE EXTERNAL TABLE ${hiveconf:database}.${hiveconf:namespace}${hiveconf:table_name}(
fact_tender_hist_id bigint,
member_key bigint,
rid int,
trxn_id string,
trxn_nbr int,
register_nbr int,
trxn_tender_seq_nbr int,
store_key int,
trxn_date date,
trxn_date_key bigint,
division_id int,
associate_key bigint,
cashier_id string,
salesperson string,
associate_sales_flag string,
orig_trxn_nbr bigint,
reissue_flag string,
tender_amount decimal(10,2),
change_due decimal(10,2),
tokenized_cc_nbr string,
tokenized_cc_key bigint,
tender_type_key bigint,
check_auth_key bigint,
post_void_ind string,
check_auth_nbr int,
currency_key bigint,
captured_loyalty_id bigint,
implied_loyalty_id bigint,
last_updated_date timestamp,
batch_id string,
match_type_key bigint)
STORED AS ORC
LOCATION '${hiveconf:location}'
;
