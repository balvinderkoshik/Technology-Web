--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     :   create_fact_card_history.hql                                       #
--# File                                                                       #
--#     :                                                                      #
--# Description                                                                #
--#     :   Creation of hive gold.fact_card_history                            #
--#                                                                            #
--# Date :  March 24, 2017                                                     #
--#                                                                            #
--# Author                                                                     #
--#     :  Akshay Rochwani                                                     #
--#                                                                            #
--##############################################################################
--#                                   Load                                     #
--##############################################################################



USE ${hiveconf:database};
DROP TABLE IF EXISTS  ${hiveconf:namespace}${hiveconf:table_name};
CREATE EXTERNAL TABLE ${hiveconf:database}.${hiveconf:namespace}${hiveconf:table_name}(
  card_history_id bigint,
  tokenized_cc_nbr string,
  card_type_key int,
  member_key bigint,
  rid bigint,
  account_open_date date,
  application_store_key int,
  last_purchase_date date,
  current_otb int,
  ytd_purchase_amt decimal(10,2),
  behavior_score int,
  credit_term_nbr int,
  cycle_nbr int,
  mgn_id string,
  previous_account_nbr string,
  credit_limit decimal(10,2),
  balance_all_plans decimal(10,2),
  application_source_code int,
  recourse_ind string,
  nbr_of_cards_issued int,
  activation_flag string,
  division_nbr int,
  last_4_of_account_nbr int,
  amt_of_last_payment decimal(10,2),
  amt_of_last_purchase decimal(10,2),
  logo int,
  closed_date date,
  cycle_update_date date,
  card_issue_date date,
  e_statement_ind string,
  e_statement_change_date date,
  last_return_date date,
  new_account_clerk_nbr int,
  old_open_close_ind string,
  marketable_ind string,
  is_primary_account_holder string,
  do_not_statement_insert string,
  do_not_sell_name string,
  spam_indicator string,
  email_change_date date,
  return_mail_ind string,
  ip_code int,
  loyalty_id bigint,
  vc_key bigint,
  lw_link_key bigint,
  status_code int,
  issued_date date,
  expiration_date date,
  registration_date date,
  unregistration_date date,
  first_card_txn_date date,
  last_card_txn_date date,
  lw_is_primary string,
  closed_date_key bigint,
  cycle_update_date_key bigint,
  card_issue_date_key bigint,
  e_statement_change_date_key bigint,
  last_return_date_key bigint,
  issued_date_key bigint,
  expiration_date_key bigint,
  registration_date_key bigint,
  unregistration_date_key bigint,
  first_card_txn_date_key bigint,
  last_card_txn_date_key bigint,
  open_close_ind string,
  last_purchase_date_key bigint,
  account_open_date_key bigint,
  last_updated_date timestamp,
  match_type_key bigint,
  source_key bigint,
  batch_id string)
PARTITIONED BY (status string)
STORED AS ORC
LOCATION '${hiveconf:location}'
;
