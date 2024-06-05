
--##############################################################################
--#                              general details                               #
--##############################################################################
--#                                                                            #
--# name         :   fact_transacion_detail                                    #
--#                                                                            #
--# file         :   create_fact_transacion_detail_hist.hql                    #
--#                                                                            #
--# description  :                                                             #
--#                                                                            #
--# date         :   may 3, 2017                                               #
--#                                                                            #
--# author       :    hemanth reddy                                            #
--#                                                                            #
--##############################################################################
--#                             table definitions                              #
--##############################################################################

use ${hiveconf:database};
drop table if exists ${hiveconf:namespace}${hiveconf:table_name};
create external table ${hiveconf:database}.${hiveconf:namespace}${hiveconf:table_name}(
transaction_detail_id bigint, 
trxn_detail_id string,
member_key bigint,
rid int,
trxn_id string,
trxn_nbr int,
register_nbr int,
trxn_seq_nbr int,
store_key bigint,
trxn_date date,
trxn_date_key bigint,
trxn_time_key bigint,
product_key bigint,
sku string,
transaction_type_key bigint,
purchase_qty int,
shipped_qty int,
units int,
original_price decimal(10,2),
retail_amount decimal(10,2),
total_line_amnt_after_discount decimal(10,2),
unit_price_after_discount decimal(10,2),
is_shipping_cost string,
discount_pct int,
discount_amount decimal(10,2),
promotion_key bigint,
discount_type_key bigint,
discount_ref_nbr int,
ean_used string,
ring_code_used int,
promotion_code int,
markdown_type string,
gift_card_sales_ind string,
division_id int,
associate_key bigint,
associate_nbr int,
cashier_id string,
salesperson string,
orig_trxn_nbr bigint,
post_void_ind string,
match_type_key bigint,
record_info_key bigint,
trxn_void_ind string,
plu_ind string,
plu_feature_id string,
currency_key bigint,
captured_loyalty_id bigint,
implied_loyalty_id bigint,
cogs decimal(10,2),
margin decimal(10,2),
last_updated_date timestamp,
batch_id string)
stored as orc
location '${hiveconf:location}';


