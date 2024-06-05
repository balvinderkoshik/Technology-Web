package com.express.processing.fact

/**
  * Created by aditi.chauhan on 5/23/2017.
  */

import com.express.cdw.spark.udfs._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import com.express.cdw.spark.DataFrameUtils._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{current_timestamp, lit, _}

object FactTransactionSummary extends FactLoad {

  override def factTableName = "fact_transaction_summary"

  override def surrogateKeyColumn = "transaction_summary_id"

  override def transform: DataFrame = {

    val tempFactTable = hiveContext.table(s"$workDB.fact_transaction_summary_temp")
                                   .drop(col("transaction_summary_id"))
    val dimMemberDF = hiveContext.sql(s"select member_key,member_acct_enroll_date,longitude as lon_mem,latitude as lat_mem from $goldDB.dim_member where status='current'")
    val dimstoreDF = hiveContext.sql(s"""select store_key,latitude  as lat_store,longitude as lon_store,district_code from $goldDB.dim_store_master
                                                where status='current' """)

    val factTenderHistory = hiveContext.sql(s"select * from $workDB.fact_tender_history_temp")


    import hiveContext.implicits._

    /*************************************************Creating fact_transaction_detail dataset for fact_transaction_summary ***********************************************************************************/

    val factTransactionDetailDataset = hiveContext.sql(
      s"""select trxn_id,transaction_detail_id,member_key,
        store_key,trxn_date,trxn_date_key,purchase_qty,units,
        retail_amount,total_line_amnt_after_discount,ring_code_used,
        discount_amount,match_type_key,record_info_key,currency_key,
        original_price,gift_card_sales_ind,is_shipping_cost,product_key,captured_loyalty_id,
        implied_loyalty_id,cogs,margin,sku
        from $workDB.fact_transaction_detail_temp order by trxn_id desc""")

    val dimProductDataset = hiveContext.sql(s"select gender,nvl(product_key,-1) as product_key from $goldDB.dim_product where status='current'")

    val factTenderHistDataset = hiveContext.sql(
      s"""select trxn_id as tender_trxn_id ,max(tender_hist_key) as max_tender_id,
      nvl(sum(express_plcc_amount),0) as express_plcc_amount ,
      nvl(sum(credit_card_amount),0) as credit_card_amount,
      nvl(sum(cash_amount),0) as cash_amount ,
      nvl(sum(check_amount),0) as check_amount,
      nvl(sum(gift_card_amount),0) as gift_card_amount,
      nvl(sum(other_amount),0) as other_amount
      from (select trxn_id,tender_type_key,max(fact_tender_hist_id) as tender_hist_key,
      (case when tender_type_key in (1,2,25) then sum(tender_amount) else 0 end) as express_plcc_amount,
      (case when tender_type_key in (21,19,24,3,7,6,27,28,5,32) then sum(tender_amount) else 0 end) as credit_card_amount,
      (case when tender_type_key in  (4,35) then sum(tender_amount) else 0 end) as cash_amount,
      (case when tender_type_key in (16,23) then sum(tender_amount) else 0 end) as check_amount,
      (case when tender_type_key in (11,9,29,18,22) then sum(tender_amount) else 0 end) as gift_card_amount,
      (case when tender_type_key not in (11,9,29,18,22,16,23,35,21,19,24,3,7,6,27,28,5,32,1,2,25,4) then sum(tender_amount) else 0 end) as other_amount
      from $workDB.fact_tender_history_temp group by trxn_id,tender_type_key) tender_history_dataset
      group by trxn_id""")


    val customerDataset = hiveContext.sql(
      s"""select transaction_date,transaction_store,transaction_register,transaction_nbr,
      nvl((redeem_1+redeem_2+redeem_3+redeem_4+redeem_5),0)  as reward_certs_qty
      from
      (select transaction_date,transaction_store,transaction_register,transaction_nbr,
      sum((case when ascii(redeemed_reward_cert_number_1) != 0 and not(isnull(redeemed_reward_cert_number_1))  then 1  else 0 end)) as redeem_1,
      sum((case when ascii(redeemed_reward_cert_number_2) != 0 and not(isnull(redeemed_reward_cert_number_2)) then 1 else 0 end)) as redeem_2,
      sum((case when ascii(redeemed_reward_cert_number_3) != 0 and not(isnull(redeemed_reward_cert_number_3)) then 1 else 0 end)) as redeem_3,
      sum((case when ascii(redeemed_reward_cert_number_4) != 0 and not(isnull(redeemed_reward_cert_number_4)) then 1 else 0 end)) as redeem_4,
      sum((case when ascii(redeemed_reward_cert_number_5) != 0 and not(isnull(redeemed_reward_cert_number_5)) then 1 else 0 end)) as redeem_5
      from $workDB.work_customer_dedup group by transaction_date,transaction_store,transaction_register,transaction_nbr)
      customer_dataset """)
      .select(GetTrxnIDUDF($"transaction_date", $"transaction_store", $"transaction_register", $"transaction_nbr")
        .alias("trxn_id"), $"reward_certs_qty")
      .join(factTenderHistory.filter("tender_type_key in ('12','11')").select("trxn_id").distinct,Seq("trxn_id"))
      .select("trxn_id","reward_certs_qty")


    /*dataframe formed after joining fact_transaction_detail and dim_product on product_key*/


    val trxnDetailProduct = factTransactionDetailDataset
      .join(dimProductDataset, Seq("product_key"), "left")
      .select("gender", factTransactionDetailDataset.columns: _*)
      .drop("product_key")

    /*Dataframe Created with columns required for condition based aggregation*/

    val transformedColumns = trxnDetailProduct
      .withColumn("w_purchase_qty", when(not(isnull($"gender")) and upper(trim($"gender")) === "W", $"purchase_qty").otherwise(lit(0)))
      .withColumn("m_purchase_qty", when(not(isnull($"gender")) and upper(trim($"gender")) === "M", $"purchase_qty").otherwise(lit(0)))
      .withColumn("gc_purchase_qty", when(not(isnull($"gift_card_sales_ind")) and (upper(trim($"gift_card_sales_ind")) === "Y" or upper(trim($"gift_card_sales_ind")) === "YES" ), $"purchase_qty").otherwise(lit(0)))
      .withColumn("other_purchase_qty", when(((upper(trim($"gender")) !== "W") and (upper(trim($"gender")) !== "M") and (upper(trim($"gift_card_sales_ind")) === "N") and (upper(trim(col("is_shipping_cost"))) === "NO")) or (isnull($"gender") and (upper(trim($"gift_card_sales_ind")) === "N") and (upper(trim(col("is_shipping_cost"))) === "NO")), $"purchase_qty").otherwise(lit(0.0)))
      .withColumn("w_units", when(not(isnull($"gender")) and upper(trim($"gender")) === "W", $"units").otherwise(lit(0)))
      .withColumn("m_units", when(not(isnull($"gender")) and upper(trim($"gender")) === "M", $"units").otherwise(lit(0)))
      .withColumn("gc_units", when(not(isnull($"gift_card_sales_ind")) and (upper(trim($"gift_card_sales_ind")) === "Y" or upper(trim($"gift_card_sales_ind")) === "YES"), $"units").otherwise(lit(0)))
      .withColumn("other_units", when(((upper(trim($"gender")) !== "W") and (upper(trim($"gender")) !== "M") and (upper(trim($"gift_card_sales_ind")) === "N") and (upper(trim(col("is_shipping_cost"))) === "NO")) or (isnull($"gender") and (upper(trim($"gift_card_sales_ind")) === "N")and (upper(trim(col("is_shipping_cost"))) === "NO")), $"units").otherwise(lit(0.0)))
      .withColumn("w_amount_after_discount", when(not(isnull($"gender")) and upper(trim($"gender")) === "W", $"total_line_amnt_after_discount").otherwise(lit(0.0)))
      .withColumn("m_amount_after_discount", when(not(isnull($"gender")) and upper(trim($"gender")) === "M", $"total_line_amnt_after_discount").otherwise(lit(0.0)))
      .withColumn("gc_amount_after_discount", when(not(isnull($"gift_card_sales_ind")) and (upper(trim($"gift_card_sales_ind")) === "Y" or upper(trim($"gift_card_sales_ind")) === "YES"), $"total_line_amnt_after_discount").otherwise(lit(0.0)))
      .withColumn("other_amount_after_discount", when(((upper(trim($"gender")) !== "W") and (upper(trim($"gender")) !== "M") and (upper(trim($"gift_card_sales_ind")) === "N") and (upper(trim(col("is_shipping_cost"))) === "NO")) or (isnull($"gender") and (upper(trim($"gift_card_sales_ind")) === "N") and (upper(trim(col("is_shipping_cost"))) === "NO")), $"total_line_amnt_after_discount").otherwise(lit(0.0)))
      .withColumn("shipping_amount", when(not(isnull($"is_shipping_cost")) and (upper(trim($"is_shipping_cost")) === "Y" or upper(trim($"is_shipping_cost")) === "YES"), $"retail_amount").otherwise(lit(0.0)))
      .withColumn("discount_amount_8888", when($"ring_code_used" === 8888, $"discount_amount").otherwise(lit(0.0)))
      .withColumn("discount_amount_no_8888", when($"ring_code_used" !== 8888, $"discount_amount").otherwise(lit(0.0)))
      .withColumn("out_w_purchase_qty", when(round($"original_price") === ($"original_price" + 0.01) and upper(trim($"gender")) === "W" and not(isnull($"gender")), $"purchase_qty").otherwise(lit(0)))
      .withColumn("out_m_purchase_qty", when(round($"original_price") === ($"original_price" + 0.01) and not(isnull($"gender")) and upper(trim($"gender")) === "M", $"purchase_qty").otherwise(lit(0)))
      .withColumn("out_w_units", when(round($"original_price") === ($"original_price" + 0.01) and upper(trim($"gender")) === "W" and not(isnull($"gender")), $"units").otherwise(lit(0)))
      .withColumn("out_m_units", when(round($"original_price") === ($"original_price" + 0.01) and upper(trim($"gender")) === "M" and not(isnull($"gender")), $"units").otherwise(lit(0)))
      .withColumn("out_w_amount_after_discount", when(round($"original_price") === ($"original_price" + 0.01) and upper(trim($"gender")) === "W" and not(isnull($"gender")), $"total_line_amnt_after_discount").otherwise(lit(0.0)))
      .withColumn("out_m_amount_after_discount", when(round($"original_price") === ($"original_price" + 0.01) and upper(trim($"gender")) === "M" and not(isnull($"gender")), $"total_line_amnt_after_discount").otherwise(lit(0.0)))

    logger.info("Dataframe Created with columns required for condition based aggregation")

    /*Aggregated Dataframe grouped over trxn_id to create a summary record for each transaction*/


    val aggregatedResultSet = transformedColumns
      .groupBy($"trxn_id")
      .agg(max($"member_key").alias("member_key"),
        max($"store_key").alias("store_key"),
        max($"trxn_date").alias("trxn_date"),
        max($"trxn_date_key").alias("trxn_date_key"),
        max($"transaction_detail_id").alias("max_detail_id"),
        sum($"w_purchase_qty").alias("w_purchase_qty"),
        sum($"m_purchase_qty").alias("m_purchase_qty"),
        sum($"gc_purchase_qty").alias("gc_purchase_qty"),
        sum($"other_purchase_qty").alias("other_purchase_qty"),
        sum($"w_units").alias("w_units"),
        sum($"m_units").alias("m_units"),
        sum($"gc_units").alias("gc_units"),
        sum($"other_units").alias("other_units"),
        sum($"w_amount_after_discount").alias("w_amount_after_discount"),
        sum($"m_amount_after_discount").alias("m_amount_after_discount"),
        sum($"gc_amount_after_discount").alias("gc_amount_after_discount"),
        sum($"other_amount_after_discount").alias("other_amount_after_discount"),
        sum($"shipping_amount").alias("shipping_amount"),
        sum($"discount_amount_8888").alias("discount_amount_8888"),
        sum($"discount_amount_no_8888").alias("discount_amount_no_8888"),
        max($"match_type_key").alias("match_type_key"),
        max($"record_info_key").alias("record_info_key"),
        max($"currency_key").alias("currency_key"),
        when(sum($"sku").isNull,max($"gift_card_sales_ind")).otherwise(min($"gift_card_sales_ind")).alias("gift_card_sales_only_ind"),
        sum($"out_w_purchase_qty").alias("out_w_purchase_qty"),
        sum($"out_m_purchase_qty").alias("out_m_purchase_qty"),
        sum($"out_w_units").alias("out_w_units"),
        sum($"out_m_units").alias("out_m_units"),
        sum($"out_w_amount_after_discount").alias("out_w_amount_after_discount"),
        sum($"out_m_amount_after_discount").alias("out_m_amount_after_discount"),
        max($"captured_loyalty_id").alias("captured_loyalty_id"),
        max($"implied_loyalty_id").alias("implied_loyalty_id"),
        sum($"cogs").alias("cogs"),
        sum($"margin").alias("margin")
      )
      .withColumn("discount_amount", $"discount_amount_no_8888")
      .withColumn("ttl_purchase_qty", $"w_purchase_qty" + $"m_purchase_qty" + $"gc_purchase_qty" + $"other_purchase_qty")
      .withColumn("ttl_purchase_units", $"w_units" + $"m_units" + $"gc_units" + $"other_units")
      .withColumn("ttl_amount_after_discount", $"w_amount_after_discount" + $"m_amount_after_discount" + $"gc_amount_after_discount" + $"other_amount_after_discount")
      .withColumn("ttl_out_purchase_qty", $"out_w_purchase_qty" + $"out_m_purchase_qty")
      .withColumn("ttl_out_units", $"out_w_units" + $"out_m_units")
      .withColumn("ttl_out_amount_after_discount", $"out_w_amount_after_discount" + $"out_m_amount_after_discount")
      .withColumn("distance_traveled", lit(null))
      .withColumn("enrollment_trxn_flag", lit(null))
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_updated_date", current_timestamp)

    /*Joining the aggregated result set with the consolidated dataframe factTenderHistDataset to retrieve express_plcc_amount
    credit_card_amount,cash_amount,check_amount,gift_card_amount,other_amount and max_tender_id
     */

    val joinedTenderHistDataset = aggregatedResultSet
      .join(factTenderHistDataset, aggregatedResultSet.col("trxn_id") === factTenderHistDataset.col("tender_trxn_id"), "left")
      .withColumn("ttl_amount", $"express_plcc_amount" + $"credit_card_amount" + $"cash_amount" + $"check_amount" + $"gift_card_amount" + $"other_amount")
      .withColumn("has_tender", when(isnull(col("tender_trxn_id")), lit("NO")).otherwise("YES"))
      .withColumn("has_detail", lit("YES"))
      .drop(col("tender_trxn_id"))


    /*Joining the above dataframe with CustomerDataset to retrieve transformed column reward_certs_qty for each summarized record*/
   val factTrxnDetailDF = joinedTenderHistDataset.join(customerDataset, Seq("trxn_id"), "left")
      .withColumn("reward_certs_qty", $"reward_certs_qty").select(tempFactTable.getColumns: _*)

    /****************************************************Creating the dataframe for fact_tender_history****************************************************************************************/

    //val factTenderHistory = hiveContext.sql(s"select * from $workDB.fact_tender_history_temp")
    val factTrxnDetail =  hiveContext.sql(s"select trxn_id as trxn_id_detail from $workDB.fact_transaction_detail_temp")



    val tenderHistoryDF = factTenderHistory.join(factTrxnDetail,col("trxn_id_detail") === col("trxn_id"),"left")
                                           .filter(isnull(col("trxn_id_detail")))
                                           .drop(col("trxn_id_detail"))
                                           .groupBy("trxn_id")
                                           .agg(max($"member_key").alias("member_key"),
                                                max($"store_key").alias("store_key"),
                                                max($"trxn_date").alias("trxn_date"),
                                                max($"trxn_date_key").alias("trxn_date_key"),
                                                max($"currency_key").alias("currency_key"),
                                                max($"match_type_key").alias("match_type_key"),
                                                max("captured_loyalty_id").alias("captured_loyalty_id"),
                                                max("implied_loyalty_id").alias("implied_loyalty_id"))
                                           .join(factTenderHistDataset,col("trxn_id") === col("tender_trxn_id"),"left")
                                           .withColumn("ttl_amount", $"express_plcc_amount" + $"credit_card_amount" + $"cash_amount" + $"check_amount" + $"gift_card_amount" + $"other_amount")
                                           .withColumn("has_detail",lit("NO"))
                                           .withColumn("has_tender",lit("YES"))
                                           .withColumn("gift_card_sales_only_ind",lit(null))
                                           .withColumn("cogs",lit(null))
                                           .withColumn("margin",lit(null))
                                           .withColumn("enrollment_trxn_flag", lit(null))
                                           .withColumn("batch_id", lit(batchId))
                                           .withColumn("last_updated_date", current_timestamp)
                                           .withColumn("distance_traveled",lit(null))

    val defaultvalue0_cols = tempFactTable.getColumns.diff(tenderHistoryDF.getColumns)

    val finaltenderDF = defaultvalue0_cols.foldLeft(tenderHistoryDF) {
                                      case (df, colName) => df.withColumn(s"$colName",lit(0))}
                                     .select(tempFactTable.getColumns: _*)

    val finalTrxnTenderSummaryDF = factTrxnDetailDF.unionAll(finaltenderDF)

    val finalTrxnSummaryDF = finalTrxnTenderSummaryDF.na.fill(0,Seq("express_plcc_amount","credit_card_amount","cash_amount","check_amount","gift_card_amount","other_amount","reward_certs_qty","ttl_amount")).persist


    /**************************************************Enriching Enrollment Trxn Flag and distance travelled on the entire dataset*************************************************************************************/

    val enrichedTrxnSummaryDF = finalTrxnSummaryDF.join(dimMemberDF,Seq("member_key"),"left")
                                                   .join(dimstoreDF,Seq("store_key"),"left")
                                                   .withColumn("enrollment_trxn_flag",when((datediff(col("trxn_date"),col("member_acct_enroll_date")) >= 0
                                                    && datediff(col("trxn_date"),col("member_acct_enroll_date")) <= 7
                                                    && not(isnull(col("member_acct_enroll_date"))) && (col("district_code") === 97)) or
                                                      (datediff(col("trxn_date"),col("member_acct_enroll_date")) === 0 && (col("district_code") !== 97))  ,lit("YES")).otherwise(lit("NO")))
                                                   .withColumn("distance_traveled",when(not(isnull(col("lat_mem"))) and not(isnull(col("lat_store"))) and not(isnull(col("lon_mem"))) and not(isnull(col("lon_store"))),
                                                     DistanceTravelled(col("lat_mem"),col("lat_store"),col("lon_mem"),col("lon_store"))).otherwise(null))
                                                   .select(tempFactTable.getColumns: _*)

    enrichedTrxnSummaryDF

  }

  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }

}
