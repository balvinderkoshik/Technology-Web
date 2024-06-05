package com.express.processing.fact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, lit, when}


/**
  * Created by akshay.rochwani on 5/17/2017.
  */
object FactCardHistLoadTlog extends FactLoad {

  override def factTableName = "fact_card_history_tlog_customer"

  override def surrogateKeyColumn = "card_history_id"

  override def backUpFactTableDF: DataFrame = hiveContext.table(s"$goldDB.fact_card_history")

  override def transform: DataFrame = {

    import hiveContext.implicits._

    val dimCardTypeDataset = hiveContext.sql(s"select tender_type_code,card_type_key from $goldDB.dim_card_type where status='current'")
    val tlogTenderDataset = hiveContext.sql(s"select tender_type  as tender_type_tender,division_id,promotion_flag,tender_number ," +
      s"concat(regexp_replace(transaction_date_iso, '-', ''),lpad(store_id,5,'0'),lpad(register_id,3,'0')," +
      s"lpad(transaction_number,5,'0') ) as trxn_id_tender from $workDB.work_tlog_tender_dedup")
      .withColumn("marketable_ind", when($"promotion_flag" === 0, lit("N")).otherwise(lit("Y")))
      .withColumnRenamed("division_id", "division_nbr")

    val tlogCmDataset = hiveContext.sql(s"select loyalty,source_key,match_key as match_type_key" +
      s",member_key,tender_type,tender_number,trxn_id from $workDB.work_tlog_customer_cm")
      .withColumnRenamed("tender_number", "tender_number_tlog_cm")

    val factTransformationDataset = tlogTenderDataset.join(tlogCmDataset,
      tlogTenderDataset.col("trxn_id_tender") === tlogCmDataset.col("trxn_id")
        && tlogTenderDataset.col("tender_number") === tlogCmDataset.col("tender_number_tlog_cm")
        && tlogTenderDataset.col("tender_type_tender") === tlogCmDataset.col("tender_type"), "left")
      .withColumn("loyalty_id", when(tlogCmDataset.col("loyalty").isNotNull, tlogCmDataset.col("loyalty")).otherwise(lit(null)))
      .withColumn("tokenized_cc_nbr", tlogTenderDataset.col("tender_number"))
      .withColumn("last_updated_date", current_timestamp)
      .withColumn("batch_id", lit(batchId))
      .withColumn("rid", lit(null))
      .withColumn("account_open_date", lit("1900-01-01"))
      .withColumn("application_store_key", lit(-1))
      .withColumn("last_purchase_date", lit("1900-01-01"))
      .withColumn("current_otb", lit(null))
      .withColumn("ytd_purchase_amt", lit(null))
      .withColumn("behavior_score", lit(null))
      .withColumn("credit_term_nbr", lit(null))
      .withColumn("cycle_nbr", lit(null))
      .withColumn("mgn_id", lit(null))
      .withColumn("previous_account_nbr", lit(null))
      .withColumn("credit_limit", lit(null))
      .withColumn("balance_all_plans", lit(null))
      .withColumn("application_source_code", lit(null))
      .withColumn("recourse_ind", lit(null))
      .withColumn("nbr_of_cards_issued", lit(null))
      .withColumn("activation_flag", lit(null))
      .withColumn("last_4_of_account_nbr", lit(null))
      .withColumn("amt_of_last_payment", lit(null))
      .withColumn("amt_of_last_purchase", lit(null))
      .withColumn("logo", lit(null))
      .withColumn("closed_date", lit("1900-01-01"))
      .withColumn("cycle_update_date", lit("1900-01-01"))
      .withColumn("card_issue_date", lit("1900-01-01"))
      .withColumn("e_statement_ind", lit(null))
      .withColumn("e_statement_change_date", lit("1900-01-01"))
      .withColumn("last_return_date", lit("1900-01-01"))
      .withColumn("new_account_clerk_nbr", lit(null))
      .withColumn("old_open_close_ind", lit(null))
      .withColumn("is_primary_account_holder", lit("NO"))
      .withColumn("do_not_statement_insert", lit(null))
      .withColumn("do_not_sell_name", lit(null))
      .withColumn("spam_indicator", lit(null))
      .withColumn("email_change_date", lit("1900-01-01"))
      .withColumn("return_mail_ind", lit(null))
      .withColumn("ip_code", lit(null))
      .withColumn("vc_key", lit(null))
      .withColumn("lw_link_key", lit(null))
      .withColumn("status_code", lit(null))
      .withColumn("issued_date", lit("1900-01-01"))
      .withColumn("expiration_date", lit("1900-01-01"))
      .withColumn("registration_date", lit("1900-01-01"))
      .withColumn("unregistration_date", lit("1900-01-01"))
      .withColumn("first_card_txn_date", lit("1900-01-01"))
      .withColumn("last_card_txn_date", lit("1900-01-01"))
      .withColumn("lw_is_primary", lit(null))
      .withColumn("expiration_date_key", lit(-1))
      .withColumn("registration_date_key", lit(-1))
      .withColumn("unregistration_date_key", lit(-1))
      .withColumn("first_card_txn_date_key", lit(-1))
      .withColumn("last_card_txn_date_key", lit(-1))
      .withColumn("open_close_ind", lit("O"))
      .withColumn("last_purchase_date_key", lit(-1))
      .withColumn("account_open_date_key", lit(-1))
      .withColumn("closed_date_key", lit(-1))
      .withColumn("cycle_update_date_key", lit(-1))
      .withColumn("card_issue_date_key", lit(-1))
      .withColumn("e_statement_change_date_key", lit(-1))
      .withColumn("last_return_date_key", lit(-1))
      .withColumn("issued_date_key", lit(-1))
      .drop("loyalty")
      .drop("tender_number_tlog_cm")
      .drop("trxn_id")
      .drop("tender_number")
      .drop("trxn_id_tender")
      .drop("tender_type")

    logger.debug("Input record count for tlog_customer fact load: {}", factTransformationDataset.count.toString)

    val joinedTransformationWithCardType = factTransformationDataset.join(dimCardTypeDataset,
      factTransformationDataset.col("tender_type_tender") === dimCardTypeDataset.col("tender_type_code"), "left")
      .select("card_type_key", factTransformationDataset.columns: _*)
      .drop("tender_type_tender")

    joinedTransformationWithCardType.na.fill(-1, Seq("card_type_key", "member_key", "source_key", "record_info_key", "match_type_key"))
  }


  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }
}
