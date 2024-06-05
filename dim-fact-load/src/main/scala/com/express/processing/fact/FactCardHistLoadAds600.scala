package com.express.processing.fact

import com.express.cdw.spark.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, lit, _}


/**
  * Created by akshay.rochwani on 4/27/2017.
  */

object FactCardHistLoadAds600 extends FactLoad {


  override def factTableName = "fact_card_history_ads_600"

  override def surrogateKeyColumn = "card_history_id"

  override def partitionBy : Option[String] = Some("source_key")

  override def backUpFactTableDF: DataFrame = hiveContext.table(s"$goldDB.fact_card_history")

  override def transform: DataFrame = {
    val finalAds400Dataset = hiveContext.sql(s"select * from $goldDB.fact_card_history where status='not_updated'")

    val initialColumnNamesAds400 = finalAds400Dataset.columns.toSeq
    val renamedColumnsAds400 = initialColumnNamesAds400.map(name => col(name).as(s"400_$name"))
    val renamedFinalAds400Dataset = finalAds400Dataset.select(renamedColumnsAds400: _*)

    //logger.info( "load the prerequisite Datasets")
    // load the prerequisite Data Set
    //load dim_date , dim_store_master, dim_card_type, ads_600_auth , ads_600_trxn, ads_600_pahp
    val dimDateDataset = hiveContext.sql(s"select sdate,date_key from $goldDB.dim_date where status = 'current'")
    val dimStoreMasterDataset = hiveContext.sql(s"select store_id,store_key from $goldDB.dim_store_master where status = 'current'")

    val dimCardTypeDataset = hiveContext.sql(s"select tender_type_code,card_type_key from $goldDB.dim_card_type where status = 'current'")
    val factTransformationDataset = hiveContext.sql(s"select * from $workDB.work_ads_600_cm")
      .selectExpr("account_number",
        "application_store_number",
        "record_type",
        "member_key",
        "account_open_date",
        "last_purchase_date",
        "current_otb",
        "ytd_purchase_amount",
        "behavior_score",
        "credit_term_number",
        "cycle_number",
        "mgn_id",
        "previous_account_number",
        "credit_limit",
        "balance_all_plans",
        "application_source_code",
        "(case when recourse_indicator = '' then NULL else recourse_indicator end) as recourse_indicator",
        "number_of_cards_issued",
        "(case when activation_flag = '' then NULL else activation_flag end) as activation_flag",
        "division_number",
        "last_4_digits_of_account_number",
        "amount_of_last_payment",
        "amount_of_last_purchase",
        "logo",
        "(case when closed_date= '' or closed_date='9999-01-01' then NULL else closed_date end) as closed_date",
        "cycle_update_date",
        "card_issue_date",
        "(case when e_statement_indicator = '' then NULL else e_statement_indicator end) as e_statement_indicator",
        "e_statement_change_date",
        "last_return_date",
        "new_account_clerk_number",
        "promotability_indicator",
        "do_not_statement_insert",
        "do_not_sell_name",
        "(case when spam_indicator = '' then NULL else spam_indicator end) as spam_indicator",
        "email_change_date",
        "(case when return_mail_indicator = '' then NULL else return_mail_indicator end) as return_mail_indicator",
        "match_key",
        "source_key")
      .withColumn("rid", lit(null))
      .withColumn("lw_link_key", lit(null))
      .withColumn("unregistration_date", lit(null))
      .withColumn("first_card_txn_date", lit(null))
      .withColumn("last_card_txn_date", lit(null))
      .withColumn("unregistration_date_key", lit(null))
      .withColumn("first_card_txn_date_key", lit(null))
      .withColumn("last_card_txn_date_key", lit(null))
      .withColumn("ip_code", lit(null))
      .withColumn("vc_key", lit(null))
      .withColumn("status_code", lit(null))
      .withColumn("issued_date", lit(null))
      .withColumn("expiration_date", lit(null))
      .withColumn("registration_date", lit(null))
      .withColumn("lw_is_primary", lit(null))
      .withColumn("old_open_close_ind", lit(null))
      .withColumn("tender_type_code", lit("05"))
      .withColumnRenamed("match_key", "match_type_key")
      .withColumnRenamed("credit_term_number", "credit_term_nbr")
      .withColumnRenamed("cycle_number", "cycle_nbr")
      .withColumnRenamed("ytd_purchase_amount", "ytd_purchase_amt")
      .withColumnRenamed("previous_account_number", "previous_account_nbr")
      .withColumnRenamed("recourse_indicator", "recourse_ind")
      .withColumnRenamed("number_of_cards_issued", "nbr_of_cards_issued")
      .withColumnRenamed("division_number", "division_nbr")
      .withColumnRenamed("last_4_digits_of_account_number", "last_4_of_account_nbr")
      .withColumnRenamed("amount_of_last_payment", "amt_of_last_payment")
      .withColumnRenamed("amount_of_last_purchase", "amt_of_last_purchase")
      .withColumnRenamed("e_statement_indicator", "e_statement_ind")
      .withColumnRenamed("new_account_clerk_number", "new_account_clerk_nbr")
      .withColumnRenamed("promotability_indicator", "marketable_ind")
      .withColumnRenamed("return_mail_indicator", "return_mail_ind")


    //logger.info("applying transformation on the required columns")
    // Calculate column set from others
    //If EXP_ADS_600.Closed_date is NULL then Y else N
    //Record Type 5 for ADS 600 then Y else N
    val calculateColumnSet = factTransformationDataset
      .withColumn("is_primary_account_holder", when(factTransformationDataset.col("record_type") === 0, lit("YES")).otherwise(lit("NO")))
      .withColumn("open_close_ind", when((factTransformationDataset.col("closed_date").isNull or factTransformationDataset.col("closed_date")>lit(current_date())), lit("O")).otherwise(lit("C")))
      .withColumnRenamed("account_number", "tokenized_cc_nbr")
      .drop("record_type")


    val joinedTransformationWithCardType = calculateColumnSet.join(dimCardTypeDataset, Seq("tender_type_code"), "left")
      .select("card_type_key", calculateColumnSet.columns: _*)
      .drop("tender_type_code")

    //Adding Lookup Columns
    //Lookup DIM_STORE_MASTER.STORE_ID to get STORE_KEY
    val joinedTransformationWithStoreMaster = joinedTransformationWithCardType.join(dimStoreMasterDataset,
      joinedTransformationWithCardType("application_store_number") === dimStoreMasterDataset.col("store_id"), "left")
      .select("store_key", joinedTransformationWithCardType.columns: _*)
      .withColumnRenamed("store_key", "application_store_key")
      .drop("application_store_number")

    //generate date keys after joining with dim_date
    val dateColumns = List("closed_date",
      "cycle_update_date",
      "card_issue_date",
      "e_statement_change_date",
      "last_return_date",
      "issued_date",
      "expiration_date",
      "registration_date",
      "last_purchase_date",
      "account_open_date")

    val joinedTransformationWithDateKey =  joinedTransformationWithStoreMaster.updateKeys(dateColumns, dimDateDataset, "sdate", "date_key")

    val finalAds600Dataset = joinedTransformationWithDateKey.na.fill(-1, Seq("account_open_date_key",
      "last_purchase_date_key",
      "registration_date_key",
      "expiration_date_key",
      "issued_date_key",
      "last_return_date_key",
      "e_statement_change_date_key",
      "card_issue_date_key",
      "cycle_update_date_key",
      "closed_date_key",
      "application_store_key",
      "card_type_key",
      "member_key",
      "source_key",
      "match_type_key"))
      .select(
        "tokenized_cc_nbr",
        "card_type_key",
        "member_key",
        "rid",
        "account_open_date",
        "application_store_key",
        "last_purchase_date",
        "current_otb",
        "ytd_purchase_amt",
        "behavior_score",
        "credit_term_nbr",
        "cycle_nbr",
        "mgn_id",
        "previous_account_nbr",
        "credit_limit",
        "balance_all_plans",
        "application_source_code",
        "recourse_ind",
        "nbr_of_cards_issued",
        "activation_flag",
        "division_nbr",
        "last_4_of_account_nbr",
        "amt_of_last_payment",
        "amt_of_last_purchase",
        "logo",
        "closed_date",
        "cycle_update_date",
        "card_issue_date",
        "e_statement_ind",
        "e_statement_change_date",
        "last_return_date",
        "new_account_clerk_nbr",
        "old_open_close_ind",
        "marketable_ind",
        "is_primary_account_holder",
        "do_not_statement_insert",
        "do_not_sell_name",
        "spam_indicator",
        "email_change_date",
        "return_mail_ind",
        "ip_code",
        "vc_key",
        "lw_link_key",
        "status_code",
        "issued_date",
        "expiration_date",
        "registration_date",
        "unregistration_date",
        "first_card_txn_date",
        "last_card_txn_date",
        "lw_is_primary",
        "closed_date_key",
        "cycle_update_date_key",
        "card_issue_date_key",
        "e_statement_change_date_key",
        "last_return_date_key",
        "issued_date_key",
        "expiration_date_key",
        "registration_date_key",
        "unregistration_date_key",
        "first_card_txn_date_key",
        "last_card_txn_date_key",
        "open_close_ind",
        "last_purchase_date_key",
        "account_open_date_key",
        "match_type_key",
        "source_key"
      )
    val initialColumnNamesAds600 = finalAds600Dataset.columns.toSeq
    val renamedColumnsAds600 = initialColumnNamesAds600.map(name => col(name).as(s"600_$name"))
    val renamedFinalAds600Dataset = finalAds600Dataset.select(renamedColumnsAds600: _*)

    //join Card_history ads400 and ads600 table
    val finalJoinedDF = renamedFinalAds600Dataset.join(renamedFinalAds400Dataset,
      renamedFinalAds600Dataset.col("600_tokenized_cc_nbr") === renamedFinalAds400Dataset.col("400_tokenized_cc_nbr"), "full")

    logger.debug("Input record count for tlog_customer fact load: {}", finalJoinedDF.count.toString)

    //ads600 insert records
    val ads600InsertDataset = finalJoinedDF.filter(isnull(col("400_tokenized_cc_nbr")))
      .select(
        "600_tokenized_cc_nbr",
        "600_card_type_key",
        "600_member_key",
        "600_rid",
        "600_account_open_date",
        "600_application_store_key",
        "600_last_purchase_date",
        "600_current_otb",
        "600_ytd_purchase_amt",
        "600_behavior_score",
        "600_credit_term_nbr",
        "600_cycle_nbr",
        "600_mgn_id",
        "600_previous_account_nbr",
        "600_credit_limit",
        "600_balance_all_plans",
        "600_application_source_code",
        "600_recourse_ind",
        "600_nbr_of_cards_issued",
        "600_activation_flag",
        "600_division_nbr",
        "600_last_4_of_account_nbr",
        "600_amt_of_last_payment",
        "600_amt_of_last_purchase",
        "600_logo",
        "600_closed_date",
        "600_cycle_update_date",
        "600_card_issue_date",
        "600_e_statement_ind",
        "600_e_statement_change_date",
        "600_last_return_date",
        "600_new_account_clerk_nbr",
        "600_old_open_close_ind",
        "600_marketable_ind",
        "600_is_primary_account_holder",
        "600_do_not_statement_insert",
        "600_do_not_sell_name",
        "600_spam_indicator",
        "600_email_change_date",
        "600_return_mail_ind",
        "600_ip_code",
        "600_vc_key",
        "600_lw_link_key",
        "600_status_code",
        "600_issued_date",
        "600_expiration_date",
        "600_registration_date",
        "600_unregistration_date",
        "600_first_card_txn_date",
        "600_last_card_txn_date",
        "600_lw_is_primary",
        "600_closed_date_key",
        "600_cycle_update_date_key",
        "600_card_issue_date_key",
        "600_e_statement_change_date_key",
        "600_last_return_date_key",
        "600_issued_date_key",
        "600_expiration_date_key",
        "600_registration_date_key",
        "600_unregistration_date_key",
        "600_first_card_txn_date_key",
        "600_last_card_txn_date_key",
        "600_open_close_ind",
        "600_last_purchase_date_key",
        "600_account_open_date_key",
        "600_match_type_key",
        "600_source_key"
      )

    val initialColumnNamesInsert600 = ads600InsertDataset.columns.toSeq.map(_.substring(4))
    val renamedColumnsInsert600 = initialColumnNamesInsert600.map(name => col("600_" + name).as(s"$name"))
    val renamedAds600InsertDataset = ads600InsertDataset.select(renamedColumnsInsert600: _*)

    //ads400 insert records
    val ads400InsertDataset = finalJoinedDF.filter(isnull(col("600_tokenized_cc_nbr")))
      .select(
        "400_tokenized_cc_nbr",
        "400_card_type_key",
        "400_member_key",
        "400_rid",
        "400_account_open_date",
        "400_application_store_key",
        "400_last_purchase_date",
        "400_current_otb",
        "400_ytd_purchase_amt",
        "400_behavior_score",
        "400_credit_term_nbr",
        "400_cycle_nbr",
        "400_mgn_id",
        "400_previous_account_nbr",
        "400_credit_limit",
        "400_balance_all_plans",
        "400_application_source_code",
        "400_recourse_ind",
        "400_nbr_of_cards_issued",
        "400_activation_flag",
        "400_division_nbr",
        "400_last_4_of_account_nbr",
        "400_amt_of_last_payment",
        "400_amt_of_last_purchase",
        "400_logo",
        "400_closed_date",
        "400_cycle_update_date",
        "400_card_issue_date",
        "400_e_statement_ind",
        "400_e_statement_change_date",
        "400_last_return_date",
        "400_new_account_clerk_nbr",
        "400_old_open_close_ind",
        "400_marketable_ind",
        "400_is_primary_account_holder",
        "400_do_not_statement_insert",
        "400_do_not_sell_name",
        "400_spam_indicator",
        "400_email_change_date",
        "400_return_mail_ind",
        "400_ip_code",
        "400_vc_key",
        "400_lw_link_key",
        "400_status_code",
        "400_issued_date",
        "400_expiration_date",
        "400_registration_date",
        "400_unregistration_date",
        "400_first_card_txn_date",
        "400_last_card_txn_date",
        "400_lw_is_primary",
        "400_closed_date_key",
        "400_cycle_update_date_key",
        "400_card_issue_date_key",
        "400_e_statement_change_date_key",
        "400_last_return_date_key",
        "400_issued_date_key",
        "400_expiration_date_key",
        "400_registration_date_key",
        "400_unregistration_date_key",
        "400_first_card_txn_date_key",
        "400_last_card_txn_date_key",
        "400_open_close_ind",
        "400_last_purchase_date_key",
        "400_account_open_date_key",
        "400_match_type_key",
        "400_source_key",
        "400_batch_id",
        "400_last_updated_date"
      )
    val initialColumnNamesInsert400 = ads400InsertDataset.columns.toSeq.map(_.substring(4))
    val renamedColumnsInsert400 = initialColumnNamesInsert400.map(name => col("400_" + name).as(s"$name"))
    val renamedAds400InsertDataset = ads400InsertDataset.select(renamedColumnsInsert400: _*)

    //ads600 update records
    val updateRecords = finalJoinedDF.filter(not(isnull(col("400_tokenized_cc_nbr"))) && not(isnull(col("600_tokenized_cc_nbr")))
    )
      .selectExpr(
        "600_tokenized_cc_nbr",
        "600_card_type_key",
        "400_member_key as 600_member_key",
        "600_rid",
        "600_account_open_date",
        "600_application_store_key",
        "600_last_purchase_date",
        "600_current_otb",
        "600_ytd_purchase_amt",
        "600_behavior_score",
        "600_credit_term_nbr",
        "600_cycle_nbr",
        "600_mgn_id",
        "600_previous_account_nbr",
        "600_credit_limit",
        "600_balance_all_plans",
        "600_application_source_code",
        "600_recourse_ind",
        "600_nbr_of_cards_issued",
        "600_activation_flag",
        "600_division_nbr",
        "600_last_4_of_account_nbr",
        "600_amt_of_last_payment",
        "600_amt_of_last_purchase",
        "600_logo",
        "600_closed_date",
        "600_cycle_update_date",
        "600_card_issue_date",
        "600_e_statement_ind",
        "600_e_statement_change_date",
        "600_last_return_date",
        "600_new_account_clerk_nbr",
        "600_old_open_close_ind",
        "(case when 600_marketable_ind = '' then NULL else 600_marketable_ind end) as 600_marketable_ind",
        "600_is_primary_account_holder",
        "600_do_not_statement_insert",
        "600_do_not_sell_name",
        "600_spam_indicator",
        "600_email_change_date",
        "600_return_mail_ind",
        "600_ip_code",
        "600_vc_key",
        "600_lw_link_key",
        "600_status_code",
        "600_issued_date",
        "600_expiration_date",
        "600_registration_date",
        "600_unregistration_date",
        "600_first_card_txn_date",
        "600_last_card_txn_date",
        "600_lw_is_primary",
        "600_closed_date_key",
        "600_cycle_update_date_key",
        "600_card_issue_date_key",
        "600_e_statement_change_date_key",
        "600_last_return_date_key",
        "600_issued_date_key",
        "600_expiration_date_key",
        "600_registration_date_key",
        "600_unregistration_date_key",
        "600_first_card_txn_date_key",
        "600_last_card_txn_date_key",
        "(case when 600_open_close_ind = '' then NULL else 600_open_close_ind end) as 600_open_close_ind",
        "600_last_purchase_date_key",
        "600_account_open_date_key",
        "600_match_type_key",
        "600_source_key"
      )
    val initialUpdateColumnNames = updateRecords.columns.toSeq.map(_.substring(4))
    val renamedUpdateColumns = initialUpdateColumnNames.map(name => col("600_" + name).as(s"$name"))
    val renamedUpdateRecords = updateRecords.select(renamedUpdateColumns: _*)

    val renamedAds600TotalDataset = renamedAds600InsertDataset.unionAll(renamedUpdateRecords).dropDuplicates()
      .withColumn("loyalty_id", lit(null))
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_updated_date", current_timestamp)

    val renamedAdsFinal =renamedAds400InsertDataset.withColumn("loyalty_id",lit(null))
      .select(renamedAds600TotalDataset.getColumns:_*)

    renamedAds600TotalDataset.unionAll(renamedAdsFinal)

  }


  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }
}