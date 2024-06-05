package com.express.processing.fact


import com.express.cdw.MatchTypeKeys
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, lit, udf, _}

/**
  * Created by akshay.rochwani on 4/27/2017.
  */
object FactTenderHist extends FactLoad {

  private val amountUDF = udf((sign: String, amount: Int) => sign match {
    case "+" => amount / 100f
    case "-" => -amount / 100f
  })

  override def factTableName = "fact_tender_history"

  override def surrogateKeyColumn = "fact_tender_hist_id"

  override def transform: DataFrame = {
    val dimStoreMasterDataset = hiveContext.sql(s"select store_id,store_key from $goldDB.dim_store_master where status='current'")
    val dimCurrencyDataset = hiveContext.sql(s"select currency_code,currency_key from $goldDB.dim_currency where status='current'")
    val dimDateDataset = hiveContext.sql(s"select sdate,date_key from $goldDB.dim_date where status='current'")
    val dimTenderTypeDataset = hiveContext.sql(s"select tender_type_code,tender_type_key from $goldDB.dim_tender_type where status='current'")
    val dimCheckAuthDataset = hiveContext.sql(s"select check_swiped_flag,check_auth_enable_flag,check_authorization_type," +
      s"check_auth_key from $goldDB.dim_check_authorization where status='current'")
      .withColumnRenamed("check_authorization_type", "check_auth_type")
    val tlogTenderDataset = hiveContext.sql(s"select *,concat(regexp_replace(transaction_date_iso, '-', ''),lpad(store_id,5,'0')," +
      s"lpad(register_id,3,'0'),lpad(transaction_number,5,'0') ) as trxn_id from $workDB.work_tlog_tender_dedup")
      .select("transaction_number",
        "change_due_sign",
        "transaction_date_iso",
        "register_id",
        "store_id",
        "transaction_sequence_number",
        "division_id",
        "cashier_id_header",
        "salesperson",
        "associate_sales_flag",
        "original_transaction_number",
        "re_issue_flag",
        "tender_amount_sign",
        "tender_amount",
        "change_due",
        "post_void_flag",
        "check_authorization_number",
        "tender_type",
        "check_authorization_enable",
        "tender_number",
        "check_swiped",
        "check_authorization_type",
        "trxn_id"
      )
      .withColumnRenamed("change_due", "chng_due")
      .withColumnRenamed("tender_amount", "tender_amt")
      .withColumnRenamed("cashier_id_header", "cashier_id")
      .withColumn("associate_sales_flag", when(col("associate_sales_flag") === 1, lit("YES")).otherwise("NO"))
      .withColumn("post_void_ind", when(col("post_void_flag") === 1, lit("YES")).otherwise("NO"))
      .withColumnRenamed("re_issue_flag", "reissue_flag")
      .withColumnRenamed("transaction_number", "trxn_nbr")
      .withColumnRenamed("register_id", "register_nbr")
      .withColumnRenamed("transaction_date_iso", "trxn_date")
      .withColumnRenamed("tender_number", "tokenized_cc_nbr")
      .withColumnRenamed("transaction_sequence_number", "trxn_tender_seq_nbr")
      .withColumnRenamed("original_transaction_number", "orig_trxn_nbr")
      .withColumnRenamed("check_authorization_number", "check_auth_nbr")
      .withColumnRenamed("check_authorization_enable", "check_auth_enable")
      .withColumnRenamed("check_swiped", "chk_swiped")
      .withColumnRenamed("check_authorization_type", "check_auth_type")
    logger.info("tlogTenderDataset :{}", tlogTenderDataset.count().toString)

    val tlogCmDataset = hiveContext.sql(s"select match_key,trxn_id,tender_number,tender_type,member_key,loyalty,member_loyalty from $workDB.work_tlog_customer_cm")
      .withColumnRenamed("match_key", "match_type_key")
      .withColumnRenamed("tender_type", "tender_type_tlog_cm")
      .withColumnRenamed("tender_number", "tender_number_tlog_cm")
      .withColumnRenamed("trxn_id", "trxn_id_tlog_cm")
    logger.debug("tlogCmDataset: {}", tlogCmDataset.count().toString)

    val factTransformationDataset = tlogTenderDataset.join(tlogCmDataset,
      tlogTenderDataset.col("trxn_id") === tlogCmDataset.col("trxn_id_tlog_cm")
        && lpad(tlogTenderDataset.col("tender_type"), 2, "0") === lpad(tlogCmDataset.col("tender_type_tlog_cm"), 2, "0")
        && lpad(tlogTenderDataset.col("tokenized_cc_nbr"), 18, "0") === lpad(tlogCmDataset.col("tender_number_tlog_cm"), 18, "0"), "left")
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_updated_date", current_timestamp)
      .withColumn("tender_amount", amountUDF(tlogTenderDataset.col("tender_amount_sign"), tlogTenderDataset.col("tender_amt")))
      .withColumn("change_due", amountUDF(tlogTenderDataset.col("change_due_sign"), tlogTenderDataset.col("chng_due")))
      .withColumn("check_authorization_enable", when(tlogTenderDataset.col("check_auth_enable") === 1, lit("Y")).otherwise(lit("N")))
      .withColumn("check_swiped", when(tlogTenderDataset.col("chk_swiped") === 1, lit("Y")).otherwise(lit("N")))
      .withColumn("check_authorization_type", when(tlogTenderDataset.col("check_auth_type") === 1, lit("Y")).otherwise(lit("N")))
      .withColumn("rid", lit(null))
      .withColumn("associate_key", lit(null))
      .withColumn("tokenized_cc_key", lit(null))
      .withColumn("captured_loyalty_id", when(col("match_type_key") === MatchTypeKeys.LoyaltyId,
        col("loyalty")).when(col("match_type_key") === MatchTypeKeys.NoMatch,
        col("loyalty")).otherwise(lit(null)))
      .withColumn("implied_loyalty_id", when(col("match_type_key") === MatchTypeKeys.PhoneEmailLoyalty, col("member_loyalty")).
        when(col("match_type_key") === MatchTypeKeys.PlccNameLoyalty, col("member_loyalty")).
        when(col("match_type_key") === MatchTypeKeys.PlccLoyalty, col("member_loyalty")).otherwise(lit(null)))
      .drop("tender_amount_sign")
      .drop("change_due_sign")
      .drop("check_auth_enable")
      .drop("chk_swiped")
      .drop("check_auth_type")
      .drop("tender_number_tlog_cm")
      .drop("trxn_id_tlog_cm")
      .drop("tender_type_tlog_cm")

    logger.debug("factTransformationDataset:" + factTransformationDataset.count().toString)

    val joinedTransformationWithStore = factTransformationDataset.join(dimStoreMasterDataset, Seq("store_id"), "left")
      .select("store_key", factTransformationDataset.columns: _*)

    logger.debug("joinedTransformationWithStore : {}", joinedTransformationWithStore.count().toString)

    val dimStoreMasterDataset2 = hiveContext.sql(s"select store_id,currency_code from $goldDB.dim_store_master where status='current'")
    val joinedWithStoreDataset = joinedTransformationWithStore.join(dimStoreMasterDataset2, Seq("store_id"), "left")
      .select("currency_code", joinedTransformationWithStore.columns: _*)
      .drop("store_id")

    val joinedWithCurrencyDataset = joinedWithStoreDataset.join(dimCurrencyDataset, Seq("currency_code"), "left")
      .select("currency_key", joinedWithStoreDataset.columns: _*)
      .drop("currency_code")

    //Lookup TLOG trailer block trxn_date on DATE to get date_key
    val joinedTransformationWithDate = joinedWithCurrencyDataset.join(dimDateDataset,
      joinedWithCurrencyDataset.col("trxn_date") === dimDateDataset.col("sdate"), "left")
      .select("date_key", joinedWithCurrencyDataset.columns: _*)
      .withColumnRenamed("date_key", "trxn_date_key")
    logger.debug("joinedTransformationWithDate : {}", joinedTransformationWithDate.count().toString)

    //lookup TLOG u block TEDER_TYPE on TENDER_TYPE.TENDER_TYPE_CODE  to get TENDER_TYPE_KEY
    val joinedTransformationWithTender = joinedTransformationWithDate.join(dimTenderTypeDataset,
      joinedTransformationWithDate.col("tender_type") === dimTenderTypeDataset.col("tender_type_code"), "left")
      .select("tender_type_key", joinedTransformationWithDate.columns: _*)
      .drop("tender_type")

    logger.debug("joinedTransformationWithTender : {}", joinedTransformationWithTender.count().toString)

    // to get CHECK_AUTH_KEY
    val joinedTransformationWithCheckAuth = joinedTransformationWithTender.join(dimCheckAuthDataset,
      joinedTransformationWithTender.col("check_authorization_enable") === dimCheckAuthDataset.col("check_auth_enable_flag")
        && joinedTransformationWithTender.col("check_swiped") === dimCheckAuthDataset.col("check_swiped_flag")
        && joinedTransformationWithTender.col("check_authorization_type") === dimCheckAuthDataset.col("check_auth_type"), "left")
      .select("check_auth_key", joinedTransformationWithTender.columns: _*)
      .drop("check_authorization_enable")
      .drop("check_swiped")
      .drop("check_authorization_type")
    logger.debug("joinedTransformationWithCheckAuth : {}", joinedTransformationWithCheckAuth.count().toString)

    joinedTransformationWithCheckAuth.na.fill(-1, Seq("currency_key",
      "store_key",
      "trxn_date_key",
      "tender_type_key",
      "check_auth_key",
      "member_key",
      "match_type_key"))
  }

  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }

}