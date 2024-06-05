package com.express.history.fact


import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs.sortAssociatedMK
import com.express.cdw.{CDWContext, CDWOptions}
import com.express.util.Settings
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.apache.spark.sql.types._


/**
  * Created by Mahendranadh.Dasari on 27-02-2019.
  */

object EdwHistoryLoad extends CDWContext with CDWOptions with LazyLogging {

  addOption("tgttableName")
  addOption("srctableName")

  def refineData(AuditData: DataFrame, ordColumn: String, grpColumns: String*): DataFrame = {
    AuditData
      .withColumn("rank", row_number().over(Window.partitionBy(grpColumns.head, grpColumns.tail: _*).orderBy(col(ordColumn).desc)))
      .filter("rank=1")
      .drop("rank")
  }

  def getExpOrderId(DF: DataFrame, OrderDF: DataFrame, JoinCol: String): DataFrame = {
    DF.join(OrderDF,
      DF.col(JoinCol) === OrderDF.col("trxn_id_order"), "left")
      //.select("exp_order_id", DF.columns: _*)
      .drop(col("trxn_id_order"))
    }

  def getStoreId(DF: DataFrame, StoreDF: DataFrame, JoinCol: String, RenameCol: String): DataFrame = {
    DF.join(broadcast(StoreDF),
      DF.col(JoinCol) === StoreDF.col("store_key"), "left")
      .select("store_id", DF.columns: _*)
      .withColumnRenamed("store_id", RenameCol)
      .drop(col("store_key"))
  }

  def transformColumns(transformations: Map[String, String], df: DataFrame): DataFrame = {
    transformations.keys.foldLeft(df) {
      (df, key) => df.withColumn(key, trim(expr(transformations(key))))
    }
  }

  def main(args: Array[String]): Unit = {
    val options = parse(args)
    val srctablename = options("srctableName")
    val tgttablename = options("tgttableName")
    val batch_id = options("batch_id")
    val cmhpcdwDB = "cmhpcdw"
    val goldTable = s"$goldDB.$srctablename"
    val smithTable = s"$smithDB.$tgttablename"
    val tableConfig = Settings.getHistoryMapping(tgttablename)
    val goldDF = hiveContext.table(goldTable)
    val smithDF = hiveContext.table(smithTable)

    import hiveContext.implicits._

    val rawData = if (tgttablename.equalsIgnoreCase("edw_dim_household_history")) {

      val ColumnList = Seq("household_key", "member_count", "first_trxn_date", "is_express_plcc", "add_date", "introduction_date",
        "last_store_purch_date", "last_web_purch_date", "account_open_date", "zip_code", "scoring_model_key", "scoring_model_segment_key", "valid_address", "record_type", "associated_member_key" ,"ingest_date", "status")

      val dimHouseholdAudit = hiveContext.table(s"$cmhpcdwDB.$srctablename" + "_audit")
        .filter("record_type!='PROSPECT' or record_type is null")
        .withColumn("associated_member_key",lit(null))
        .select(ColumnList.head, ColumnList.tail: _*)
      val refinedHouseholdAudit = refineData(dimHouseholdAudit, "status", "household_key", "ingest_date")
        .withColumn("status",when(col("status")==="I", lit("A")).otherwise(lit("C")))

      val HouseholdGold = goldDF
        .withColumn("ingest_date",to_date(col("last_updated_date")))
        .filter("status='history' and batch_id > '20180322034002' and (record_type!='PROSPECT' or record_type is null) and ingest_date>'2018-03-26'")
        .select("last_updated_date", ColumnList: _*)
      val refineddimHHGold = refineData(HouseholdGold, "last_updated_date", "household_key", "ingest_date")
        .withColumn("valid_address", when(col("valid_address") === "STRICT" or col("valid_address") === "LOOSE" or col("valid_address") === "INVALID", col("valid_address")).otherwise(lit(null)))
        .withColumn("status", lit(null))

      val auditGoldUnion = refinedHouseholdAudit.select(ColumnList.head, ColumnList.tail: _*).unionAll(refineddimHHGold.select(ColumnList.head, ColumnList.tail: _*))

      auditGoldUnion
        .withColumn("rank", row_number().over(Window.partitionBy("household_key").orderBy(col("ingest_date"))))
        .withColumn("status",
          when(col("status").isNull and col("rank") === 1, lit("A"))
            .otherwise(when(col("status").isNull and (col("rank") !== 1), lit("C"))
              .otherwise(col("status"))))


    } else if (tgttablename.equalsIgnoreCase("edw_dim_member_history")) {

      val dimStoreMasterDF = hiveContext.sql(s"select store_id,store_key from gold.dim_store_master where status='current'").persist()


      val dimMemberAuditOldDF = hiveContext.sql(s"select member_key,household_key,first_name,last_name,address,city,state,zip_code,zip4,non_us_postal_code,country_code,valid_strict,valid_loose,valid_email,valid_phone_for_sms,phone_nbr,phone_type,email_address,loyalty_id,gender,direct_mail_consent,email_consent,sms_consent,birth_date,deceased,address_is_prison,customer_add_date,customer_introduction_date,latitude,longitude,closest_store_key,second_closest_store_key,distance_to_closest_store,distance_to_sec_closest_store,preferred_store_key,distance_to_preferred_store,is_express_plcc,first_trxn_date,ads_do_not_statement_insert,ads_do_not_sell_name,ads_spam_indicator,ads_email_change_date,ads_return_mail_ind,cm_gender_merch_descr,is_dm_marketable,is_em_marketable,is_sms_marketable,record_type,email_consent_date,sms_consent_date,direct_mail_consent_date,preferred_channel,is_loyalty_member,tier_name,member_status,member_acct_open_date,member_acct_close_date,member_acct_enroll_date,ncoa_last_change_date,last_store_purch_date,last_web_purch_date,date(ingest_date) as ingest_date,status as cc_flag,first_trxn_store_key from cmhpcdw.dim_member_audit_old where ingest_date >= '2015-01-16' and ingest_date <= '2017-01-29' and (record_type != 'PROSPECT' or record_type is null)")
        .withColumn("lw_enrollment_source_key", lit(null))
        .withColumn("lw_enrollment_store_id", lit(null))


      val dimMemberAudit = hiveContext.sql(s"select member_key,household_key,first_name,last_name,address,city,state,zip_code,zip4,non_us_postal_code,country_code,valid_strict,valid_loose,valid_email,valid_phone_for_sms,phone_nbr,phone_type,email_address,loyalty_id,gender,direct_mail_consent,email_consent,sms_consent,birth_date,deceased,address_is_prison,customer_add_date,customer_introduction_date,latitude,longitude,closest_store_key,second_closest_store_key,distance_to_closest_store,distance_to_sec_closest_store,preferred_store_key,distance_to_preferred_store,is_express_plcc,first_trxn_date,ads_do_not_statement_insert,ads_do_not_sell_name,ads_spam_indicator,ads_email_change_date,ads_return_mail_ind,cm_gender_merch_descr,is_dm_marketable,is_em_marketable,is_sms_marketable,record_type,email_consent_date,sms_consent_date,direct_mail_consent_date,preferred_channel,is_loyalty_member,tier_name,member_status,member_acct_open_date,member_acct_close_date,member_acct_enroll_date,ncoa_last_change_date,last_store_purch_date,last_web_purch_date,lw_enrollment_store_key,lw_enrollment_source_key,ingest_date,status as cc_flag,first_trxn_store_key from cmhpcdw.dim_member_audit where ingest_date >= '2017-01-30' and ingest_date <= '2018-03-21' and (record_type != 'PROSPECT' or record_type is null)")


      val dimMemberAuditDF = getStoreId(dimMemberAudit, dimStoreMasterDF, "lw_enrollment_store_key", "lw_enrollment_store_id")
        .drop(col("lw_enrollment_store_key"))

      val auditTableData = dimMemberAuditOldDF.unionAll(dimMemberAuditDF.select(dimMemberAuditOldDF.getColumns: _*))

      val dimMemberAuditAndAuditOld = refineData(auditTableData, "cc_flag", "member_key", "ingest_date")
        .withColumn("address2", lit(null).cast(StringType))
        .withColumn("first_name_scrubbed", lit(null).cast(StringType))
        .withColumn("last_name_scrubbed", lit(null).cast(StringType))
        .withColumn("address1_scrubbed", lit(null).cast(StringType))
        .withColumn("address2_scrubbed", lit(null).cast(StringType))
        .withColumn("city_scrubbed", lit(null).cast(StringType))
        .withColumn("state_scrubbed", lit(null).cast(StringType))
        .withColumn("zip_code_scrubbed", lit(null).cast(StringType))
        .withColumn("zip4_scrubbed", lit(null).cast(StringType))
        .withColumn("zip_full_scrubbed", lit(null).cast(StringType))
        .withColumn("country_code_scrubbed", lit(null).cast(StringType))
        .withColumnRenamed("address", "address1")
        .withColumn("cc_flag", when(col("cc_flag") === lit("I"), lit("A")).otherwise(lit("C")))

      val memberGold = hiveContext.sql(s"select member_key,household_key,first_name,last_name,address1,address2,city,state,zip_code,zip4,first_name_scrubbed,last_name_scrubbed,address1_scrubbed,address2_scrubbed,city_scrubbed,state_scrubbed,zip_code_scrubbed,zip4_scrubbed,zip_full_scrubbed,non_us_postal_code,country_code,country_code_scrubbed,valid_strict,valid_loose,valid_email,valid_phone_for_sms,phone_nbr,phone_type,email_address,loyalty_id,gender,direct_mail_consent,email_consent,sms_consent,birth_date,deceased,address_is_prison,customer_add_date,customer_introduction_date,latitude,longitude,closest_store_key,second_closest_store_key,distance_to_closest_store,distance_to_sec_closest_store,preferred_store_key,distance_to_preferred_store,is_express_plcc,first_trxn_date,ads_do_not_statement_insert,ads_do_not_sell_name,ads_spam_indicator,ads_email_change_date,ads_return_mail_ind,cm_gender_merch_descr,is_dm_marketable,is_em_marketable,is_sms_marketable,record_type,email_consent_date,sms_consent_date,direct_mail_consent_date,preferred_channel,is_loyalty_member,tier_name,member_status,member_acct_open_date,member_acct_close_date,member_acct_enroll_date,ncoa_last_change_date,last_store_purch_date,last_web_purch_date,lw_enrollment_store_key,lw_enrollment_source_key,last_updated_date,date(last_updated_date) as ingest_date,batch_id,first_trxn_store_key from gold.dim_member where status='history' and date(last_updated_date) > '2018-03-22' and (record_type != 'PROSPECT' or record_type is null) ")


      val dimMemberGold = refineData(memberGold, "last_updated_date", "member_key", "ingest_date")
        .drop(col("last_updated_date"))
        .drop(col("batch_id"))

      val dimMemberGoldDF = getStoreId(dimMemberGold, dimStoreMasterDF, "lw_enrollment_store_key", "lw_enrollment_store_id")
        .drop(col("lw_enrollment_store_key"))
        .withColumn("cc_flag", when(col("ingest_date") === col("customer_add_date"), lit("A")).otherwise(lit("C")))


      val dimMember = dimMemberGoldDF.unionAll(dimMemberAuditAndAuditOld.select(dimMemberGoldDF.getColumns: _*))


      val joinedMemberForClosestStore = getStoreId(dimMember, dimStoreMasterDF, "closest_store_key", "closest_store_id")

      val joinedMemberForSecondClosestStore = getStoreId(joinedMemberForClosestStore, dimStoreMasterDF, "second_closest_store_key", "second_closest_store_id")

      val joinedMemberForPreferredStore = getStoreId(joinedMemberForSecondClosestStore, dimStoreMasterDF, "preferred_store_key", "preferred_store_id")

      val joinedMemberForFirstTrxnStore = getStoreId(joinedMemberForPreferredStore, dimStoreMasterDF, "first_trxn_store_key", "first_trxn_store_id")

      val age = round(months_between(current_date(), col("birth_date")) / 12)

      val FindAgeAndAgeQualifier = joinedMemberForFirstTrxnStore
        .withColumn("customer_age", when((year(col("birth_date")) <= 1900) || col("birth_date") > current_date() || col("birth_date").isNull, lit(null)).otherwise(age))
        .withColumn("birth_date", when((year(col("birth_date")) <= 1900) || col("birth_date") > current_date() || col("birth_date").isNull, lit(null)).otherwise(col("birth_date")))
        .withColumn("customer_age_qualifier", when(col("customer_age") > 0, lit("X")).otherwise(lit("U")))



      val finalDF = FindAgeAndAgeQualifier
        .withColumn("last_trxn_date",
          when(isnull(col("last_store_purch_date")) or col("last_store_purch_date") < col("last_web_purch_date"),
            col("last_web_purch_date")).otherwise(col("last_store_purch_date")))
        .withColumn("primary_store_id", lit(null))
        .withColumn("secondary_store_id", lit(null))
        .withColumn("brand_first_trxn_date", lit(null))
        .withColumn("gender_merch_spend", lit(null))
        .withColumn("dominant_shopping_channel", lit(null))
        .withColumn("last_browse_date", lit(null))
        .withColumn("customer_next_conv_status", lit(null))
        .withColumn("plcc_open_date", lit(null))
        .withColumn("plcc_closed_date", lit(null))
        .withColumn("gender",
          when(trim(upper(col("gender"))) === "M", lit("MALE"))
            .when(trim(upper(col("gender"))) === "F", lit("FEMALE"))
            .when(trim(upper(col("gender"))) === "U", lit("UNKNOWN"))
            .when(trim(upper(col("gender"))) === lit("FEMALE"), lit("FEMALE"))
            .when(trim(upper(col("gender"))) === lit("MALE"), lit("MALE"))
            .when(trim(upper(col("gender"))) === lit("UNKNOWN"), lit("UNKNOWN")).otherwise(lit(null)))
        .withColumn("run_date", current_date())
      //.withColumn("ads_return_mail_ind", when(trim(upper(col("ads_return_mail_ind"))) === "Y", lit("YES")).when(trim(upper(col("ads_return_mail_ind"))) === "N", lit("NO")).otherwise(col("ads_return_mail_ind")))

      finalDF
    } else if (tgttablename.equalsIgnoreCase("edw_transaction_detail")) {

      val dimStoreMasterDF = hiveContext.table("gold.dim_store_master")
        .filter("status='current'")
        .select("store_id", "store_key")

      val ColumnList = Seq("trxn_detail_id", "member_key", "trxn_date", "trxn_id", "trxn_nbr", "register_nbr", "trxn_seq_nbr", "store_key", "sku",
        "transaction_type_key", "trxn_time_key", "purchase_qty", "shipped_qty", "units", "is_shipping_cost", "original_price", "retail_amount","orig_trxn_nbr",
        "total_line_amnt_after_discount", "unit_price_after_discount", "discount_pct", "discount_amount", "discount_type_key", "ring_code_used",
        "markdown_type", "gift_card_sales_ind", "division_id", "cashier_id", "salesperson", "orig_trxn_nbr", "post_void_ind", "implied_loyalty_id",
        "captured_loyalty_id", "cogs", "margin", "last_updated_date")

      val factTrxnDetail = hiveContext.table("gold.fact_transaction_detail")
        .select(ColumnList.head, ColumnList.tail: _*)
        .filter("trxn_date>='2015-01-01'")
        .withColumn("gift_card_sales_ind", when(col("gift_card_sales_ind")==="Y", lit("YES"))
          .when(col("gift_card_sales_ind")==="N", lit("NO"))
          .otherwise(null))
        .withColumn("cc_flag", lit("A"))
        .withColumn("post_date", current_date())
        .withColumn("ingest_date", lit(to_date(col("last_updated_date"))))
        .withColumnRenamed("store_key", "trxn_store_key")
        .withColumnRenamed("trxn_time_key", "ftd_trxn_time_key")
        .withColumnRenamed("trxn_detail_id", "transaction_detail_id")


      val joinStoreIdDF = getStoreId(factTrxnDetail, dimStoreMasterDF, "trxn_store_key", "store_id")

      val expOrderIdDF = hiveContext.table("smith.ecomm_order_translation")
        .withColumn("rnk", row_number().over(Window.partitionBy(col("trxn_id"))))
        .filter("rnk=1")
        .withColumnRenamed("trxn_id", "trxn_id_order")
        .select("trxn_id_order", "exp_order_id","demandloc")

      val joinExpOrderIdDF = getExpOrderId(joinStoreIdDF, expOrderIdDF, "trxn_id")

      val trxnTimeDF = hiveContext.table("gold.dim_time")
        .filter("status='current'")
        .select("time_key", "time_in_24hr_day")

      val invoiceDF = hiveContext.table("gold.exp_ecomm_order_data")
        .withColumn("rank", row_number().over(Window.partitionBy(col("id_trn")).orderBy(col("ingest_date").desc)))
        .filter("rank=1")
        .withColumn("ecomm_trxn_date", concat_ws("-",substring(col("id_trn"),9,4),substring(col("id_trn"),13,2),substring(col("id_trn"),15,2)))
        .withColumn("ecomm_store_id", regexp_replace(substring(col("id_trn"),1,8),"^0*",""))
        .withColumn("ecomm_register_nbr", when(length(trim(regexp_replace(substring(col("id_trn"),17,10),"^0*","")))===0 , lit(0))
          .otherwise(trim(regexp_replace(substring(col("id_trn"),17,10),"^0*",""))))
        .withColumn("ecomm_trxn_nbr", when(length(trim(regexp_replace(substring(col("id_trn"),28,6),"^0*","")))===0 , lit(0))
          .otherwise(trim(regexp_replace(substring(col("id_trn"),28,6),"^0*",""))))
        .select("invoicenumber", "ecomm_trxn_date", "ecomm_store_id", "ecomm_register_nbr", "ecomm_trxn_nbr")

      val joinInvoiceDF = joinExpOrderIdDF.join(invoiceDF, col("trxn_date") === col("ecomm_trxn_date") && col("store_id") === col("ecomm_store_id")
        && col("register_nbr") === col("ecomm_register_nbr") && col("trxn_nbr") === col("ecomm_trxn_nbr"), "left")
        .select("invoicenumber", joinExpOrderIdDF.columns: _*)
        .withColumn("store_id",when(upper(col("demandloc")) === lit("NULL") or length(col("demandloc")) === 0 or isnull(col("demandloc")) ,col("store_id")).otherwise(col("demandloc")))
        .drop("demandloc")

      val joinColumnsDF = joinInvoiceDF
        .join(trxnTimeDF, col("time_key") === col("ftd_trxn_time_key"), "left")
        .withColumnRenamed("time_in_24hr_day", "trxn_time_key")
        .select("trxn_time_key", joinInvoiceDF.columns: _*)

      joinColumnsDF

    } else if (tgttablename.equalsIgnoreCase("edw_transaction_summary")) {

      val dimStoreMasterDF = hiveContext.sql(s"select store_id,store_key from gold.dim_store_master where status='current'").persist()

      val ecomOrder = hiveContext.table("smith.ecomm_order_translation")
        .select("trxn_id", "exp_order_id","demandloc")

      val factTrxnDet = Seq("trxn_id","trxn_nbr","register_nbr")

      val trnNbr = hiveContext.table("gold.fact_transaction_detail")
        .filter("trxn_date >= '2015-01-01'")
        .select(factTrxnDet.head, factTrxnDet.tail: _*)

      val sourceDFcolumns = Seq("store_key","trxn_id","member_key","trxn_date","w_purchase_qty","m_purchase_qty","gc_purchase_qty","other_purchase_qty","ttl_purchase_qty","w_units","m_units","gc_units","other_units","ttl_purchase_units","w_amount_after_discount","m_amount_after_discount","gc_amount_after_discount","other_amount_after_discount","ttl_amount_after_discount","discount_amount_8888","discount_amount_no_8888","discount_amount","shipping_amount","express_plcc_amount","credit_card_amount","cash_amount","check_amount","gift_card_amount","other_amount","ttl_amount","has_tender","has_detail","gift_card_sales_only_ind","out_w_purchase_qty","out_m_purchase_qty","ttl_out_purchase_qty","out_w_units","out_m_units","ttl_out_units","out_w_amount_after_discount","out_m_amount_after_discount","ttl_out_amount_after_discount","distance_traveled","reward_certs_qty","captured_loyalty_id","implied_loyalty_id","enrollment_trxn_flag","cogs","margin","last_updated_date")

      val sourceDF = hiveContext.table("gold.fact_transaction_summary")
        .filter("trxn_date >= '2015-01-01'")
        .select(sourceDFcolumns.head, sourceDFcolumns.tail: _*)
        .withColumnRenamed("store_key","trxn_store_key")
        .withColumn("gift_card_sales_only_ind", when(col("gift_card_sales_only_ind") === "Y" ,lit("YES"))
          .when(col("gift_card_sales_only_ind") === "N" ,lit("NO"))
          .otherwise(null))

      val invoiceDF = hiveContext.table("gold.exp_ecomm_order_data")
        .withColumn("rank", row_number().over(Window.partitionBy(col("id_trn")).orderBy(col("ingest_date").desc)))
        .filter("rank=1")
        .withColumn("ecomm_trxn_date", concat_ws("-",substring(col("id_trn"),9,4),substring(col("id_trn"),13,2),substring(col("id_trn"),15,2)))
        .withColumn("ecomm_store_id", regexp_replace(substring(col("id_trn"),1,8),"^0*",""))
        .withColumn("ecomm_register_nbr", when(length(trim(regexp_replace(substring(col("id_trn"),17,10),"^0*","")))===0 , lit(0))
          .otherwise(trim(regexp_replace(substring(col("id_trn"),17,10),"^0*",""))))
        .withColumn("ecomm_trxn_nbr", when(length(trim(regexp_replace(substring(col("id_trn"),28,6),"^0*","")))===0 , lit(0))
          .otherwise(trim(regexp_replace(substring(col("id_trn"),28,6),"^0*",""))))
        .select("invoicenumber", "ecomm_trxn_date", "ecomm_store_id", "ecomm_register_nbr", "ecomm_trxn_nbr")

      val getStoreid_final = getStoreId(sourceDF, dimStoreMasterDF, "trxn_store_key", "store_id")

      val editsourceDF = getStoreid_final
        .withColumnRenamed("last_updated_date","ingest_date")
        .withColumn("cc_flag", lit("A"))
        .withColumn("post_date", current_date())

      val expOrderid = editsourceDF.join(ecomOrder.withColumn("rank", row_number() over Window.partitionBy("trxn_id","exp_order_id"))
        .filter("rank = 1"),Seq("trxn_id"),"left")

      val expOrderid_final = expOrderid.join(trnNbr.withColumn("Seq_rank", row_number() over Window.partitionBy("trxn_id"))
        .filter("Seq_rank = 1"),Seq("trxn_id"),"inner")
        .withColumnRenamed("post_date","run_date")
        .withColumnRenamed("trxn_nbr","tran_number")
        //.withColumnRenamed("register_nbr","register_number")
        .drop("trxn_store_key")
        .drop("rank")
        .drop("Seq_rank")
        .withColumn("member_rank", row_number() over Window.partitionBy("trxn_id").orderBy(desc("member_key")))
        .filter("member_rank=1")
        .drop("member_rank")

      val joinInvoiceDF = expOrderid_final.join(invoiceDF, col("trxn_date") === col("ecomm_trxn_date") && col("store_id") === col("ecomm_store_id")
        && col("register_nbr") === col("ecomm_register_nbr") && col("tran_number") === col("ecomm_trxn_nbr"), "left")
        .select("invoicenumber", expOrderid_final.columns: _*)
        .withColumn("store_id",when(upper(col("demandloc")) === lit("NULL") or length(col("demandloc")) === 0 or isnull(col("demandloc")) ,col("store_id")).otherwise(col("demandloc")))
        .drop("demandloc")


      joinInvoiceDF
    }
    else
      goldDF

    logger.debug("Inserting into table : ", smithTable)

    val renamedDF = rawData.renameColumns(tableConfig.renameConfig).withColumn("run_date", current_date())
    val transformDF = transformColumns(tableConfig.transformConfig, renamedDF)


    transformDF.select(smithDF.getColumns: _*).write.partitionBy("run_date").mode(SaveMode.Overwrite).insertInto(smithTable)
  }


}
