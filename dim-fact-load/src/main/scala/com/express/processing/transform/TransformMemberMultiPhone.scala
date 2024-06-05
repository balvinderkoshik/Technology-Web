package com.express.processing.transform


import java.text.SimpleDateFormat
import java.util.Calendar

import com.express.cdw.Settings
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SaveMode}
/**
  * Created by poonam.mishra on 6/14/2017.
  */
object  TransformMemberMultiPhone extends LazyLogging {


  System.setProperty("hive.exec.dynamic.partition", "true")
  System.setProperty("hive.exec.dynamic.partition.mode", "nonstrict")
  val workDB: String = Settings.getWorkDB
  val lookupDB: String = Settings.getGoldDB
  val format = new SimpleDateFormat("yyyy-dd-MM")
  val dateInstance = format.format(Calendar.getInstance.getTime)
  val phoneCol = "phone_number"
  val memberKeyCol = "member_key"
  val phoneColCM = "phone_number_cm"
  val memberKeyColCM = "member_key_cm"
  val joinType = "full"
  val insertAction = "I"
  val updateAction = "U"
  val conf = Settings.sparkConf
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  import sqlContext.implicits._
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  val TlogAdminWorkTable = s"$workDB.work_tlog_admin_dedup"
  val TlogDiscountWorkTable = s"$workDB.work_tlog_discount_dedup"
  val TlogSaleWorkTable = s"$workDB.work_tlog_sale_dedup"
  val TLogTenderWorkTable = s"$workDB.work_tlog_tender_dedup"
  val TLogTransactionWorkTable = s"$workDB.work_tlog_transaction_dedup"
  val TransactionID = "trxn_id"
  val TraansformCMResultTable = s"work.dim_member_multi_phone"

  val Member = "DimMember"
  val DimMember = hiveContext.sql("select member_key,loyalty_id,is_loyalty_member from " + lookupDB + ".dim_member where status='current'").alias(Member)

  val targetColumnList = Seq(memberKeyCol, phoneCol, "phone_type","phone_consent","phone_consent_date","valid_phone","best_mobile_flag","best_member",
    "is_loyalty_flag","source_key","original_source_key","match_type_key")

  val sourceColumnList = Seq(memberKeyColCM, phoneColCM,"phone_type_cm","phone_consent_cm","phone_consent_date_cm","valid_phone_cm","best_mobile_flag_cm","best_member_cm",
    "is_loyalty_flag_cm","source_key_cm","original_source_key_cm","match_type_key_cm" )


  // Deduplicate The insert records
  def loadSourceData(sourceCMData: DataFrame, processing_file: String): DataFrame = {

    val dedupSourceData = processing_file match {
      case "ads_400" =>
        sourceCMData.withColumn("phone_number_home_ads_400" ,when(not(isnull(col("home_phone_number"))),col("home_phone_number")).otherwise(lit("UNKNOWN")))
          .withColumn("phone_number_emp_ads_400",when(not(isnull(col("employee_phone_number"))),col("employee_phone_number")).otherwise(lit("UNKNOWN")))
          .registerTempTable("customer_matching_output_table")
        val unionForVariousPhoneTypes = hiveContext.sql("select division_number,account_number,sur_title,first_name,middle_initial,last_name,street_address,secondary_street_address,city,state,zip_code,"+
          " zip_plus_4,filler_1,filler_2,filler_3,indicator,open_date,phone_number_home_ads_400 AS phone_number_cm,birth_date,logo,email_address,"+
          " application_store_number,last_4_digits_of_account_number,transferred_from_account_number,application_number,credit_term_number,promotability_indicator,"+
          " email_opt_in_indicator,address_change_date,etl_unique_id,batch_id,source_key,record_info_key,member_key_cm,match_key,match_status from customer_matching_output_table"+
          " union all select division_number,account_number,sur_title,first_name,middle_initial,last_name,street_address,secondary_street_address,city,state,zip_code,"+
          " zip_plus_4,filler_1,filler_2,filler_3,indicator,open_date,phone_number_emp_ads_400 AS phone_number_cm,birth_date,logo,email_address,application_store_number,last_4_digits_of_account_number,"+
          " transferred_from_account_number,application_number,credit_term_number,promotability_indicator,email_opt_in_indicator,address_change_date,etl_unique_id,batch_id,source_key,record_info_key,"+
          " member_key_cm,match_key,match_status from customer_matching_output_table")
        unionForVariousPhoneTypes.registerTempTable("unionForVariousPhoneTypes")
        hiveContext.sql("select *,row_number() over (PARTITION BY member_key_cm,phone_number_cm order by"+
          " open_date) as rnk from unionForVariousPhoneTypes")

      case "ads_600" =>
        sourceCMData.withColumn("phone_number_home_ads_600",when(not(isnull(col("home_phone"))),col("home_phone")).otherwise(lit("UNKNOWN")))
          .withColumn("phone_number_mob_ads_600",when(not(isnull(col("mobile_phone"))),col("mobile_phone")).otherwise(lit("UNKNOWN")))
          .registerTempTable("customer_matching_output_table")
        val unionForVariousPhoneTypes = hiveContext.sql("select record_type,account_open_date,application_store_number,first_name,middle_initial,last_name," +
          "street_address,city,state,zip_code,zip_plus_4,phone_number_home_ads_600 AS phone_number_cm,last_purchase_date,current_otb,filler_1,filler_2,ytd_purchase_amount," +
          "filler_3,filler_4,account_number,filler_5,filler_6,filler_7,filler_8,behavior_score,birth_date,marketing_promo_flag,email_address,credit_term_number,cycle_number," +
          "mgn_id,previous_account_number,credit_limit,balance_all_plans,application_source_code,email_opt_in_indicator,do_not_direct_mail,do_not_statement_insert,do_not_telemarket," +
          "do_not_sell_name,spam_indicator,country_code,recourse_indicator,number_of_cards_issued,activation_flag,division_number,last_4_digits_of_account_number,filler_9,amount_of_last_payment," +
          "amount_of_last_purchase,secondary_street_address,foreign_map_code,sur_title_code,logo,return_mail_indicator,promotability_indicator,closed_date,cycle_update_date,card_issue_date," +
          "email_change_date,e_statement_indicator,e_statement_change_date,last_return_date,new_account_clerk_number,filler_10,etl_unique_id,batch_id,source_key,record_info_key,member_key_cm," +
          "match_key,match_status from customer_matching_output_table union all select record_type,account_open_date,application_store_number,first_name,middle_initial,last_name,street_address," +
          "city,state,zip_code,zip_plus_4,phone_number_mob_ads_600 AS phone_number_cm,last_purchase_date,current_otb,filler_1,filler_2,ytd_purchase_amount,filler_3,filler_4,account_number,filler_5," +
          "filler_6,filler_7,filler_8,behavior_score,birth_date,marketing_promo_flag,email_address,credit_term_number,cycle_number,mgn_id,previous_account_number,credit_limit,balance_all_plans," +
          "application_source_code,email_opt_in_indicator,do_not_direct_mail,do_not_statement_insert,do_not_telemarket,do_not_sell_name,spam_indicator,country_code,recourse_indicator,number_of_cards_issued," +
          "activation_flag,division_number,last_4_digits_of_account_number,filler_9,amount_of_last_payment,amount_of_last_purchase,secondary_street_address,foreign_map_code,sur_title_code,logo," +
          "return_mail_indicator,promotability_indicator,closed_date,cycle_update_date,card_issue_date,email_change_date,e_statement_indicator,e_statement_change_date,last_return_date,new_account_clerk_number," +
          "filler_10,etl_unique_id,batch_id,source_key,record_info_key,member_key_cm,match_key,match_status from customer_matching_output_table")
        unionForVariousPhoneTypes.registerTempTable("unionForVariousPhoneTypes")
        hiveContext.sql("select *,row_number() over(PARTITION BY member_key_cm,phone_number_cm)"+
          "as rnk from unionForVariousPhoneTypes")
      case "tlog_customer" =>
        val storeMaster = hiveContext.sql("select store_id,store_key from " + lookupDB + ".dim_store_master where store_id is not null and status='current'")
        val tlogTrxnIDUDF = GetTrxnIDUDF($"transaction_date_iso", $"store_id", $"register_id", $"transaction_number")
        val tlogSaleWork = hiveContext.table(TlogSaleWorkTable).withColumn(TransactionID, tlogTrxnIDUDF).select($"$TransactionID", $"transaction_time")
        val tlogNonMerchandise = hiveContext.table(TLogTransactionWorkTable).withColumn(TransactionID, tlogTrxnIDUDF).select($"$TransactionID", $"transaction_time")
        val tlogDiscount = hiveContext.table(TlogDiscountWorkTable).withColumn(TransactionID, tlogTrxnIDUDF).select($"$TransactionID", $"transaction_time")
        // Union the merchandise, non-merchandise and discount transactions
        val transactions = tlogSaleWork.unionAll(tlogNonMerchandise).unionAll(tlogDiscount)
        transactions.distinct.join(sourceCMData, Seq(TransactionID)).orderBy(col("transaction_date_iso").desc, col("transaction_time").desc)
          .withColumn("phone_number_tlog_cust",when(not(isnull(trim(col("phone_number")))) and not(isnull(trim(col("phone_number")))),trim(col("phone_number"))).otherwise(lit("UNKNOWN")))
          .registerTempTable("customer_matching_output_table")

        val tlogCustomerDF = hiveContext.sql("select *,row_number() over (PARTITION BY member_key_cm,phone_number_tlog_cust) as rnk from customer_matching_output_table ")
          .drop(col("phone_number")).withColumnRenamed("phone_number_tlog_cust","phone_number_cm")
        //.withColumn("do_not_email", lit(null))
        //hiveContext.sql("select *,row_number() over (PARTITION BY member_key_cm,email_address_cm order by do_not_email) as rnk from customer_matching_output_table ")
        storeMaster.join(tlogCustomerDF, storeMaster.col("store_id") === tlogCustomerDF.col("store_id")).drop(storeMaster.col("store_id"))

      case "peoplesoft" =>
        sourceCMData.withColumn("phone_number_peoplesoft",when(not(isnull(col("phone"))),col("phone")).otherwise(lit("UNKNOWN")))
          .withColumn("phone_number_mob_peoplesoft",when(not(isnull(col("mobile_phone"))),col("mobile_phone")).otherwise(lit("UNKNOWN")))
          .registerTempTable("customer_matching_output_table")
        val unionForVariousPhoneTypes = hiveContext.sql("select record_type,division_code,first_name,middle_initial,last_name,address,apartment_number,city,state,zip,zip4,employee_id,employee_status,hire_date,termination_date,"+
          "rehire_date,discount_percentage,full_part_time_indicator,phone_number_peoplesoft AS phone_number_cm,location,update_add_indicator,email_address,clerk_id,home_store_number,etl_unique_id,batch_id,source_key,"+
          "member_key_cm,match_key,match_status from customer_matching_output_table union all select record_type,division_code,first_name,middle_initial,last_name,address,apartment_number,city,state,zip,zip4,employee_id,"+
          "employee_status,hire_date,termination_date,rehire_date,discount_percentage,full_part_time_indicator,phone_number_mob_peoplesoft AS phone_number_cm,location,update_add_indicator,email_address,clerk_id,home_store_number,etl_unique_id,"+
          "batch_id,source_key,member_key_cm,match_key,match_status from customer_matching_output_table")
        //" as rnk_mobile from customer_matching_output_table")
        unionForVariousPhoneTypes.registerTempTable("unionForVariousPhoneTypes")
        hiveContext.sql("select *,row_number() over(PARTITION BY member_key_cm,phone_number_cm)"+
          "as rnk from unionForVariousPhoneTypes")

      case "bp_member" =>
        sourceCMData.withColumn("phone_number_bp_member",when(not(isnull(col("primaryphonenumber"))),col("primaryphonenumber")).otherwise(lit("UNKNOWN")))
          .registerTempTable("customer_matching_output_table")
        hiveContext.sql("select *,row_number() over (PARTITION BY member_key_cm,phone_number_bp_member order by membercreatedate desc) as rnk from customer_matching_output_table")
          .drop(col("primaryphonenumber")).withColumnRenamed("phone_number_bp_member","phone_number_cm")


      case _ =>
        sourceCMData.withColumn("phone_number_persio",when(not(isnull(col("phone_number"))),col("phone_number")).otherwise(lit("UNKNOWN")))
          .registerTempTable("customer_matching_output_table")
        hiveContext.sql("select *,row_number() over (PARTITION BY member_key_cm,phone_number_persio order by consent_date) as rnk from customer_matching_output_table")
          .drop(col("phone_number")).withColumnRenamed("phone_number_persio","phone_number_cm")



    }
    dedupSourceData
  }

  def main(args: Array[String]): Unit = {

    val processing_file = args {0}.toLowerCase.trim()

    // Source: Customer_Matching Work Tables
    // Filter Null records from Natural key columns and deduplicate the source data
    val sourceCMData = hiveContext.sql("select * from " + workDB + ".work_" + processing_file + "_cm").withColumnRenamed(memberKeyCol, memberKeyColCM)
    val sourceFileData = loadSourceData(sourceCMData, processing_file)
    val SourceData = "sourceData"
    val sourceData = sourceFileData.filter(col("rnk") === 1).filter(CheckNotEmptyUDF(col(phoneColCM))).filter(upper(trim(col(phoneColCM))) !== "UNKNOWN").alias(SourceData)

    // Load data from lookup table
    val targetData = hiveContext.sql("select * from " + lookupDB + ".dim_member_multi_phone where trim(phone_number) is not null AND member_key is not null and status='current' order by member_key ")

    //sourceCMData.select("batch_id").distinct().show()
    val batch_id = sourceData.select("batch_id").take(1).head.getAs[String]("batch_id")
    val batch_id_col = sourceData.col("batch_id")
    val sourceDataColumnExpr = com.express.util.Settings.getMemberMultiPhoneTransformColumnMapping(processing_file)

    val targetTable = hiveContext.sql(s"select * from $lookupDB.dim_member_multi_phone where status='current'").withColumnRenamed("source_key","trg_source_key").drop("batch_id")
    val sourceTable = sourceData.join(targetTable,sourceData.col(memberKeyColCM) === targetTable.col("member_key") and sourceData.col(phoneColCM) === targetTable.col("phone_number"),"left")
      .withColumn("member_multi_phone_key_cm", lit(null))
      .withColumn(memberKeyColCM, sourceData.col(memberKeyColCM))
      .withColumn(phoneColCM, sourceData.col(phoneColCM))
      .withColumn("phone_type_cm", expr(sourceDataColumnExpr("phone_type_cm")))
      .withColumn("valid_phone_cm", expr(sourceDataColumnExpr("valid_phone_cm")))
      .withColumn("best_mobile_flag_cm", expr(sourceDataColumnExpr("best_mobile_flag_cm")))
      .withColumn("best_member_cm", expr(sourceDataColumnExpr("best_member_cm")))
      .withColumn("is_loyalty_flag_cm", expr(sourceDataColumnExpr("is_loyalty_flag_cm")))
      .withColumn("source_key_cm", expr(sourceDataColumnExpr("source_key_cm")))
      .withColumn("phone_consent_cm", when( col("source_key_cm") === 2226 ,expr(sourceDataColumnExpr("phone_consent_cm"))).otherwise(targetTable.col("phone_consent")))
      .withColumn("phone_consent_date_cm", when( col("source_key_cm") === 2226 ,expr(sourceDataColumnExpr("phone_consent_date_cm")).cast(TimestampType)).otherwise(targetTable.col("phone_consent_date")))
      .withColumn("original_source_key_cm", expr(sourceDataColumnExpr("source_key_cm")))
      .withColumn("match_type_key_cm", expr(sourceDataColumnExpr("match_type_key_cm")))
      .select("member_multi_phone_key_cm", sourceColumnList: _*)




    // Generate consent columns based on source table columns
    val transformedRecord=sourceTable
      .withColumn("action_cd", lit(null))
      .withColumn("last_updated_date", current_timestamp())
      .withColumn("batch_id", lit(batch_id) )
      .withColumn("process", lit(processing_file))


    // Insert into target table
    transformedRecord.insertIntoHive(SaveMode.Overwrite, TraansformCMResultTable, Some("process"), batch_id)
  }
}

