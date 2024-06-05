package com.express.processing.transform


import java.text.SimpleDateFormat
import java.util.Calendar

import com.express.cdw.Settings
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs.GetTrxnIDUDF
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{expr, _}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.express.util.Settings._
/**
  * Created by anil.aleppy on 5/15/2017.
  */

class TransformMemberMultiEmail(sourceCMData: DataFrame, processing_file: String) extends LazyLogging {

  import TransformMemberMultiEmail._
  import sourceCMData.sqlContext.implicits._

  //__________________________UDF__________________________________
  val sourceDataColumnExpr = getMemberMultiEmailTransformColumnMapping(processing_file)
  //val ans = sourceDataDefaultColumnTransform(sourceCMData,sourceColumnList,sourceDataColumnExpr)


  // Deduplicate The insert records

  def loadSourceData() : DataFrame = {

   //def loadSourceData(sourceCMData: DataFrame, processing_file: String): DataFrame = {

    val dedupSourceData = processing_file match {
      case "member_email_update" =>
        sourceCMData.orderBy(col("last_modified_date")).registerTempTable("customer_matching_output_table")

        hiveContext.sql("select *,row_number() over (PARTITION BY member_key_cm,trim(lower(email_address_cm)) order by (case when trim(CONSENT_VALUE_RETAIL) == '1' THEN 'yes' when trim(CONSENT_VALUE_OUTLET) == '1' THEN 'yes' else 'no' end)) as rnk from customer_matching_output_table ")
      case "tlog_customer" =>
        val storeMaster = hiveContext.sql("select store_id,store_key from " + lookupDB + ".dim_store_master where store_id is not null and status='current'")
        val tlogTrxnIDUDF = GetTrxnIDUDF($"transaction_date_iso", $"store_id", $"register_id", $"transaction_number")
        val tlogSaleWork = hiveContext.table(TlogSaleWorkTable).withColumn(TransactionID, tlogTrxnIDUDF).select($"$TransactionID", $"transaction_time")
        val tlogNonMerchandise = hiveContext.table(TLogTransactionWorkTable).withColumn(TransactionID, tlogTrxnIDUDF).select($"$TransactionID", $"transaction_time")
        val tlogDiscount = hiveContext.table(TlogDiscountWorkTable).withColumn(TransactionID, tlogTrxnIDUDF).select($"$TransactionID", $"transaction_time")
        // Union the merchandise, non-merchandise and discount transactions
        val transactions = tlogSaleWork.unionAll(tlogNonMerchandise).unionAll(tlogDiscount)
        transactions.distinct.join(sourceCMData, Seq(TransactionID)).orderBy(col("transaction_date_iso").desc, col("transaction_time").desc).registerTempTable("customer_matching_output_table")
        val tlogCustomerDF = hiveContext.sql("select *,row_number() over (PARTITION BY member_key_cm,trim(lower(email_address_cm))) as rnk from customer_matching_output_table ")
          .withColumn("do_not_email", lit(null))
        //hiveContext.sql("select *,row_number() over (PARTITION BY member_key_cm,email_address_cm order by do_not_email) as rnk from customer_matching_output_table ")
        storeMaster.join(tlogCustomerDF, storeMaster.col("store_id") === tlogCustomerDF.col("store_id")).drop(storeMaster.col("store_id"))
      case "peoplesoft" =>
        sourceCMData.registerTempTable("customer_matching_output_table")
        hiveContext.sql("select *,row_number() over (PARTITION BY member_key_cm,trim(lower(email_address_cm))) as rnk from customer_matching_output_table")
      case "bp_emdmconsent" =>
        sourceCMData.registerTempTable("customer_matching_output_table")
        hiveContext.sql("select *,row_number() over (PARTITION BY member_key_cm,lower(email_address_cm) order by optdate) as rnk from customer_matching_output_table")
        val dimMmeDF = hiveContext.sql("select *,row_number() over (PARTITION BY member_key_cm,lower(email_address_cm) order by optdate) as rnk from customer_matching_output_table")
        val dimMemberDF = hiveContext.sql(s"select member_key as member_key_alias,is_loyalty_member from $lookupDB.dim_member where status='current'")
        val joinedDataset = dimMmeDF.join(dimMemberDF,col("member_key_cm") === col("member_key_alias"),"left")
          .drop(col("member_key_alias"))
        joinedDataset
      case "bp_member" =>
        sourceCMData.registerTempTable("customer_matching_output_table")
        val SourceDF = hiveContext.sql("select *,row_number() over (PARTITION BY member_key_cm,lower(email_address_cm) order by membercreatedate desc) as rnk from customer_matching_output_table")
        val storeMaster = hiveContext.sql("select store_id,store_key from " + lookupDB + ".dim_store_master where store_id is not null and status='current'")
        val joinedDF = SourceDF.join(storeMaster,col("a_enrollmentstorenumber") === col("store_id"),"left")
        joinedDF
      case "ads_400" =>
        sourceCMData.registerTempTable("customer_matching_output_table")
        val SourceDF = hiveContext.sql("select *,row_number() over (PARTITION BY member_key_cm,trim(lower(email_address_cm)) order by trim(email_opt_in_indicator)) as rnk from customer_matching_output_table")
        val storeMaster = hiveContext.sql("select store_id,store_key from " + lookupDB + ".dim_store_master where store_id is not null and status='current'")
        val joinedDF = SourceDF.join(storeMaster,col("application_store_number") === col("store_id"),"left")
        joinedDF
      case "ads_600" =>
        sourceCMData.registerTempTable("customer_matching_output_table")
        val SourceDF = hiveContext.sql("select *,row_number() over (PARTITION BY member_key_cm,trim(lower(email_address_cm)) order by trim(email_opt_in_indicator)) as rnk from customer_matching_output_table")
        val storeMaster = hiveContext.sql("select store_id,store_key from " + lookupDB + ".dim_store_master where store_id is not null and status='current'")
        val joinedDF = SourceDF.join(storeMaster,col("application_store_number") === col("store_id"),"left")
        joinedDF
      case _ =>
       sourceCMData.withColumn("rnk",lit(1))
    }

    /** Add dim_member_multi_email lookup here and join with dedupSourceData*/
    val DimMemberMultiEmailLookUp= hiveContext.table(s"$lookupDB.dim_member_multi_email")
      .filter("status='current'")
      .select("member_key","email_address","consent_date_retail","consent_value_retail","consent_date_outlet","consent_value_outlet","consent_date_next","consent_value_next","email_consent_date","email_consent","valid_email", "is_loyalty_email","first_pos_consent_date","mme_id","consent_history_id","welcome_email_consent_date","frequency")
      .withColumnRenamed("member_key","dim_member_key")
      .withColumnRenamed("email_address","dim_email_address")
      .withColumnRenamed("consent_date_retail","dim_consent_date_retail")
      .withColumnRenamed("consent_value_retail","dim_consent_value_retail")
      .withColumnRenamed("consent_date_outlet","dim_consent_date_outlet")
      .withColumnRenamed("consent_value_outlet","dim_consent_value_outlet")
      .withColumnRenamed("consent_date_next","dim_consent_date_next")
      .withColumnRenamed("consent_value_next","dim_consent_value_next")
      .withColumnRenamed("email_consent_date","dim_email_consent_date")
      .withColumnRenamed("frequency","dim_frequency")
      .withColumnRenamed("email_consent","dim_email_consent")
      .withColumnRenamed("valid_email","dim_valid_email")
      .withColumnRenamed("first_pos_consent_date","dim_first_pos_consent_date")
      .withColumnRenamed("mme_id","dim_mme_id")
      .withColumnRenamed("consent_history_id","dim_consent_history_id")
      .withColumnRenamed("welcome_email_consent_date","dim_welcome_email_consent_date")




    val JoinDimMemberMultiEmaildedupSourceData = dedupSourceData.join(DimMemberMultiEmailLookUp,
      dedupSourceData.col(memberKeyColCM)===DimMemberMultiEmailLookUp.col("dim_member_key") &&
        trim(lower(dedupSourceData.col(emailColCM)))===trim(lower(DimMemberMultiEmailLookUp.col("dim_email_address"))),"left")

    //val sourceTable = dedupSourceData.filter(col("rnk") === 1).distinct()
    val sourceTable = JoinDimMemberMultiEmaildedupSourceData.filter(col("rnk") === 1).distinct()
      .withColumn("member_multi_email_key_cm", lit(null))
      .withColumn(memberKeyColCM, dedupSourceData.col(memberKeyColCM))
      .withColumn(emailColCM, dedupSourceData.col(emailColCM))
      .withColumn("first_pos_consent_date_cm", expr(sourceDataColumnExpr("first_pos_consent_date_cm")).cast(DateType))
      .withColumn("consent_date_retail_cm", expr(sourceDataColumnExpr("consent_date_retail_cm")).cast(DateType))
      .withColumn("consent_value_retail_cm", expr(sourceDataColumnExpr("consent_value_retail_cm")))
      .withColumn("consent_date_outlet_cm", expr(sourceDataColumnExpr("consent_date_outlet_cm")).cast(DateType))
      .withColumn("consent_value_outlet_cm", expr(sourceDataColumnExpr("consent_value_outlet_cm")))
      .withColumn("consent_date_next_cm", expr(sourceDataColumnExpr("consent_date_next_cm")))
      .withColumn("consent_value_next_cm", expr(sourceDataColumnExpr("consent_value_next_cm")))
      .withColumn("email_consent_cm", expr(sourceDataColumnExpr("email_consent_cm")))
      .withColumn("email_consent_date_cm", expr(sourceDataColumnExpr("email_consent_date_cm")).cast(DateType))
      .withColumn("valid_email_cm", expr(sourceDataColumnExpr("valid_email_cm")))
      .withColumn("best_email_flag_cm", expr(sourceDataColumnExpr("best_email_flag_cm")))
      .withColumn("best_member_cm", expr(sourceDataColumnExpr("best_member_cm")))
      .withColumn("is_loyalty_email_cm", expr(sourceDataColumnExpr("is_loyalty_email_cm")))
      .withColumn("source_key_cm", expr(sourceDataColumnExpr("source_key_cm")))
      .withColumn("original_source_key_cm", expr(sourceDataColumnExpr("source_key_cm")))
      .withColumn("original_store_key_cm", expr(sourceDataColumnExpr("original_store_key_cm")))
      .withColumn("active_inactive_flag_cm", expr(sourceDataColumnExpr("active_inactive_flag_cm")))
      .withColumn("gender_version_cm", expr(sourceDataColumnExpr("gender_version_cm")))
      .withColumn("frequency_cm", when(expr(sourceDataColumnExpr("frequency_cm")).isNotNull,expr(sourceDataColumnExpr("frequency_cm"))).otherwise(DimMemberMultiEmailLookUp.col("dim_frequency")))
      .withColumn("affiliate_id_cm", expr(sourceDataColumnExpr("affiliate_id_cm")))
      .withColumn("sub_list_id_cm", expr(sourceDataColumnExpr("sub_list_id_cm")))
      .withColumn("consent_history_id_cm", expr(sourceDataColumnExpr("consent_history_id_cm")))
      .withColumn("mme_id_cm", expr(sourceDataColumnExpr("mme_id_cm")))
      .withColumn("welcome_email_consent_date_cm", expr(sourceDataColumnExpr("welcome_email_consent_date_cm")).cast(DateType))
      .withColumn("match_type_key_cm", if(processing_file == "bp_emdmconsent") lit(0) else col("match_key")) //col("match_key")
      .withColumn("action_cd", lit(null))
      .withColumn("last_updated_date", current_timestamp())
      .withColumn("batch_id", lit(null))
      .withColumn("rid_cm", expr(sourceDataColumnExpr("rid_cm")))
      .withColumn("process", lit(processing_file))
      .withColumn("first_pos_consent_date_cm", expr(
         """CASE WHEN process not in ("peoplesoft") and dim_first_pos_consent_date IS NULL THEN
            (CASE WHEN consent_value_outlet_cm = 'YES' AND consent_value_retail_cm = 'YES' THEN least(consent_date_retail_cm,consent_date_outlet_cm)
                 WHEN consent_value_outlet_cm = 'YES' AND ((consent_value_retail_cm = 'NO') OR (consent_value_retail_cm IS NULL) OR (length(consent_value_retail_cm) = 0) )
                 THEN consent_date_outlet_cm
                 WHEN consent_value_retail_cm = 'YES' AND ((consent_value_outlet_cm = 'NO') OR (consent_value_outlet_cm IS NULL) OR (length(consent_value_outlet_cm) = 0) )
                 THEN consent_date_retail_cm ELSE dim_first_pos_consent_date END) ELSE dim_first_pos_consent_date END"""))
      .select("member_multi_email_key_cm", sourceColumnList:_*)
//.withColumn("match_type_key_cm", when(col("processing")==="bp_emdmconsent",lit("0")).otherwise(col("match_key"))) //col("match_key")
     val batch_id =  dedupSourceData.select("batch_id").take(1).head.getAs[String]("batch_id")

     sourceTable.withColumn("batch_id", lit(batch_id) )



  }


}

object TransformMemberMultiEmail extends LazyLogging {


  System.setProperty("hive.exec.dynamic.partition", "true")
  System.setProperty("hive.exec.dynamic.partition.mode", "nonstrict")
  val workDB: String = Settings.getWorkDB
  val lookupDB: String = Settings.getGoldDB
  val format = new SimpleDateFormat("yyyy-dd-MM")
  val dateInstance = format.format(Calendar.getInstance.getTime)
  val emailCol = "email_address"
  val memberKeyCol = "member_key"
  val emailColCM = "email_address_cm"
  val memberKeyColCM = "member_key_cm"
  val joinType = "full"
  val insertAction = "I"
  val updateAction = "U"
  val conf = Settings.sparkConf
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)


  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  val TlogAdminWorkTable = s"$workDB.work_tlog_admin_dedup"
  val TlogDiscountWorkTable = s"$workDB.work_tlog_discount_dedup"
  val TlogSaleWorkTable = s"$workDB.work_tlog_sale_dedup"
  val TLogTenderWorkTable = s"$workDB.work_tlog_tender_dedup"
  val TLogTransactionWorkTable = s"$workDB.work_tlog_transaction_dedup"
  val TransactionID = "trxn_id"
  val TransformCMResultTable = s"$workDB.dim_member_multi_email"
  import sqlContext.implicits._
  val targetColumnList = Seq(memberKeyCol, emailCol, "first_pos_consent_date", "consent_date_retail", "consent_value_retail",
    "consent_date_outlet", "consent_value_outlet", "consent_date_next", "consent_value_next", "email_consent", "email_consent_date",
    "valid_email", "best_email_flag", "best_member", "is_loyalty_email", "source_key", "original_source_key", "original_store_key",
    "active_inactive_flag", "gender_version", "frequency", "affiliate_id", "sub_list_id", "consent_history_id", "rid","mme_id", "welcome_email_consent_date")

  val sourceColumnList = Seq(memberKeyColCM, emailColCM, "first_pos_consent_date_cm", "consent_date_retail_cm", "consent_value_retail_cm",
    "consent_date_outlet_cm", "consent_value_outlet_cm", "consent_date_next_cm", "consent_value_next_cm", "email_consent_cm", "email_consent_date_cm",
    "valid_email_cm", "best_email_flag_cm", "best_member_cm", "is_loyalty_email_cm", "source_key_cm", "original_source_key_cm", "original_store_key_cm",
    "active_inactive_flag_cm", "gender_version_cm", "frequency_cm", "affiliate_id_cm", "sub_list_id_cm", "consent_history_id_cm","mme_id_cm", "welcome_email_consent_date_cm",
    "match_type_key_cm","action_cd","last_updated_date","batch_id","rid_cm","process")


  def main(args: Array[String]): Unit = {

    val processing_file = args {0}.toLowerCase.trim()
    // Source: Customer_Matching Work Tables
    // Filter Null records from Natural key columns and deduplicate the source data
    val sourceCMData = if (processing_file == "bp_emdmconsent") {
      hiveContext.sql("select * from " + workDB + ".work_" + processing_file + "_dataquality where channel = 1 and trim(emailaddress) is not null " +
        "and trim(emailaddress) != '' and ipcode is not null and ipcode > 0 ")
        .withColumnRenamed("emailaddress","email_address")
        .withColumnRenamed("memberkey","member_key") //.withColumnRenamed("ipcode","member_key")
        .withColumnRenamed(emailCol, emailColCM)
        .withColumnRenamed(memberKeyCol, memberKeyColCM);
    }
    else if (processing_file == "bp_member"){
      hiveContext.sql("select * from " + workDB + ".work_" + processing_file + "_cm where trim(primaryemailaddress) is not null " +
      "and trim(primaryemailaddress) != '' and member_key is not null and member_key>0 ").withColumnRenamed("primaryemailaddress",emailColCM).withColumnRenamed(memberKeyCol, memberKeyColCM);
      }
    else {
      hiveContext.sql("select * from " + workDB + ".work_" + processing_file + "_cm where trim(email_address) is not null " +
        "and trim(email_address) != '' and member_key is not null and member_key>0 ").withColumnRenamed(emailCol, emailColCM).withColumnRenamed(memberKeyCol, memberKeyColCM);
    }


    val mmeTransform = new TransformMemberMultiEmail(sourceCMData,processing_file)
    val transformedRecord = mmeTransform.loadSourceData()
    transformedRecord
      .write.mode(SaveMode.Overwrite).partitionBy("process").insertInto(TransformCMResultTable)


  }

}

