package com.express.processing.enrichment

import com.express.cdw.Settings
import com.express.cdw.spark.DataFrameUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}


/**
  * Created by akshay.rochwani on 6/16/2017.
  */

object EnrichMemberMultiPhone extends LazyLogging {

  def bestPhoneForMember(phoneData: DataFrame): DataFrame = {
    import phoneData.sqlContext.implicits._
    val memberCountWindowSpec = Window.partitionBy("member_key")

    val setUnknownPhoneDF= phoneData.filter("phone_type!='MOBILE'").withColumn("best_mobile_flag_enriched",lit("NO"))

    val bestMobileDF=phoneData.filter("phone_type='MOBILE'")
      .withColumn("best_phone_rank",row_number().over(memberCountWindowSpec.orderBy(desc("acknowledgement"),desc("phone_consent_date"),desc("best_mobile_flag"))))
      .withColumn("best_mobile_flag_enriched",when($"best_phone_rank"===1,lit("YES")).otherwise("NO"))
      .drop("best_phone_rank")
    bestMobileDF.unionAll(setUnknownPhoneDF)
  }


  def bestMemberForPhone(phoneData: DataFrame): DataFrame = {

    import phoneData.sqlContext.implicits._
    val phoneCountWindowSpec = Window.partitionBy("phone_number")

    val setUnknownPhoneDF= phoneData.filter("phone_type!='MOBILE'").withColumn("best_member_flag_enriched",lit("NO"))

    val bestMember=phoneData.filter("phone_type='MOBILE'")
      .withColumn("best_member_rank",row_number().over(phoneCountWindowSpec.orderBy(asc("segment_rank"),desc("trxn_date"), desc("time_in_24hr_day"),desc("total_line_amnt_after_discount"),desc("best_member"))))
      .withColumn("best_member_flag_enriched",when($"best_member_rank"===1,lit("YES")).otherwise(lit("NO")))
      .drop("best_member_rank")
    bestMember.unionAll(setUnknownPhoneDF)
  }


  def main(args: Array[String]): Unit = {
    //Configurations
    System.setProperty("hive.exec.dynamic.partition.mode", "nonstrict")
    val conf = Settings.sparkConf
    val sc = new SparkContext(conf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val workDB = Settings.getWorkDB
    val goldDB = Settings.getGoldDB
    import hiveContext.implicits._

    val workTable = s"$workDB.dim_member_multi_phone"
//    val batch_id = args(0)
    val workBpMemberCm = hiveContext.sql(s"select member_key,primaryphonenumber from $workDB.work_bp_member_cm where trim(primaryphonenumber) <>'' or primaryphonenumber is not null")
      .withColumnRenamed("primaryphonenumber","phone_number")
      .withColumn("is_latest_loyalty_record",lit("YES"))

    //Load required Source tables
    val dimMemberMultiPhoneDF = hiveContext.sql(s"select * from $goldDB.dim_member_multi_phone where status='current' and member_key !='1134577523' ").persist()
    val scoringHistoryDF = hiveContext.sql(s"select member_key,segment_rank,scoring_date,scoring_history_id from $goldDB.fact_scoring_history ")
      .filter("member_key is not null")
      .withColumn("rank", row_number().over(Window.partitionBy("member_key").orderBy(desc("scoring_date"),desc("scoring_history_id"))))
      .filter("rank=1")
      .drop("rank")

    val bcResultsDF = hiveContext.sql(s"select phone_number,delivery_status,count(*) as acknowledgement from $goldDB.fact_broadcast_results where delivery_status='Y' group by phone_number,delivery_status")
      .drop("delivery_status")

    val timeDimension = hiveContext.table(s"$goldDB.dim_time").filter("status = 'current'").select("time_in_24hr_day", "time_key")

    val trxnDetailDF = hiveContext.table(s"$goldDB.fact_transaction_detail").filter("member_key != -1").select("member_key", "trxn_date", "trxn_time_key", "total_line_amnt_after_discount", "transaction_detail_id")
      .join(timeDimension, col("trxn_time_key") === col("time_key"), "left")
      .withColumn("rank", rank().over(Window.partitionBy("member_key").orderBy(desc("trxn_date"), desc("time_in_24hr_day"), desc("total_line_amnt_after_discount"),desc("transaction_detail_id"))))
      .filter("rank=1")
      .drop("rank")

    val joinedMultiPhoneWithBcResults = dimMemberMultiPhoneDF
      .join(bcResultsDF, Seq("phone_number"), "left")

    val bestPhoneForMemberEnriched = bestPhoneForMember(joinedMultiPhoneWithBcResults)

    val bestMemberForPhoneData = bestPhoneForMemberEnriched
      .join(trxnDetailDF, Seq("member_key"), "left")
      .join(scoringHistoryDF, Seq("member_key"), "left")

    val bestMemberForPhoneEnriched = bestMemberForPhone(bestMemberForPhoneData)

    val enrichedDF = bestMemberForPhoneEnriched .withColumn("action_flag", when(not($"best_mobile_flag_enriched" === $"best_mobile_flag")
      or not($"best_member_flag_enriched" === $"best_member"), lit("U")).otherwise(lit("NCD")))
      .withColumn("best_member", $"best_member_flag_enriched")
      .withColumn("best_mobile_flag", $"best_mobile_flag_enriched")

    val enrichLoyaltyPhone = enrichedDF.join(workBpMemberCm, Seq("member_key", "phone_number"), "left")
      .withColumn("loyalty_rank", row_number().over(Window.partitionBy("member_key", "is_loyalty_flag").orderBy(desc("is_latest_loyalty_record"))))
      .withColumn("is_loyalty_flag", when(col("is_loyalty_flag").equalTo("YES") and col("loyalty_rank").notEqual(1), lit("NO")).otherwise(col("is_loyalty_flag"))) /* update is_loyalty_flag within member */
//      .withColumn("phone_rank", row_number().over(Window.partitionBy("phone_number", "is_loyalty_flag").orderBy(desc("is_latest_loyalty_record"))))
//      .withColumn("is_loyalty_flag", when(col("is_loyalty_flag").equalTo("YES") and col("phone_rank").notEqual(1), lit("NO")).otherwise(col("is_loyalty_flag")))
      .withColumn("process", lit("enrich"))


    val targetCols = hiveContext.table(workTable).columns
    val finalResult = enrichLoyaltyPhone.select(targetCols.head, targetCols.tail :_*)
    finalResult.insertIntoHive(SaveMode.Overwrite, s"$workDB.dim_member_multi_phone", Some("process"), null)
  }
}