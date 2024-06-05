package com.express.processing.fact

/**
  * Created by aditi.chauhan on 7/24/2017.
  */
import com.express.cdw.Settings
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.types.DateType




object SegmentRank extends LazyLogging {


  def SegmentRankCalculator(hiveContext: HiveContext): DataFrame = {
 
    /*****Initializing required environment specific variables*****/

    val gold_db = Settings.getGoldDB

    val date = Calendar.getInstance.getTime
    val year_month_format = new SimpleDateFormat("YYYY-MM")
    val year_month = year_month_format.format(date)

    val current_dt_format = new SimpleDateFormat("YYYY-MM-dd")
    val current_dt = current_dt_format.format(date)

    val data_through_date = hiveContext.sql(s"""select cast(date_sub(to_date(fiscal_month_begin_date),1) as string) from $gold_db.dim_date
                                                where sdate = '$current_dt' and status='current'""").rdd.map(r => r(0).asInstanceOf[String])
                                       .collect.head

    /****Initializing hive context*****/
    import hiveContext.implicits._

   /*****Fetching data from dim_member,fact_transaction_detail,dim_scoring_model_segment*****/

    val MemberDataset = hiveContext.sql(s"select distinct member_key from $gold_db.dim_member where status = 'current' and customer_introduction_date <= to_date('$data_through_date')")

    val DimScoringSegmentDataset = hiveContext.sql(
      s"""select
          cast(segment_rank as string),
          scoring_model_segment_key,model_id,var_channel,var_plcc_holder
          from $gold_db.dim_scoring_model_segment
          where
          status='current'
          and model_id = 104
          and segment_in_use = 'YES'
          and segment_grouping_rank is not null""")

    logger.info("Fetched columns for dim_segment_scoring_model table")

    /***Fetching data for the last three years for each member to calculate number of distinct transactions and corresponding amount spent ****/

    val ThreeYearsDataset = hiveContext.sql(
      s"""select member_key,trxn_id,sum(unit_price_after_discount) as unit_price_after_discount
          from
          $gold_db.fact_transaction_detail
          where
          round(months_between(to_date('$data_through_date'),to_date(trxn_date))) between 0 and 36
          and trxn_date <= '$data_through_date'
          group by
          member_key,trxn_id
          """)

    val AggregatedDataset = ThreeYearsDataset
                             .groupBy($"member_key")
                             .agg(count($"trxn_id").alias("trips"),
                             sum($"unit_price_after_discount").alias("sum_calculated"))
                            .withColumn("ads",$"sum_calculated"/$"trips")

  /***Fetching minimum trxn_date which will be the first transaction date for a member.This analysis is performed on the entire data set****/


    val MinMaxTrxnDate = hiveContext.sql(
      s"""select member_key,
           datediff(to_date('$data_through_date'),to_date(max_trxn_date)) as days_since_last_trxn,
           datediff(to_date('$data_through_date'),to_date(min_trxn_date)) as days_since_first_trxn from
           (select member_key,min(trxn_date) as min_trxn_date,max(trxn_date) as max_trxn_date
           from
           $gold_db.fact_transaction_detail
           where trxn_date <= '$data_through_date'
           group by
           member_key) first_last_trxn""")

    /***Final Dataset formed from Fact Transaction Detail****/

    val FactTransactionDetail = AggregatedDataset.join(MinMaxTrxnDate,Seq("member_key"),"left")

    logger.info("Fetched relevant data from fact_transaction_detail table")

    val MemberFactTransactionDetail = MemberDataset.join(FactTransactionDetail, Seq("member_key"), "left")

    /****dataset for segment rank,segment_type,segment_group calculation****/

      val ScoringDataset = MemberFactTransactionDetail
        .selectExpr("member_key","trips","ads", "days_since_last_trxn", "days_since_first_trxn")
        .withColumn("scoring_attributes",CustomerCategory(col("trips"),col("ads"),col("days_since_last_trxn"),col("days_since_first_trxn")))
        .withColumn("segment_rank",col("scoring_attributes").getItem(0))
        .withColumn("segment_type",col("scoring_attributes").getItem(1))
        .withColumn("segment_group",col("scoring_attributes").getItem(2))
        .drop(col("scoring_attributes"))


    /***Joining the above dataset with dim_scoring_model_segment to fetch the scoring_model_segment_key based on segment_rank***/

    val JoinedDimSegmentModel =     ScoringDataset.join(DimScoringSegmentDataset,
                                    ScoringDataset.col("segment_rank") === DimScoringSegmentDataset.col("segment_rank"),"left")
                                   .drop(DimScoringSegmentDataset.col("segment_rank"))
                                   .withColumn("scoring_model_segment_key",$"scoring_model_segment_key")
                                   .withColumn("model_id",$"model_id")


    logger.info("Final Dataframe formed with columns member_key,segment_rank,segment_type,segment_group,scoring_model_segmet_key and model_id")

    logger.info("Function SegmentRankCalculator executed successfully")

    JoinedDimSegmentModel

       }


  
  /**********************************************************Defining Customer Category UDF ****************************************************************/

  val CustomerCategory = udf((trips : Int,ads : Double,days_since_last_trxn : Int,days_since_first_trxn : Int) => {

    var segment_rank = "0"
    var segment_type = "Unknown"
    var segment_group = "Unknown"

    if (trips >= 15 && ads >= 63 && days_since_first_trxn > 150 && days_since_last_trxn < 450) {
      segment_rank = "1"
      segment_type = "Premier"
      segment_group = "Best"
    } else if ((trips >= 6 && trips <= 14) && ads >= 81 && days_since_first_trxn > 150 && days_since_last_trxn < 450) {
      segment_rank = "2"
      segment_type = "Best Spender"
      segment_group = "Best"
    } else if (trips >= 15 && ads <= 63 && days_since_first_trxn > 150 && days_since_last_trxn < 450) {
      segment_rank = "3"
      segment_type = "Best Shopper"
      segment_group = "Core"
    } else if ((trips >= 6 && trips <= 14) && (ads >= 52 && ads < 81) && days_since_first_trxn > 150 && days_since_last_trxn < 450) {
      segment_rank = "4"
      segment_type = "Seasonal Shopper"
      segment_group = "Core"
    } else if ((trips >= 2 && trips < 6) && (ads >= 146) && days_since_first_trxn > 150 && days_since_last_trxn < 450) {
      segment_rank = "5"
      segment_type = "Wardrober"
      segment_group = "Core"
    } else if ((trips >= 6 && trips <= 14) && (ads < 52) && days_since_first_trxn > 150 && days_since_last_trxn < 450) {
      segment_rank = "6"
      segment_type = "Loyal Browser"
      segment_group = "Core"
    } else if ((trips >= 2 && trips < 6) && (ads >= 72 && ads < 146) && days_since_first_trxn > 150 && days_since_last_trxn < 450) {
      segment_rank = "7"
      segment_type = "OccasionalSpender"
      segment_group = "Core"
    } else if ((trips >= 4 && trips <= 6) && (ads < 72) && days_since_first_trxn > 150 && days_since_last_trxn < 450) {
      segment_rank = "8"
      segment_type = "Average Jane/Joe"
      segment_group = "Undervalued"
    } else if ((trips >= 2 && trips < 4) && (ads >= 40 && ads < 72) && days_since_first_trxn > 150 && days_since_last_trxn < 450) {
      segment_rank = "9"
      segment_type = "Annual Shopper"
      segment_group = "Undervalued"
    } else if (( trips >= 2 && trips < 4) && (ads < 40) && days_since_first_trxn > 150 && days_since_last_trxn < 450) {
      segment_rank = "10"
      segment_type = "Annual Browser"
      segment_group = "Undervalued"
    } else if (trips == 1 && days_since_first_trxn > 150 && days_since_last_trxn < 450) {
      segment_rank = "11"
      segment_type = "OneTimers"
      segment_group = "Undervalued"
    } else if (trips >= 2 && ads >= 75 && (days_since_first_trxn > 30 && days_since_first_trxn <= 150)) {
      segment_rank = "12"
      segment_type = "New30+Days,2+Trips,ADS>=$75"
      segment_group = "OneTimers/NewCustomers"
    } else if (trips >= 2 && ads < 75 && (days_since_first_trxn > 30 && days_since_first_trxn <= 150)) {
      segment_rank = "13"
      segment_type = "New30+Days,2+Trips,ADS<$75"
      segment_group = "OneTimers/New"
    } else if (trips == 1 && (days_since_first_trxn > 30 && days_since_first_trxn <= 150)) {
      segment_rank = "14"
      segment_type = "New 30+Days 1Trip"
      segment_group = "OneTimers/NewCustomers"
    } else if (days_since_first_trxn <= 30) {
      segment_rank = "15"
      segment_type = "New 0-30 Days"
      segment_group = "OneTimers/NewCustomers"
    } else if (days_since_last_trxn >= 450 && days_since_last_trxn < 690) {
      segment_rank = "16"
      segment_type = "Lapsed"
      segment_group = "Lapsed"
    }
    else if (days_since_last_trxn >= 690 && days_since_last_trxn <= 1080) {
      segment_rank = "17"
      segment_type = "Deeply Lapsed"
      segment_group = "Deeply Lapsed"
    } else {
      segment_rank = "0"
      segment_type = "Unknown"
      segment_group = "Unknown"
    }
    List(segment_rank, segment_type, segment_group)
  })
}