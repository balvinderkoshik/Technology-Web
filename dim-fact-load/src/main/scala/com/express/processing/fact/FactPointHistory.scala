package com.express.processing.fact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * Created by anish.nair on 7/12/2017.
  */
object FactPointHistory extends FactLoad {


  override def factTableName = "fact_point_history"

  override def surrogateKeyColumn = "point_history_id"

  override def transform: DataFrame = {

    /**
      * Lookup tables
      */
    val dimDateDataset = hiveContext.sql(s"select nvl(date_key,-1) as date_key,sdate from $goldDB.dim_date where status='current'")

    val factPointHistoryWindow = Window.partitionBy("point_transaction_id")

    val factPointHistoryDataset = hiveContext.sql(s"select * from $goldDB.fact_point_history")
      .withColumn("rank",row_number().over(factPointHistoryWindow.orderBy(desc("txn_date"),desc("point_history_id"))))
      .filter("rank=1")
      .select(col("point_transaction_id"),col("point_history_id"))
      .withColumnRenamed("point_transaction_id", "fact_point_transaction_id")



    val factCardHistoryDataset = hiveContext.sql(s"""select vc_key,member_key,ip_code,
                                                     row_number() over (partition by vc_key order by card_history_id desc) as rank from $goldDB.fact_card_history
                                                     where vc_key is not null and vc_key <> -1 and (ascii(vc_key) != 0) """ )
                                                    .filter("rank=1")
                                                    .withColumnRenamed("member_key", "fact_card_member_key")
                                                    .withColumnRenamed("ip_code", "fact_card_ip_code")

    val factRewardHistoryDataset = hiveContext.sql(s"select id from $goldDB.fact_reward_history").distinct()

    /**
      * Input Dataset from work_bp_memberpoints_dataquality
      */
    val memberPointsDataset = hiveContext.sql(s"select point_transaction_id,vc_key,point_type_id,point_type_name,point_type_description," +
      s"point_event_id,point_event_name,point_event_description,transaction_type,cast(transaction_date as date),cast(point_award_date as date),points,cast(expiration_date as date)," +
      s"points_consumed,points_on_hold,money_backed,consumption_priority,default_points,owner_type,owner_id,row_key," +
      s"parent_transaction_id from $workDB.work_bp_memberpoints_dataquality")
      .withColumnRenamed("points", "points_earned")
      .withColumnRenamed("point_award_date", "points_awarded_date")
      .withColumnRenamed("transaction_date", "txn_date")
      .withColumn("promo_code", lit(null))
      .withColumn("expiration_reason", lit(null))
      .withColumn("transaction_type_description",   when(col("transaction_type") === 1,lit("CREDIT"))
                                                    .when(col("transaction_type") === 2,lit("DEBIT"))
                                                    .when(col("transaction_type") === 3,lit("HOLD"))
                                                    .when(col("transaction_type") === 4,lit("POINTS CONSUMED"))
                                                    .when(col("transaction_type") === 5,lit("POINTS TRANSFER")))

    /**
      * Applying all the join logic
      */

    val joinFactCardHistoryDF = memberPointsDataset.join(factCardHistoryDataset, Seq("vc_key"), "left")
      .withColumn("member_key", col("fact_card_member_key"))
      .withColumn("ip_code", col("fact_card_ip_code"))
      .drop("fact_card_member_key").drop("fact_card_ip_code")

    val joinFactRewardHistoryDataset = joinFactCardHistoryDF.join(factRewardHistoryDataset,
       memberPointsDataset.col("point_transaction_id") === factRewardHistoryDataset.col("id"), "left")
      .withColumn("associated_reward_history_id", col("id")).drop("id")

    val joinFactPointHistoryDataset = joinFactRewardHistoryDataset.join(factPointHistoryDataset,
       joinFactRewardHistoryDataset.col("parent_transaction_id") === factPointHistoryDataset.col("fact_point_transaction_id"), "left")
      .withColumnRenamed("point_history_id", "consumes_point_history_id")
      .drop("fact_point_parent_transaction_id")
      .drop("fact_point_transaction_id")

    val joinedExpirationWithDate = joinFactPointHistoryDataset.join(dimDateDataset,
      joinFactPointHistoryDataset.col("expiration_date") === dimDateDataset.col("sdate"), "left")
      .select("date_key", joinFactPointHistoryDataset.columns: _*)
      .withColumnRenamed("date_key", "expiration_date_key")

    val joinedTrxnDateWithDate = joinedExpirationWithDate.join(dimDateDataset,
      joinFactPointHistoryDataset.col("txn_date") === dimDateDataset.col("sdate"), "left")
      .select("date_key", joinedExpirationWithDate.columns: _*)
      .withColumnRenamed("date_key", "txn_date_key")

    joinedTrxnDateWithDate.join(dimDateDataset,
      joinFactPointHistoryDataset.col("points_awarded_date") === dimDateDataset.col("sdate"), "left")
      .select("date_key", joinedTrxnDateWithDate.columns: _*)
      .withColumnRenamed("date_key", "points_awarded_date_key")
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_updated_date", current_timestamp)
      .withColumn("card_type_key", lit(39))
      .withColumn("point_source_id", lit(-1))
      .withColumn("promotion_key", lit(null))
      .withColumnRenamed("ip_code","ipcode")
      .na.fill(-1,Seq("consumes_point_history_id","promotion_key"))
      .withColumn("associated_reward_history_id", when(col("transaction_type") === 4 and col("owner_type") === 6,col("row_key")).otherwise(lit(null)))
      .withColumn("consumes_point_history_id", col("parent_transaction_id"))
  }

  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }

}
