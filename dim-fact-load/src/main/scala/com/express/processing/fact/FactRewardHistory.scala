package com.express.processing.fact

import com.express.cdw.spark.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.functions._

/**
  * Created by Mahendranadh.Dasari on 11-07-2017.
  */

object FactRewardHistory extends FactLoad {

  override def factTableName = "fact_reward_history"

  override def surrogateKeyColumn = "reward_history_id"

  override def transform: DataFrame = {
    //Loading dim_reward_def table
    val dimRewardDefDataset = hiveContext.sql(s"select reward_def_id,reward_def_key from $goldDB.dim_reward_def where status='current'")

    //Loading dim_date table
    val dimDateDataset = hiveContext.sql(s"select sdate,nvl(date_key,-1) as date_key from $goldDB.dim_date where status='current'")

    val dimMemberDF = hiveContext.sql(s"select ip_code,member_key from $goldDB.dim_member where status='current' and ip_code is not null")
      .withColumn("rank",row_number().over(Window.partitionBy(col("ip_code")) orderBy(desc("member_key"))))
      .filter("rank=1")
      .drop("rank")


    val lookUpFactRewardHistory=hiveContext.sql(s"select * from $goldDB.fact_reward_trxn_history")
      .select("fact_reward_trxn_history_id","trxn_id","certificate_nbr")
      .withColumnRenamed("certificate_nbr","certificate_nbr_trxn_history")
      .withColumn("rank",row_number().over(Window.partitionBy(col("certificate_nbr_trxn_history"))orderBy(desc("fact_reward_trxn_history_id"))))
      .filter("rank=1")
      .drop("rank")

    val lwMemberRewardsDataset = hiveContext.sql(s"select * from $workDB.work_bp_memberrewards_dataquality")
      .selectExpr("id",
        "rewarddef_id",
        "member_id",
        "certificate_nmbr",
        "available_balance",
        "fulfillment_option",
        "cast(date_issued as date)",
        "cast(expiration as date)",
        "cast(fulfillment_date as date)",
        "cast(redemption_date as date)",
        "product_id",
        "product_variant_id",
        "changed_by"
      )
      .withColumnRenamed("rewarddef_id","reward_def_id")
      .withColumnRenamed("certificate_nmbr", "certificate_nbr")
      .withColumnRenamed("date_issued", "reward_issue_date")
      .withColumnRenamed("expiration", "reward_expiration_date")
      .withColumnRenamed("fulfillment_date", "reward_fulfillment_date")
      .withColumnRenamed("redemption_date", "api_redemption_date")
    logger.info("lwMemberRewardsDataset :{}", lwMemberRewardsDataset.count().toString)


    //logger.info("join on dim_reward_def to get reward_def_key")
    val factTransformationDataset = lwMemberRewardsDataset.join(dimRewardDefDataset, Seq("reward_def_id"), "left")
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_updated_date", current_timestamp)
      .withColumn("trxn_redemption_date",lwMemberRewardsDataset.col("api_redemption_date"))
      .join(dimMemberDF,col("member_id")===col("ip_code"),"left")
      .drop("ip_code")
      .withColumn("ipcode",col("member_id"))
      .join(lookUpFactRewardHistory,col("certificate_nbr_trxn_history")===col("certificate_nbr"),"left")
      .withColumn("member_key",when(col("member_key").isNull,lit(-1)).otherwise(col("member_key")))

    logger.debug("factTransformationDataset:" + factTransformationDataset.count().toString)



    val dateColumns = Seq("reward_issue_date", "reward_expiration_date", "reward_fulfillment_date", "api_redemption_date","trxn_redemption_date")
    factTransformationDataset.updateKeys(dateColumns, dimDateDataset, "sdate", "date_key")
  }

  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }

}
