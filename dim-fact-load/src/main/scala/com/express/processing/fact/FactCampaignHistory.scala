package com.express.processing.fact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions._
/**
  * Created by Mahendranadh.Dasari on 21-08-2017.
  */

object FactCampaignHistory extends FactLoad {

  override def factTableName = "fact_campaign_history"

  override def surrogateKeyColumn = "fact_campaign_history_id"

  override def transform: DataFrame = {
    val dimMemberDF = hiveContext.sql(s"select ip_code,member_key from $goldDB.dim_member where status='current' and ip_code is not null")
      .withColumnRenamed("ip_code","dim_ip_code")
      .withColumn("rank",row_number().over(Window.partitionBy(col("dim_ip_code")) orderBy(desc("member_key"))))
      .filter("rank=1")
      .drop("rank")

    val factCampaignHistoryTransformed = hiveContext.table(s"$workDB.work_bp_campaign_dataquality")
      .withColumn("last_updated_date", current_timestamp)
    logger.debug("Input record count for fact campaign History Fact Load:{}", factCampaignHistoryTransformed.count.toString)
     factCampaignHistoryTransformed.join(dimMemberDF,col("dim_ip_code")===col("ip_code"),"left")
      .drop("dim_ip_code")
      .withColumn("member_key",when(col("member_key").isNull,lit(-1)).otherwise(col("member_key")))
  }

  def main(args: Array[String]): Unit = {
    load()
  }
}
