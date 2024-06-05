package com.express.processing.fact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by Mahendranadh.Dasari on 15-06-2017.
  */
object FactBroadcastResults extends FactLoad {

  override def factTableName = "fact_broadcast_results"

  override def surrogateKeyColumn = "fact_broadcast_results_id"

  override def transform: DataFrame = {
    val factBroadcastResultsTransformed = hiveContext.table(s"$workDB.work_ir_bc_results_dataquality")
      .withColumnRenamed("container", "ir_campaign_id").withColumn("last_updated_date", current_timestamp)
    logger.debug("Input record count for FactBroadcastResults fact load: {}", factBroadcastResultsTransformed.count.toString)
    factBroadcastResultsTransformed
  }

  def main(args: Array[String]): Unit = {
    load()
  }
}
