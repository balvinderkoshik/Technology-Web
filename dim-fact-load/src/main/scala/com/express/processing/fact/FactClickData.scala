package com.express.processing.fact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by poonam.mishra on 5/16/2017.
  */
object FactClickData extends FactLoad {

  override def factTableName = "fact_click_data"

  override def surrogateKeyColumn = "fact_click_data_id"

  override def transform: DataFrame = {
    val factClickDataTransformed = hiveContext.sql(s"select member_key,cast(recipient_id AS string),mme_id,email_address," +
      s"cast(trackinglogrcp_id AS string),cast(broadlogrcp_id AS string),cast(delivery_id AS string)," +
      s"delivery_code,cast(url_id AS string),source_url,cast(url_type AS string),url_label,url_category,os,log_date" +
      s" from $workDB.work_clickdata_dataquality")
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_updated_date", current_timestamp)
      .withColumn("url_type_text", lit(null))

    logger.debug("Input record count for FactClickData fact load: {}", factClickDataTransformed.count.toString)
    factClickDataTransformed
  }


  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }
}
