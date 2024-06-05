package com.express.processing.fact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by poonam.mishra on 5/16/2017.
  */
object FactDeliveryData extends FactLoad {

  override def factTableName = "fact_delivery_data"

  override def surrogateKeyColumn = "fact_delivery_data_id"

  override def transform: DataFrame = {
    val factDeliveryDataTransformed = hiveContext.sql(s"select member_key,cast(recipient_id AS string),mme_id,cast(broadlogrcp_id AS string),"+
      s"cast(delivery_id AS string),delivery_label,cast(campaign_id AS string),campaign_label,event_date,delivery_code,"+
      s"delivery_contact_date,email_address,cast(status AS string),cast(failure_reason AS string),cast(channel AS string),"+
      s"household_key,campaign_key,cell_code,control_type_code,targeting_date,mobile_number,"+
      s"full_campaign_path,campaign_bu_1,campaign_channel_1,campaign_bu_2,campaign_channel_2," +
      s"campaign_bu_3,campaign_channel_3,campaign_bu_4,campaign_channel_4,campaign_bu_5," +
      s"campaign_channel_5 from $workDB.work_deliverydata_dataquality")
      .withColumn("batch_id",lit(batchId))
      .withColumn("last_updated_date",current_timestamp)
      .withColumn("status_text_temp",lit("This column will be populated later"))
      .withColumn("status_text", lit(null))
      .withColumn("failure_reason_text",lit(null))
      .withColumn("channel_text",lit(null))
    logger.debug("Input record count for FactDeliveryData fact load: {}", factDeliveryDataTransformed.count.toString)
    factDeliveryDataTransformed
  }

  def main (args: Array[String]): Unit =  {
    batchId = args(0)
    load()
  }
}
