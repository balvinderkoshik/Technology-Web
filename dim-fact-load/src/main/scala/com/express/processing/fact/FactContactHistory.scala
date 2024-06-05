package com.express.processing.fact

import com.express.cdw.spark.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, lit, _}


/**
  * Created by Mahendrandh.dasari on 10-02-2018.
  */

object FactContactHistory extends FactLoad {


  override def factTableName = "fact_contact_history"

  override def surrogateKeyColumn = "contact_history_id"

  override def transform: DataFrame = {

    /**
      * Lookup tables
      */

    val dimRingCode = hiveContext.sql(s"select campaign_key,campaign_segment_key from $goldDB.dim_ring_code where status='current'").persist()

    val dimScoringModel = hiveContext.sql(s"select scoring_model_key from $goldDB.dim_scoring_model where status='current'")
    val dimScoringSegmentModel = hiveContext.sql(s"select scoring_model_segment_key from $goldDB.dim_scoring_model_segment where status='current'")

    /**
      * Input Dataset from work_deliverydata_dataquality
      */

    val workDeliveryDataDF = hiveContext.sql(s"select member_key,mme_id,campaign_key,email_address,mobile_number from $workDB.work_deliverydata_dataquality")
      .withColumnRenamed("mme_id", "primary_customer_rid")
      .withColumnRenamed("mobile_number", "mobile_nbr")
      .withColumn("rid", lit(null))
      .withColumn("tracking_group_id", lit(null))
      .withColumn("version_tracking_end_date", lit(null))
      .withColumn("cs_descr", lit(null))
      .withColumn("mailed_campaign_segment_key", lit(null))
      .withColumn("email_definition_key", lit(null))
      .withColumn("cm_omniture_user_id", lit(null))
      .withColumn("scoring_model_key", lit(null))
      .withColumn("scoring_model_segment_key", lit(null))


    /**
      * Applying all the join logic
      */


val joinDimRingCodeDF =workDeliveryDataDF.join(broadcast(dimRingCode), Seq("campaign_key"), "left")
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_updated_date", current_timestamp)

    joinDimRingCodeDF


  }

  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }

}
