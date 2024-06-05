package com.express.processing.fact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by poonam.mishra on 5/16/2017.
  */
object FactQuarantineData extends FactLoad {

  override def factTableName = "fact_quarantine_data"

  override def surrogateKeyColumn = "fact_quarantine_data_id"

  override def transform: DataFrame = {
    val factQuarantineDataTransformed = hiveContext.sql(s"select member_key,cast(recipient_id AS string),mme_id,cast(address_id AS string)," +
      s"address AS email_address, cast(status AS string),cast(number_of_errors AS string),cast(error_reason AS string)," +
      s"error_text,cast(type AS string),update_status_date,last_error_date from $workDB.work_quarantinedata_dataquality")
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_updated_date", current_timestamp)
      .withColumn("status_text", lit(null))
      .withColumn("type_text", lit(null))
    logger.debug("Input record count for FactQuarantineData fact load: {}", factQuarantineDataTransformed.count.toString)
    factQuarantineDataTransformed
  }

  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }
}
