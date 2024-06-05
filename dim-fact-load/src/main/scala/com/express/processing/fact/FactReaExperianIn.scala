package com.express.processing.fact

import com.express.cdw._
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import org.apache.spark.sql.functions.{current_timestamp, lit, udf, _}

/**
  * Created by poonam.mishra on 10/23/2017.
  */
object FactReaExperianIn extends FactLoad {

  override def factTableName = "fact_rea_experian_in"

  override def surrogateKeyColumn = "fact_rea_experian_out_key"

  override def transform: DataFrame = {
    val reaExperianDataset = hiveContext.sql(s"select * from $workDB.work_experian_rea_in_dataquality")

    val remove_brackets_mk: UserDefinedFunction = {
      udf(remove_brackets(_: String))
    }

    val factTransformationDataset = reaExperianDataset
      .withColumnRenamed("client_data","email_client_data")
      .select("first_name","last_name","address_1","address_2","city","state","postal_code","zip4_code","email","match_level","email_client_data","member_key")
      .withColumn("member_key",remove_brackets_mk(col("member_key")))
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_updated_date", current_timestamp)

    logger.debug("factTransformationDataset:" + factTransformationDataset.count().toString)
    factTransformationDataset
  }

  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }
}
