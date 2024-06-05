package com.express.processing.fact
import com.express.cdw._
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import org.apache.spark.sql.functions._
/**
  * Created by nikita.dane on 10/30/2017.
  */
object FactRPAExperian  extends FactLoad{
  /**
    * Get Fact Table name
    *
    * @return [[String]]
    */
  override def factTableName: String = "fact_rpa_experian"
  /**
    * Get Surrogate Key Column
    *
    * @return [[String]]
    */
  override def surrogateKeyColumn: String = "fact_rpa_experian_out_key"
  /**
    * Transformation for the Fact data
    *
    * @return Transformed [[DataFrame]]
    */
  override def transform: DataFrame = {
    //import hiveContext.implicits._
    val remove_brackets_mk: UserDefinedFunction = {
      udf(remove_brackets(_: String))
    }

    hiveContext.table(s"$workDB.work_experian_rpa_dataquality")
      .withColumnRenamed("secondary_address_line","ADDRESS_1")
      .withColumnRenamed("primary_address_line","ADDRESS_2")
      .withColumnRenamed("rrc","RECIPIENT_RELIABILITY_CODE")
      .withColumn("ZIP4_CODE", lit(null))
      .withColumn("member_key",remove_brackets_mk(col("member_key")))
      .withColumnRenamed("key_field","KEY_FIELD_PHONE")
      .withColumn("last_updated_date",current_timestamp)
      .withColumn("batch_id",lit(batchId))
  }
  def main (args: Array[String]): Unit =  {

    batchId = args(0)
    load()
  }
}