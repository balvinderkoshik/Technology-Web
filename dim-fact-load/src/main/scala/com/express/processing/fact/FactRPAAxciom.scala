package com.express.processing.fact
import com.express.cdw._
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import org.apache.spark.sql.functions._

/**
  * Created by bhautik.patel on 24/10/17.
  */
object FactRPAAcxiom extends FactLoad{
  /**
    * Get Fact Table name
    *
    * @return [[String]]
    */
  override def factTableName: String = "fact_rpa_acxiom"

  /**
    * Get Surrogate Key Column
    *
    * @return [[String]]
    */
  override def surrogateKeyColumn: String = "fact_rpa_acxiom_id"

  /**
    * Transformation for the Fact data
    *
    * @return Transformed [[DataFrame]]
    */
  override def transform: DataFrame = {
    import hiveContext.implicits._
    val remove_brackets_mk: UserDefinedFunction = {
      udf(remove_brackets(_: String))
    }

    hiveContext.table(s"$workDB.work_acxiom_rpa_dataquality")
      .withColumn("member_key",remove_brackets_mk(col("member_key")))
      .withColumn("last_updated_date",current_timestamp)
      .withColumn("batch_id",lit(batchId))
  }

  def main (args: Array[String]): Unit =  {
    batchId = args(0)
    load()
  }
}
