package com.express.processing.fact

import com.express.cdw._
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import org.apache.spark.sql.functions.{current_timestamp, lit, udf, _}

/**
  * Created by poonam.mishra on 10/23/2017.
  */
object FactReaAcxiomIn extends FactLoad {

  override def factTableName = "fact_rea_acxiom_in"

  override def surrogateKeyColumn = "fact_rea_acxiom_out_key"

  override def transform: DataFrame = {
    val reaAcxiomDataset = hiveContext.sql(s"select * from $workDB.work_acxiom_rea_in_dataquality")

    val remove_brackets_mk: UserDefinedFunction = {
      udf(remove_brackets(_: String))
    }

    val factTransformationDataset = reaAcxiomDataset
      .withColumnRenamed("ddqoutcleansedemail","ddq_cleansed_email")
      .withColumnRenamed("fname_foc","first_name").withColumnRenamed("lname_foc","last_name").withColumnRenamed("addr1_foc","address_1").withColumnRenamed("addr2_foc","address_2")
      .withColumnRenamed("city_foc","city").withColumnRenamed("state_foc","state").withColumnRenamed("zip_foc","zip")
      .select("email","ddq_cleansed_email","first_name","last_name","address_1","address_2","city","state","zip","member_key")
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
