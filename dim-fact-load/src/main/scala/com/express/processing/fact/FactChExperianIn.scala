package com.express.processing.fact


import com.express.cdw._
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import com.express.cdw.spark.DataFrameUtils._
import org.apache.spark.sql.functions.{current_timestamp, lit, udf, _}

import scala.collection.GenSeq
/**
  * Created by poonam.mishra on 10/26/2017.
  */
object FactChExperianIn extends FactLoad {

  override def factTableName = "fact_ch_experian_in"

  override def surrogateKeyColumn = "fact_ch_experian_out_key"

  override def transform: DataFrame = {
    val chExperianDataset = hiveContext.sql(s"select * from $workDB.work_experian_ch_dataquality")

    val remove_brackets_mk: UserDefinedFunction = {
      udf(remove_brackets(_: String))
    }
    val latLongConv: UserDefinedFunction = {
      udf(latitudeLongitudeConv(_: String))
    }

    val ExcludedCols = List("filler1","filler2","filler3","filler4","filler5","filler6","filler7","filler8","filler9","filler10","filler11","filler12","filler13","filler14","filler15")
    val factTransformationDataset = chExperianDataset
      .select(chExperianDataset.getColumns(ExcludedCols):_*)
        .withColumn("latitude_6_decimals", latLongConv(col("latitude_6_decimals")))
        .withColumn("longitude_6_decimals", latLongConv(col("longitude_6_decimals")))
      .withColumn("member_key",remove_brackets_mk(col("member_key")))
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_updated_date", current_timestamp)
    println("factTransformationDataset")
    factTransformationDataset.printSchema()
    logger.debug("factTransformationDataset:" + factTransformationDataset.count().toString)
    factTransformationDataset
  }

  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }

}
