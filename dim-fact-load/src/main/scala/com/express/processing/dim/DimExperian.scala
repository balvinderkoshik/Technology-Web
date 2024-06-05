package com.express.processing.dim

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame


object DimExperian extends DimensionLoad with LazyLogging {
  override def dimensionTableName: String = "dim_experian"
  override def surrogateKeyColumn: String = "experian_key"
  override def naturalKeys: Seq[String] = Seq("member_key")
  override def derivedColumns: Seq[String] = Nil

  override def transform: DataFrame = {
  val DimExperianWorkTable = s"$workDB.work_Demographic_Overlay_DataQuality"
    logger.info("Reading Experian Demographic table: {}", DimExperianWorkTable)
    hiveContext.table(DimExperianWorkTable)

  }

  def main(args: Array[String]): Unit = {
    val batchId = args(0)
    load(batchId)
  }
}