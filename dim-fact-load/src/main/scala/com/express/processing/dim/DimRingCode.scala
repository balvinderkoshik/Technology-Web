package com.express.processing.dim

import com.express.processing.dim.DimPointEventType.{hiveContext, load, logger, workDB}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, udf}
/**
  * Created by manasee.kamble on 7/17/2017.
  */
object DimRingCode extends DimensionLoad with LazyLogging{
  override def dimensionTableName: String = "dim_ring_code"

  override def surrogateKeyColumn: String = "ring_code_id"

  override def naturalKeys: Seq[String] = Seq("campaign_key","campaign_code","cell_code","campaign_segment_key","ring_code")

  override def derivedColumns: Seq[String] = Nil

  override def transform: DataFrame = {
    val ringcodelogDQWorkTable = s"$workDB.work_ringcodelog_dataquality"
    logger.info("Reading work_ringcodelog_dataquality Work Table: {}", ringcodelogDQWorkTable)
    hiveContext.table(ringcodelogDQWorkTable).
      withColumnRenamed("campaign_key","campaign_key_source").
      withColumn("campaign_key",checkEmptyUDF(col("campaign_key_source"))).//here we are generating column campaign_key with value -1
      withColumn("campaign_code",col("rcl_cell_name")).//here campaign_code is from source table
      withColumn("campaign_segment_key",lit(-1)).
      withColumn("cell_code",checkEmptyUDF(col("cell_code"))).
      withColumn("ring_code",checkEmptyUDF(col("ring_code"))).
      withColumnRenamed("ring_code_channel","channel").
      withColumnRenamed("campaign_reporting_group","campaign_reporting_grp").
      withColumnRenamed("rcl_campaign_name","campaign_name").
      withColumn("elf_segment",lit(null)).
      withColumn("circulation",lit(null))
  }
  val checkEmptyUDF  = udf ((inputStr : String)=>{
    inputStr match {
      case "" => null
      case "null" => null
      case _ => inputStr
    }
  })
  def main(args: Array[String]): Unit = {
    val batchId = args(0)
    load(batchId)
  }

}
