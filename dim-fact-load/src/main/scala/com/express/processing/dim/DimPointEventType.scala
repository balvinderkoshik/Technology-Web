package com.express.processing.dim

import com.express.processing.dim.DimEmployee.{hiveContext, load, logger, workDB}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame

/**
  * Loading Dim_point_event table
  * Source : work_lw_pointtransaction_etl_pe_dataquality
  * Created by manasee.kamble on 7/11/2017.
  */
object DimPointEventType extends DimensionLoad with LazyLogging {

  override def dimensionTableName: String = "dim_point_event"

  override def surrogateKeyColumn: String = "point_event_key"

  override def naturalKeys: Seq[String] = Seq("point_event_id")

  override def derivedColumns: Seq[String] = Nil

  override def transform: DataFrame = {
    val pointeventWorkTable = s"$workDB.work_lw_point_event_dataquality"
    logger.info("Reading work_lw_point_event_dataquality Work Table: {}", pointeventWorkTable)
    hiveContext.table(pointeventWorkTable).withColumnRenamed("name","point_event_name").
      withColumnRenamed("description","point_event_descr")
  }

  def main(args: Array[String]): Unit = {
    val batchId = args(0)
    load(batchId)
  }


}
