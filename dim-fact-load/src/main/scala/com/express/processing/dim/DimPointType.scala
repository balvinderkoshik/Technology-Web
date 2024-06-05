package com.express.processing.dim

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
/**
  * Created by poonam.mishra on 7/11/2017.
  */
object DimPointType extends DimensionLoad with LazyLogging{
  val pointTypeTxnId = 1
  override def dimensionTableName: String = "dim_point_type"

  override def surrogateKeyColumn: String = "point_type_key"

  override def naturalKeys: Seq[String] = Seq("point_type_id")

  override def derivedColumns: Seq[String] = Nil

  override def transform: DataFrame = {
    val pointTypeWorkTable = s"$workDB.work_lw_point_type_dataquality"
    logger.info("Reading Point Type Work Table: {}", pointTypeWorkTable)
    hiveContext.table(pointTypeWorkTable)
      .withColumn("point_txn_type_id",lit(pointTypeTxnId))
      .withColumn("point_type",col("name"))
      .withColumn("point_type_descr",col("description"))
      .withColumn("point_basis",lit(null))
      .withColumn("point_group",lit(null))
      .withColumn("txn_type_descr",lit(null))//this column population logic will change
      .withColumn("money_backed_points",col("money_backed"))
      .withColumn("consumption_priority",col("consumption_priority"))

  }
 def main(args: Array[String]): Unit = {
    val batchId = args(0)
    load(batchId)
  }
}
