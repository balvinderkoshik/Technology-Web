package com.express.processing.dim

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by amruta.chalakh on 7/11/2017.
  */
object DimTier extends DimensionLoad with LazyLogging {

  override def dimensionTableName: String = "dim_tier"

  override def surrogateKeyColumn: String = "tier_key"

  override def naturalKeys: Seq[String] = Seq("tier_id")

  override def derivedColumns: Seq[String] = Nil

  override def transform: DataFrame = {
    val tierWorkTable = s"$workDB.work_bp_tiers_dataquality"
    logger.info("Reading Tier Work Table: {}", tierWorkTable)
    hiveContext.table(tierWorkTable)
      .withColumn("tier_source",lit("LW"))
      .withColumnRenamed("description", "tier_descr")
      .withColumnRenamed("entry_points", "min_points_for_tier")
      .withColumnRenamed("exit_points", "max_points_for_tier")
      .withColumn("point_event_names", lit(null))
      .withColumn("tier_point_type_name", lit(null))
      .withColumn("expire_date_expr",lit(null))
      .withColumn("activity_period_start_expr",lit(null))
      .withColumn("activity_period_end_expr",lit(null))
      .drop("last_dml_id")
      .drop("etl_unique_id")

  }

  def main(args: Array[String]): Unit = {
    val batchId = args(0)
    load(batchId)
  }

}
