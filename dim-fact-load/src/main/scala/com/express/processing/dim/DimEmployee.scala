package com.express.processing.dim

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Employee Dimension Load
  *
  * @author mbadgujar
  */
object DimEmployee extends DimensionLoad with LazyLogging {

  private val dedupProcess: String = "dedup"

  private var processType: Option[String] = None

  override def dimensionTableName: String = "dim_employee"

  override def surrogateKeyColumn: String = "employee_key"

  override def naturalKeys: Seq[String] = Seq("employee_id")

  override def derivedColumns: Seq[String] = Nil

  override def transform: DataFrame = {
    processType match {
      case Some(`dedupProcess`) =>
        hiveContext.table(s"$workDB.dim_employee")
      case _ =>
        val employeeWorkTable = s"$workDB.work_peoplesoft_cm"
        logger.info("Reading Employee Work Table: {}", employeeWorkTable)
        hiveContext.table(employeeWorkTable)
          .withColumn("employee_status", when(col("employee_status") === "", null).otherwise(col("employee_status")))
          .withColumn("full_part_time_indicator", when(col("full_part_time_indicator") === "", null).otherwise(col("full_part_time_indicator")))
          .withColumnRenamed("location", "location_cd")
          .withColumnRenamed("division_code", "division_cd")
          .withColumnRenamed("discount_percentage", "discount_pct")
          .withColumnRenamed("full_part_time_indicator", "full_part_time_ind")
          .withColumnRenamed("home_store_number", "home_store_key")
    }
  }

  def main(args: Array[String]): Unit = {
    val batchId = args(0)
    if (args.length > 1)
      processType = Some(args(1))
    load(batchId)
  }
}
