package com.express.processing.dim

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by Mahendranadh.Dasari on 06-09-2017.
  */

object DimKillList extends DimensionLoad with LazyLogging {


  override def dimensionTableName: String = "dim_kill_list"

  override def surrogateKeyColumn: String = "kill_list_id"

  override def naturalKeys: Seq[String] = Seq("ip_code", "household_key","first_name","last_name","address_line_one","zipcode")

  override def derivedColumns: Seq[String] = Nil

  override def transform: DataFrame = {
    val KilllistWorkTable = s"$workDB.work_bp_kill_suppression_dataquality"
    logger.info("Reading Kill List Work Table: {}", KilllistWorkTable)
    hiveContext.table(KilllistWorkTable)
      .withColumn("member_id",col("ip_code"))
      .withColumn("household_key",col("household_key"))
      .withColumn("member_key",col("member_key"))
      .withColumn("first_name",col("first_name"))
      .withColumn("name_prefix",col("name_prefix"))
      .withColumn("middle_name",col("middle_name"))
      .withColumn("last_name",col("last_name"))
      .withColumn("name_suffix",col("name_suffix"))
      .withColumn("address_line_one",col("address_line_one"))
      .withColumn("address_line_two",col("address_line_two"))
      .withColumn("address_line_three",col("address_line_three"))
      .withColumn("address_line_four",col("address_line_four"))
      .withColumn("city",col("city"))
      .withColumn("state",col("state"))
      .withColumn("zipcode",col("zip_code"))
      .withColumn("country",col("country"))
      .withColumn("status_code",col("status_code"))
  }

  def main(args: Array[String]): Unit = {
    val batchId = args(0)
    load(batchId)
  }

}
