package com.express.processing.dim

/**
  * Created by anil.aleppy on 5/15/2017.
  */

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame


object DimHouseHold extends DimensionLoad with LazyLogging{

  override def dimensionTableName: String = "dim_household"

  override def surrogateKeyColumn: String = "household_id"

  override def naturalKeys: Seq[String] = Seq("household_key")

  override def derivedColumns: Seq[String] = Seq("add_date","add_date_key")

  override def transform: DataFrame = {
    hiveContext.table(s"$workDB.dim_household")
      .drop(surrogateKeyColumn)
      .drop("last_updated_date")
      .drop("action_flag")
  }

  def main (args: Array[String]): Unit = {
    val batchId = args(0)
    val process = args(1)
    if (process == "dim_household") {
      loader(batchId, process, derivedNew = derivedColumns ++ Seq("scoring_model_key","scoring_model_segment_key","record_type"))
    }
    else {
      load(batchId, process)
    }
  }
}