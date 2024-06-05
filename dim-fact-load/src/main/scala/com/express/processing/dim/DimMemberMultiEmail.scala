package com.express.processing.dim

/**
  * Created by anil.aleppy on 5/15/2017.
  */

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame


object DimMemberMultiEmail extends DimensionLoad with LazyLogging{

  override def dimensionTableName: String = "dim_member_multi_email"

  override def surrogateKeyColumn: String = "member_multi_email_key"

  override def naturalKeys: Seq[String] = Seq("member_key", "email_address")

  override def derivedColumns: Seq[String] = Seq("original_source_key","original_store_key")

  override def transform: DataFrame = {
    hiveContext.table(s"$workDB.dim_member_multi_email")
      .drop(surrogateKeyColumn)
      .drop("last_updated_date")
      .drop("action_flag")
  }

  def main (args: Array[String]): Unit = {
    val batchId = args(0)
    val process = args(1)
    load(batchId, process.trim)
  }
}
