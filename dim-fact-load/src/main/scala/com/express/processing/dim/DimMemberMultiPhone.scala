package com.express.processing.dim

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame

/**
  * Created by amruta.chalakh on 6/13/2017.
  */
object DimMemberMultiPhone extends DimensionLoad with LazyLogging{

  override def dimensionTableName: String = "dim_member_multi_phone"

  override def surrogateKeyColumn: String = "member_multi_phone_key"

  override def naturalKeys: Seq[String] = Seq("member_key", "phone_number")

  override def derivedColumns: Seq[String] = Seq("original_source_key")

  override def transform: DataFrame = {
    hiveContext.table(s"$workDB.dim_member_multi_phone")
      .drop(surrogateKeyColumn)
      .drop("last_updated_date")
      .drop("action_flag")
  }

  def main (args: Array[String]): Unit = {
    val batchId = args(0)
    val process = args(1)
    load(batchId, process)
  }
}
