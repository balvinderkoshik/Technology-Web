package com.express.processing.dim

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame

/**
  * Created by anil.aleppy on 5/15/2017.
  */
object DimMember extends DimensionLoad with LazyLogging {

  override def dimensionTableName: String = "dim_member"

  override def surrogateKeyColumn: String = "member_id"

  override def naturalKeys: Seq[String] = Seq("member_key")

  override def derivedColumns: Seq[String] = Nil

  override def transform: DataFrame = {
    hiveContext.table(s"$workDB.dim_member")
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
