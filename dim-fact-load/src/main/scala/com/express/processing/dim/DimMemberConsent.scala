package com.express.processing.dim
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame

/**
  * Created by bhautik.patel on 13/07/17.
  */
object DimMemberConsent extends DimensionLoad with LazyLogging {

  override def dimensionTableName: String = "dim_member_consent"

  override def surrogateKeyColumn: String = "consent_history_id"

  override def naturalKeys: Seq[String] = Seq("member_key","consent_type")//,"consent_subtype")

  override def derivedColumns: Seq[String] = Seq("original_source_key")

  override def transform: DataFrame = hiveContext.table(s"$workDB.dim_member_consent")
    .drop(surrogateKeyColumn)
    .drop("last_updated_date")
    .drop("action_cd")

  def main (args: Array[String]): Unit = {
    val batchId = args(0)
    val process = args(1)
    load(batchId, process)
  }
}
