package com.express.edw.lookup

import com.express.edw.transform.SmithLoad
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions._

object EDWLkpLoyalty extends SmithLoad with LazyLogging{

  override def tableName: String = "edw_lkp_source"

  override def sourcetable: String = s"$goldDB.dim_source"

  override def filterCondition: String = "status='current'"

  override def sourceColumnList: Seq[String] = Seq("source_key","source_name","source_code","source_description","is_prospect","is_cheetahmail","is_plcc","is_smartreply","is_lw_pilot_conv","is_lw_national_conv","legacy_source_code","legacy_source_description","marketing_brand_code","marketing_brand")

  def main(args: Array[String]): Unit = {
    val options = parse(args)
    val rundate = options("rundate")
    val transformedDF=transformData(run_date = rundate)
    load(transformedDF)
  }
}
