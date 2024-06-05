package com.express.edw.lookup


import com.express.edw.transform.SmithLoad
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions._

object EDWLkpDiscountType extends SmithLoad with LazyLogging{

  override def tableName: String = "edw_lkp_discount_type"

  override def sourcetable: String = s"$goldDB.dim_discount_type"

  override def filterCondition: String = "status='current'"

  override def sourceColumnList: Seq[String] = Seq("discount_type_key","discount_type_code","discount_deal_code","discount_type_description","discount_deal_description","data_source")

  def main(args: Array[String]): Unit = {
    val options = parse(args)
    val rundate = options("rundate")
    val transformedDF=transformData(run_date=rundate)
    load(transformedDF)
  }
}
