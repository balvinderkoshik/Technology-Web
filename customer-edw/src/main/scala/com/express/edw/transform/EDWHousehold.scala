package com.express.edw.transform

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions._

object EDWHousehold extends SmithLoad with LazyLogging{

  override def tableName: String = "edw_dim_household_history"

  override def sourcetable: String = s"$goldDB.dim_household"

  override def filterCondition: String = "status='current' and (record_type != 'PROSPECT' or record_type is null)"

  override def sourceColumnList: Seq[String] = Seq("household_key", "member_count", "first_trxn_date", "is_express_plcc", "add_date", "introduction_date",
    "last_store_purch_date", "last_web_purch_date", "account_open_date", "zip_code", "scoring_model_key", "scoring_model_segment_key", "valid_address",
    "record_type", "associated_member_key", "last_updated_date")

  def main(args: Array[String]): Unit = {
    val options = parse(args)
    val businessdate = options("businessdate")
    val rundate = options("rundate")
    val transformedDF=transformData(A_C_check=Some(col("add_date")),run_date = rundate,business_date = businessdate)
    load(s"$smithDB.edw_dim_household",transformedDF)
    load(transformedDF.filter(s"ingest_date > '$businessdate'"))
  }

}