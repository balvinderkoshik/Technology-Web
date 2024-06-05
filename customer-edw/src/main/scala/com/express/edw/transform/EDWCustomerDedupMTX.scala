package com.express.edw.transform


import org.apache.spark.sql.functions._

object EDWCustomerDedupMTX  extends SmithLoad {
  override def tableName: String = "edw_customer_dedup_mtx"

  override def sourcetable: String = s"$goldDB.fact_dedupe_member_history"

  override def filterCondition: String = "is_invalid=false"

  override def sourceColumnList: Seq[String] = Seq("old_member_key","new_member_key","last_updated_date")

  def main(args: Array[String]): Unit = {

    val options = parse(args)
    val businessdate = options("businessdate")
    val rundate = options("rundate")
    val transformedDF=transformData(A_C_check=Some(col("ingest_date")),sourceDF=sourceData.filter(s"to_date(last_updated_date)>'$businessdate'"),run_date=rundate,business_date = businessdate)
    load(transformedDF)
  }

}
