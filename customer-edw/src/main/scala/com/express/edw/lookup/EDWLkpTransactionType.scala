package com.express.edw.lookup

import com.express.edw.transform.SmithLoad
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions._

object EDWLkpTransactionType extends SmithLoad with LazyLogging{

  override def tableName: String = "edw_lkp_transaction_type"

  override def sourcetable: String = s"$goldDB.dim_transaction_type"

  override def filterCondition: String = "status='current'"

  override def sourceColumnList: Seq[String] = Seq("transaction_type_key","lbi_trxn_line_type_code","lbi_trxn_line_type_cat_code","price_modified_ind","mobile_pos_ind","associate_trxn_flag","tax_status_flag","transaction_type","loyalty_ind")

  def main(args: Array[String]): Unit = {
    val options = parse(args)
    val rundate = options("rundate")
    val transformedDF=transformData(run_date = rundate)
    load(transformedDF)
  }
}
