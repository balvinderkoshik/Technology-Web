package com.express.edw.lookup

import com.express.edw.transform.SmithLoad
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions._

object EDWLkpScoringModel extends SmithLoad with LazyLogging{

  override def tableName: String = "edw_lkp_scoring_model"

  override def sourcetable: String = s"$goldDB.dim_scoring_model"

  override def filterCondition: String = "status='current'"

  override def sourceColumnList: Seq[String] = Seq("scoring_model_key","model_id","model_version_id","model_source","marketing_brand_model_code","model_description","model_level_code","model_level","scored_record_count","current_model_ind","effective_date","expiration_date","data_through_date")

  def main(args: Array[String]): Unit = {
    val options = parse(args)
    val rundate = options("rundate")
    val transformedDF=transformData(run_date = rundate)
    load(transformedDF)
  }
}
