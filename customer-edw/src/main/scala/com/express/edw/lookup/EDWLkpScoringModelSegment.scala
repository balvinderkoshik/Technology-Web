package com.express.edw.lookup

import com.express.edw.transform.SmithLoad
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions._

object EDWLkpScoringModelSegment extends SmithLoad with LazyLogging{

  override def tableName: String = "edw_lkp_scoring_model_segment"

  override def sourcetable: String = s"$goldDB.dim_scoring_model_segment"

  override def filterCondition: String = "status='current'"

  override def sourceColumnList: Seq[String] = Seq("scoring_model_segment_key","model_id","segment_type","segment_rank","segment_description","segment_grouping","segment_grouping_rank","segment_in_use")

  def main(args: Array[String]): Unit = {
    val options = parse(args)
    val rundate = options("rundate")
    val transformedDF=transformData(run_date = rundate)
    load(transformedDF)
  }
}
