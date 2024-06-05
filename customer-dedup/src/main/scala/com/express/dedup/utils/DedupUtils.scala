package com.express.dedup.utils

import com.express.dedup.rules._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit, when}

object DedupUtils {

  //Constants
  val GroupingCriteriaIDColumn = "grouping_criteria_id"
  val GroupIDColumn = "grouping_id"
  val InvalidGroupIDStatusColumn = "is_invalid"
  val GroupedColumn = "grouped_data"
  val ResolvedColumn = "resolved"
  val ResolvedMemberIDColumn = "resolved_member"
  val ResolvedFlagColumn = "is_resolved"
  val CollectionIDColumn = "collection_id"
  val CollapsedMemberColumn = "collapsed_member"
  val ProcessType = "dedup"
  val isLoyalResolved = "is_loyalty_resolved"
  val actionFlag = "action_flag"
  val ResolutionInfoColumn = "resolution_info"

  /**
    * Identifies Grouping Criteria the Member record belongs to
    *
    * @param inputDF Member [[DataFrame]]
    * @return [[DataFrame]] with [[GroupingCriteriaIDColumn]] assigned
    *
    */


  // Dedup function list
  val DedupFunctionList: Seq[DeDupResolutionTrait] =
  Seq(DeDupOnNameAddressZipCode, DeDupOnBankCard, DeDupOnEmail, DeDupOnPhone, DeDupOnEmailPhone)

  def identifyGroups(inputDF: DataFrame): DataFrame = {
    inputDF.withColumn(GroupingCriteriaIDColumn,
      DedupFunctionList.foldLeft(None: Option[Column]) {
        case (None, function) => Some(when(function.groupingIdentificationRule, function.groupingCriteriaId))
        case (Some(condition), function) => Some(condition.when(function.groupingIdentificationRule, function.groupingCriteriaId))
      }.get.otherwise(lit(0))
    )
  }

  /**
    * Resolves the group based on the [[GroupingCriteriaIDColumn]] values
    *
    * @param inputDF [[DataFrame]] with group identified
    * @return [[DataFrame]] with resolution details for groups
    */
  def resolveGroups(inputDF: DataFrame): DataFrame = {
    DedupFunctionList.map(function =>
      inputDF
        .filter(col(GroupingCriteriaIDColumn) === function.groupingCriteriaId)
        .drop(GroupingCriteriaIDColumn)
        .transform(function.resolveGroup(_))
    ).reduce(_ unionAll _)
  }
}
