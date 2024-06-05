package com.express.dedup.rules

import org.apache.spark.sql.{Column, _}
import org.apache.spark.sql.functions._
import com.express.cdw.spark.DataFrameUtils._


/**
  * Created by aman.jain on 6/28/2017.
  */
object DeDupOnNameAddressZipCode extends DeDupResolutionTrait {


  override def groupingCriteriaId: Int = 1

  override def groupingIdentificationRule: Column =
    expr(" (not_empty(first_name) and first_name<>'UNKNOWN' ) " +
      "and (not_empty(last_name) and last_name<>'UNKNOWN') " +
      "and (not_empty(address1_scrubbed) and address1_scrubbed<>'UNKNOWN') " +
      "and (not_empty(zip_code_scrubbed) and zip_code_scrubbed<>'UNKNOWN')")

  override def groupingColumns: Seq[String] = Seq("first_name", "last_name", "address1_scrubbed", "address2_scrubbed", "zip_code_scrubbed")

  /**
    *
    * Groups and Resolves members in a group based on [[MemberResolution]] rules
    *
    * @param dataFrame [[DataFrame]] with groups identified
    * @return [[DataFrame]] with resolution details
    */
  override def resolveGroup(dataFrame: DataFrame, nonMemberColumns: Seq[String] = Nil): DataFrame = {
    val exactMatch = super.resolveGroup(dataFrame)
    exactMatch.cache
    // 1 -exact

    //Excluding members with first_name length < 1 and last_name length < 1
    val (exactMatchedValid, exactmatchInvalid) = exactMatch
      .partition(expr("length(first_name) > 1 and length(last_name) > 1"))
    val (fuzzyNameMatch, fuzzyNameMatchUnGrouped) = DeDupOnFuzzyName.fuzzyResolveOnName(exactMatchedValid.drop("is_invalid"))
    val (fuzzyAddressMatch, fuzzyAddressMatchUnGrouped) = DeDupOnFuzzyAddress
      .fuzzyResolveOnAddress(fuzzyNameMatchUnGrouped.drop("is_invalid").cache)
    val resolvedGroup = exactMatchedValid
      .filter("grouped_count != 1")
      .unionAll(exactmatchInvalid)
      .unionAll(fuzzyNameMatch.drop("identifier"))
      .unionAll(fuzzyAddressMatch.drop("identifier"))
      .unionAll(super.resolveGroup(fuzzyAddressMatchUnGrouped.drop("is_invalid")))
    resolvedGroup
  }
}

