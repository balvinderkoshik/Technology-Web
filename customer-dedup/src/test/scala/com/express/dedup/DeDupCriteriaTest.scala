package com.express.dedup

import com.express.cdw.spark.DataFrameUtils._
import com.express.dedup.DataUtil._
import com.express.dedup.utils.DedupUtils._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by aman.jain on 6/28/2017.
  */
class DeDupCriteriaTest extends FlatSpec with Matchers {


  val goldDB = "gold"
  val workDB = "work"
  // Tables used for Dedup process
  val DimSourceTable = s"$goldDB.dim_source"
  val DimMemberSourceTable = s"$goldDB.dim_member"
  val DimCardTypeTable = s"$goldDB.dim_card_type"
  val DimTimeTable = s"$goldDB.dim_time"
  val FactCardHistorySourceTable = s"$goldDB.fact_card_history"
  val DimMemberMultiEmailSourceTable = s"$goldDB.dim_member_multi_email"
  val DimMemberMultiPhoneSourceTable = s"$goldDB.dim_member_multi_phone"
  val FactTransactionDetailSourceTable = s"$goldDB.fact_transaction_detail"
  val DimMemberDedupResultsTable = s"$workDB.dim_member"
  val DimDedupInfoLookUpTable = s"$goldDB.dim_dedupe_info"
  val FactDedupTargetTable = s"$goldDB.fact_dedupe_member_history"


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

  "DeDupOnBankCard function" should "generate the identification results correctly" in {

    val data = DataUtil.memberCardFinalDF
    data.select("member_key", "first_name", "last_name", "address1_scrubbed", "address2_scrubbed", "zip_code_scrubbed", "city_scrubbed", "state_scrubbed", "bank_card", "dedup_email_address", "dedup_phone_number").show(100, false)
    val identifiedGroupsDF = identifyGroups(data).persist
    val resolvedGroupDF = identifiedGroupsDF.transform(resolveGroups)
    resolvedGroupDF.select("member_key", "grouping_id", "grouped_count", "customer_introduction_date", "member_key", "first_name", "last_name", "address1_scrubbed", "address2_scrubbed", "zip_code_scrubbed", "city_scrubbed", "state_scrubbed", "bank_card", "dedup_email_address", "dedup_phone_number", "is_loyalty_member", "is_invalid", "grouping_criteria_id", "resolved.is_resolved").persist.show(100, false)


    //resolvedGroupDF.show(100,false)
    val (resolved, unresolved) = resolvedGroupDF
      .partition(col(s"$ResolvedColumn.$ResolvedFlagColumn") === true && CheckEmptyUDF(col(MemberLoyaltyIDColumn)))


    resolvedGroupDF.show(100,false)
    // Name , Address Matching Criteria 1
    resolvedGroupDF.filter(s"grouping_criteria_id = 1").count() should be(6)
    resolvedGroupDF.filter(s"grouping_criteria_id = 1 and grouping_id = 'EDMUNDO~RICO~4502 E INDIAN SCHOOL RD~APT 126~85018'").count() should be(2)

    //Fuzzy Matching Criteria 2
    resolvedGroupDF.filter(s"grouping_criteria_id = 2").count() should be(2)
    resolvedGroupDF.filter(s"grouping_criteria_id = 2 and grouping_id = '5920 E UNIVERSITY BLVD~APT 5107~75206~DALLAS~TX~1248302855|1238553898'").count() should be(2)
    resolvedGroupDF.filter(s"grouping_criteria_id = 2 and grouping_id = '5920 E UNIVERSITY BLVD~APT 5107~75206~DALLAS~TX~1248302855|1238553898'").withColumn("RESOLVED_MEMBER", col("resolved.resolved_member")).select("RESOLVED_MEMBER").head().getAs[Long]("RESOLVED_MEMBER") should be(1248302855L)

    //BankCard Matching Criteria 3

    resolvedGroupDF.filter(s"grouping_criteria_id = 3").count() should be(8)
    resolvedGroupDF.filter(s"grouping_criteria_id = 3 and is_invalid = false").count() should be(2)
    resolvedGroupDF.filter(s"grouping_criteria_id = 3 and is_invalid = true").count() should be(6)
    resolvedGroupDF
      .filter("grouping_criteria_id = 3 and grouping_id = 'B1'")
      .withColumn("bnk_crd_cnt", functions.size(col("bank_card")))
      .withColumn("rnk", rank() over Window.partitionBy("member_key", "bnk_crd_cnt").orderBy("grouping_id"))
      .filter("rnk > 1").count() shouldBe (0)

    //Email Matching Test criteria 5
    resolvedGroupDF.filter(s"grouping_criteria_id = 5").count() should be(5)
    resolvedGroupDF.filter(s"grouping_criteria_id = 5 and grouping_id = 'megancyphers@outlook.com'").count() should be(2)
    resolvedGroupDF.filter(s"grouping_criteria_id = 5").head().getAs[String]("grouping_id") should be("megancyphers@outlook.com")
    println("Criteria 5 successfull")

    //Phone Matching Test criteria 4
    resolvedGroupDF.filter(s"grouping_criteria_id = 4").count() should be(1)
    resolvedGroupDF.filter(s"grouping_criteria_id = 4 and grouping_id = '7818997162'").count() should be(2)
    resolvedGroupDF.filter(s"grouping_criteria_id = 4").head().getAs[String]("grouping_id") should be("7818997162")

    //Email and Phone Matching Test criteria 7
    resolvedGroupDF.filter(s"grouping_criteria_id = 7").count() should be(2)
    resolvedGroupDF.filter(s"grouping_criteria_id = 7 and grouping_id = 'aznchic91@yahoo.com~7634782180'").count() should be(2)
    resolvedGroupDF.filter(s"grouping_criteria_id = 7").head().getAs[String]("grouping_id") should be("aznchic91@yahoo.com~7634782180")

  }


}
