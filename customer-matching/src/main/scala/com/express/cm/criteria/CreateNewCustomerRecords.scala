package com.express.cm.criteria


import com.express.cdw.MatchColumnAliases._
import com.express.cdw.isNotEmpty
import com.express.cdw.spark.DataFrameUtils._
import com.express.cm.MCDStatuses._
import com.express.cm.lookup.LookUpTable
import com.express.cm.lookup.LookUpTableUtil.getLookupTable
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import com.express.cm.{formatPhoneNumberUSCan,checkPhoneValidity}

import scala.collection.JavaConversions._


/**
  * Created by poonam.mishra on 4/24/2017.
  *
  * Contains functionality to create new customer records for unmatched customers in CM process
  */

object CreateNewCustomerRecords extends LazyLogging {

  val USAddressColumns = Seq(FirstNameAlias, LastNameAlias, AddressLineOneAlias, ZipCodeAlias)
  val CanadaAddressColumns = Seq(FirstNameAlias, LastNameAlias, AddressLineOneAlias, NonUSAPostalCodeAlias)
  val McdCheckAlias = "McdCheckAlias"
  val McdCheckStatusColumn = "mcd_rule_status"
  val MCDStatus = "MCD_Status"
  val MCDCriteria = "MCD_Criteria"
  val Anonymous: Int = -1
  val MemberKeyColumn = "member_key"
  val groupedDataCol = "grouped_data"

  /**
    * Perform Minimum Customer Definition Rule check Function
    *
    * @param columnUnderMCD Column present for MCD
    * @param row            Record on which mcd is to applied
    * @return MCD check status
    */
  private def mcdRuleCheck(columnUnderMCD: Seq[String], row: Row): (Boolean, String) = {

    if (columnUnderMCD.contains(ImpliedLoyaltyIdAlias)) {
      val loyaltyId = row.getAs[String](columnUnderMCD.indexOf(ImpliedLoyaltyIdAlias))
      if (loyaltyId != null && isNotEmpty(loyaltyId))
        return (true, MCDLoyalty)
    }

    if (USAddressColumns.forall(columnUnderMCD.contains(_))) {
      if (
        USAddressColumns.forall(col => {
          val rowValue = row.get(columnUnderMCD.indexOf(col))
          rowValue != null && isNotEmpty(rowValue.toString)
        })
      ) return (true, MCDNameAddressUS)
    }

    if (CanadaAddressColumns.forall(columnUnderMCD.contains(_))) {
      if (
        CanadaAddressColumns.forall(col => {
          val rowValue = row.get(columnUnderMCD.indexOf(col))
          rowValue != null && isNotEmpty(rowValue.toString)
        })
      ) return (true, MCDNameAddressCanada)
    }

    if (columnUnderMCD.contains(CardAlias)) {
      val bankCard = row.get(columnUnderMCD.indexOf(CardAlias))
      if (bankCard != null && isNotEmpty(bankCard.toString) && bankCard.toString.toLong != 0)
        return (true, MCDBankCard)
    }

    if (columnUnderMCD.contains(EmailAddressAlias)) {
      val emailAddress = row.getAs[String](columnUnderMCD.indexOf(EmailAddressAlias))
      //if (emailAddress != null)
      if (emailAddress != null && isNotEmpty(emailAddress))
        return (true, MCDEmail)
    }
    if (columnUnderMCD.contains(PhoneAlias)) {
      val phone = row.get(columnUnderMCD.indexOf(PhoneAlias))
      if (phone != null && phone != "UNKNOWN" && isNotEmpty(phone.toString) && formatPhoneNumberUSCan(phone.toString) != 0 && checkPhoneValidity(formatPhoneNumberUSCan(phone.toString)))
        return (true, MCDPhone)
    }
    (false, MCDFailed)
  }


  /**
    * Resolve and assign same member key to duplicates based on MCD criteria
    *
    * @param mcdSatisfied MCD satisfied data
    * @return [[DataFrame]] with duplicates resolved
    */
  private def resolveMCDDuplicates(mcdSatisfied: DataFrame,mcdColumns: Seq[String]): DataFrame = {
    mcdSatisfied.persist()
    val otherGroupingColumns = (USAddressColumns ++ CanadaAddressColumns).distinct.filter(mcdColumns.contains)
    val emptyDFSchema = StructType(Seq(StructField(groupedDataCol, ArrayType(mcdSatisfied.schema))))
    val emptyRow: List[Row] = List()
    val emptyDataframe = mcdSatisfied.sqlContext.createDataFrame(emptyRow, emptyDFSchema)

    val mcdStatusColumnMap = Map(
      MCDLoyalty -> Seq(ImpliedLoyaltyIdAlias),
      MCDNameAddressUS -> USAddressColumns,
      MCDNameAddressCanada -> CanadaAddressColumns,
      MCDBankCard -> (Seq(CardAlias) ++ otherGroupingColumns),
      MCDEmail -> (Seq(EmailAddressAlias) ++ otherGroupingColumns),
      MCDPhone -> (Seq(PhoneAlias) ++ otherGroupingColumns)
    )

    mcdStatusColumnMap.foldLeft(emptyDataframe) {
      case (df, (mcdStatus, columns)) =>
        val mcdStatusRecords = mcdSatisfied.filter(s"$MCDCriteria = '$mcdStatus'")
        if (mcdStatusRecords.isNotEmpty) {
          mcdStatusRecords
            .groupByAsList(columns)
            .select(groupedDataCol)
            .unionAll(df)
        } else
          df
    }
  }

  /**
    * Create new {member_key} values for Unmatched customers
    *
    * @param unmatched [[DataFrame]] containing unmatched records during CM process
    * @return Updated [[DataFrame]] with new Customers
    */
  def create(unmatched: DataFrame): DataFrame = {

    if (unmatched.count() == 0) {
      logger.info("Count is 0 : {}", "Numbers of unmatched customers is Zero: Skipping New Customer Creation")
      return unmatched
    }

    val lookUpDFMember = getLookupTable(LookUpTable.MemberDimensionLookup)
    //The list contains columns that are involved in NCD Rules
    val columnsUnderMCDRule = Seq(ImpliedLoyaltyIdAlias, EmailAddressAlias, FirstNameAlias, LastNameAlias,
      AddressLineOneAlias, ZipCodeAlias, NonUSAPostalCodeAlias, PhoneAlias, CardAlias)

    //The list contains columns that are unmatched as a result of CM
    val unmatchedColList: List[String] = unmatched.columns.toList

    val columnsPresentUnderMCDRule = columnsUnderMCDRule.filter(mcdColumn => unmatchedColList.contains(mcdColumn))

    val mcdRuleCheckUDF = udf[(Boolean, String), Row](mcdRuleCheck(columnsPresentUnderMCDRule, _: Row))

    import unmatched.sqlContext.implicits._
    val mcdCheck = unmatched.withColumn(McdCheckStatusColumn, mcdRuleCheckUDF(
      struct(columnsPresentUnderMCDRule.head, columnsPresentUnderMCDRule.tail: _*)))
      .withColumn(MCDStatus, $"$McdCheckStatusColumn._1")
      .withColumn(MCDCriteria, $"$McdCheckStatusColumn._2")
      .alias(McdCheckAlias)

    val mcdSatisfied = mcdCheck.filter($"$MCDStatus" === true)
      .select(mcdCheck.getColumns(McdCheckAlias, List(MemberKeyColumn)): _*)

    val mcdWithDuplicates = resolveMCDDuplicates(mcdSatisfied, columnsPresentUnderMCDRule)

    val mcdNotSatisfied = mcdCheck.filter($"$MCDStatus" === false)
      .select(mcdCheck.getColumns(McdCheckAlias, List(MemberKeyColumn)): _*)

    //get max member_key from dim_member
    val maxMemberId = lookUpDFMember.maxKeyValue($"$MemberKeyColumn")
    logger.info("Max Member key obtained: {}", maxMemberId.toString)
    val unmatchedNewMembers = mcdWithDuplicates.generateSequence(maxMemberId, Some(MemberKeyColumn))
    logger.info("Number of New Members : {}", unmatchedNewMembers.count().toString)

    val unmatchedNewMembersExploded = unmatchedNewMembers.selectExpr(MemberKeyColumn, "explode(grouped_data) as member_data")
      .unstruct("member_data", mcdSatisfied.schema)

    val unmatchedAnonymousMembers = mcdNotSatisfied
      .withColumn(MemberKeyColumn, lit(Anonymous))
      .select(unmatchedNewMembersExploded.getColumns: _*)
    logger.info("Number of Anonymous Members : {}", unmatchedAnonymousMembers.count().toString)

    unmatchedNewMembersExploded.unionAll(unmatchedAnonymousMembers)
  }

}