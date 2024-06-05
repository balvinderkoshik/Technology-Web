package com.express.cm.processing

import com.express.cdw.MatchColumnAliases._
import com.express.cdw.SourceKeyColumn
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw._
import com.express.cm.criteria.RecordInfo.assignRecordInfoKey
import com.express.cm.criteria._
import com.express.cm.spark.DataFrameUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SaveMode}


/**
  * Created by bhautik.patel on 27/04/17.
  */
object EmailSubscribeUnsubscribeFile extends CustomerMatchProcess with LazyLogging {

  val EmailSubscribeUnsubscribeSourceTable = s"$workDB.work_memberemailupdate_dataquality"
  val EmailSubscribeUnsubscribeCMResultTable = s"$workDB.work_member_email_update_cm"

  private val sortTrimCols = Seq("mme_id",EmailAddressAlias)
  /**
    * Get the Source key for Input file
    *
    * @return [[Column]]
    */
  // TODO: update correct source key
  override def getSourceKey: Column = lit(2225)


  /**
    * Entry point for
    *
    * @param args not used
    */
  def main(args: Array[String]): Unit = {

    logger.info("Starting Customer Matching process for EmailSubscribeUnsubscribeFile Data")
    val hiveContext = initCMContext

    // Read source employee data
    logger.info("Reading EmailSubscribeUnsubscribeFile data from Work layer")
    val emailData = hiveContext.table(EmailSubscribeUnsubscribeSourceTable)
      .withColumn(FirstNameAlias, lit(""))
      .withColumn(LastNameAlias, lit(""))
      .withColumn(AddressLineOneAlias, lit(""))
      .withColumn(ZipCodeAlias, lit(""))
    //TODO: Add/Update columns using REA


    val sourceDataWithMemberKey = assignRecordInfoKey(emailData.filter("member_key is not null and member_key>0"))
      .withColumn(MatchTypeKeyColumn, lit(0))
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(SourceKeyColumn, getSourceKey)
      .withColumn(MCDCriteriaColumn,lit("matched"))

    val sourceDataWithoutMemberKey = emailData.filter("member_key is null or member_key<1")

    // Create function lists
    val customerMatchFunctionList: Seq[MatchTrait] = Seq(new EmailMatch)

    // Apply the match functions to employee data
    val (Some(matched), unmatched) = sourceDataWithoutMemberKey.applyCustomerMatch(customerMatchFunctionList)

    val sortUnmatched = sortTrimDF(unmatched,sortTrimCols)
      //unmatched.sort(FirstNameAlias,LastNameAlias,ZipCodeAlias,AddressLineOneAlias,EmailAddressAlias)
    val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(sortUnmatched)
    val emailSubscribeUnsubscribeCMTable = hiveContext.table(EmailSubscribeUnsubscribeCMResultTable)

    // Assign record information key
    val updatedWithRecordInfo = assignRecordInfoKey(matched.unionAll(unmatchedWithNewMemberKeys.select(matched.getColumns: _*)))

    val finalEmailSubscribeUnSubscribeRecords = updatedWithRecordInfo
      .withColumn(SourceKeyColumn, getSourceKey)
      .select(emailSubscribeUnsubscribeCMTable.getColumns: _*)
      .unionAll(sourceDataWithMemberKey.select(emailSubscribeUnsubscribeCMTable.getColumns: _*))

    finalEmailSubscribeUnSubscribeRecords.insertIntoHive(SaveMode.Overwrite, emailData, EmailSubscribeUnsubscribeCMResultTable)

  }
}