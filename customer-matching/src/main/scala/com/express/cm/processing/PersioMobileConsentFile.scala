package com.express.cm.processing

import com.express.cdw.SourceKeyColumn
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.MatchColumnAliases._
import com.express.cm.criteria.RecordInfo.assignRecordInfoKey
import com.express.cm.criteria._
import com.express.cm.spark.DataFrameUtils._
import com.express.cm.{AddressFormat, NameFormat, UDF}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.{Column, SaveMode}

/**
  * Created by poonam.mishra on 5/2/2017.
  */
object PersioMobileConsentFile extends CustomerMatchProcess with LazyLogging {

  val PersioMobileConsentSourceTable = s"$workDB.work_ir_mobile_consent_dataquality"
  val PersioMobileConsentCMResultTable = s"$workDB.work_ir_mobile_consent_cm"

  private val LongToStrUDF = udf((l: Long) => l.toString)
  private val PhoneCol = "phone_number"
  private val sortTrimCols = Seq(PhoneAlias)

  /**
    * Get the Source key for Input file
    *
    * @return [[Column]]
    */
  override def getSourceKey: Column = lit(2226)

  def main(args: Array[String]): Unit = {

    logger.info("Starting Customer Matching process for Persio Mobile Consent Data")
    val hiveContext = initCMContext
    import hiveContext.implicits._

    val nullStr: String = ""

    // Read source employee data
    logger.info("Reading Persio Mobile Consent data from Work layer")
    val persioMobileConsentData = hiveContext.table(PersioMobileConsentSourceTable)
      .withColumn(PhoneAlias, LongToStrUDF($"$PhoneCol"))
      .withColumn("firstName", lit(nullStr))
      .withColumn("lastName",lit(nullStr))
      .withColumn("address_line_1",lit(nullStr))
      .withColumn("city",lit(nullStr))
      .withColumn("state",lit(nullStr))
      .withColumn("zipCode",lit(nullStr))
      .drop(PhoneCol)

    // Initialize required UDFs
    val nameFormatUDF = UDF(udf(NameFormat("", _: String,"", _: String, "")),
      Seq("firstName","lastName"))

    val addressFormatUDF = UDF(udf(AddressFormat("","", _: String, _: String, _: String,"", "", ""
      , "")), Seq("city", "state", "zipCode"))

    // Create function list
    val customerMatchFunctionList: Seq[MatchTrait] = Seq(new NameAddressMatch(nameFormatUDF,addressFormatUDF), new PhoneMatch(Some(nameFormatUDF)))

    // Apply the match functions to employee data
    val (Some(matched), unmatched) = persioMobileConsentData.applyCustomerMatch(customerMatchFunctionList)

    val persioMobileConsentCMTable = hiveContext.table(PersioMobileConsentCMResultTable)
    // Create new customers for matched records
    //val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(unmatched)
    val sortUnmatched = sortTrimDF(unmatched,sortTrimCols)

    //unmatched.sort(PhoneAlias,"firstName","lastName","address_line_1","city","state","zipCode")

    val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(sortUnmatched)

    // Assign record information key
    val updatedWithRecordInfo = assignRecordInfoKey(matched.unionAll(unmatchedWithNewMemberKeys.select(matched.getColumns: _*)))

    //unmatchedWithNewMemberKeys.show()
    val finalPersioRecords = updatedWithRecordInfo
      .withColumnRenamed(PhoneAlias,PhoneCol)
      .withColumn(SourceKeyColumn, getSourceKey)
      .select(persioMobileConsentCMTable.getColumns: _*)

    // Write the Customer match results to output file
    finalPersioRecords.insertIntoHive(SaveMode.Overwrite, persioMobileConsentData, PersioMobileConsentCMResultTable)

  }
}
