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
  * Employee/Peoplesoft Customer Matching Process
  *
  * @author mbadgujar
  */
object EmployeeFile extends CustomerMatchProcess with LazyLogging {

  val EmployeeSourceTable = s"$workDB.work_peoplesoft_dedup"
  val EmployeeCMResultTable = s"$workDB.work_peoplesoft_cm"

  // Source to CM alias mapping
  private val CMAliasMap = Map("address" -> AddressLineOneAlias, "zip" -> ZipCodeAlias,
    "mobile_phone" -> MobileAlias)

  private val sortTrimCols = Seq(EmployeeIdAlias,FirstNameAlias,LastNameAlias,AddressLineOneAlias)

  /**
    * Get the Source key for Input file
    *
    * @return [[Column]]
    */
  override def getSourceKey: Column = lit(144)

  /**
    * Entry point for Employee file
    *
    * @param args unused
    */
  def main(args: Array[String]): Unit = {

    logger.info("Starting Customer Matching process for Employee/PeopleSoft Data")
    val hiveContext = initCMContext

    // Read source employee data
    logger.info("Reading Peoplesoft data from Work layer: {}", EmployeeSourceTable)
    val employeeData = hiveContext.table(EmployeeSourceTable)
      .withColumnRenamed("first_name_", FirstNameAlias) /* Column name incorrect in work */
      .renameColumns(CMAliasMap)

    // Initialize required UDFs
    val nameFormatUDF = UDF(udf(NameFormat("", _: String, _: String, _: String, "")),
      Seq(FirstNameAlias, "middle_initial", "last_name"))

    val addressFormatUDF = UDF(udf(AddressFormat(_: String, _: String, _: String, _: String, _: String, _: String, "", ""
      , "")), Seq(AddressLineOneAlias, "apartment_number", "city", "state", ZipCodeAlias, "zip4"))

    // Create function list
    val customerMatchFunctionList: Seq[MatchTrait] = Seq(new PhoneEmailImpliedLoyaltyMatch(nameFormatUDF), EmployeeIDMatch,
      new NameAddressMatch(nameFormatUDF, addressFormatUDF), new PhoneEmailNameMatch(nameFormatUDF),
      new PhoneEmailMatch(nameFormatUDF), new EmailNameMatch(nameFormatUDF), new EmailMatch,
      new PhoneNameMatch(nameFormatUDF), new PhoneMatch)

    // Apply the match functions to employee data
    val (Some(matched), unmatched) = employeeData.applyCustomerMatch(customerMatchFunctionList)

    val employeeCMTable = hiveContext.table(EmployeeCMResultTable)
    // Create new customers for matched records
    val sortUnmatched = sortTrimDF(unmatched,sortTrimCols)
      //unmatched.sort(AddressLineOneAlias, "apartment_number", "city", "state", ZipCodeAlias, "zip4",MobileAlias,FirstNameAlias,"middle_initial", "last_name")

    val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(sortUnmatched)
    //val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(unmatched)

    // Assign record information key
    val updatedWithRecordInfo = assignRecordInfoKey(matched.unionAll(unmatchedWithNewMemberKeys.select(matched.getColumns: _*)))

    val finalEmployeeRecords = updatedWithRecordInfo
      .renameColumns(CMAliasMap, reverse = true)
      .withColumn(SourceKeyColumn, getSourceKey)
      .select(employeeCMTable.getColumns: _*)

    // Write the Customer match results to output file
    finalEmployeeRecords.insertIntoHive(SaveMode.Overwrite, employeeData, EmployeeCMResultTable)
  }

}
