package com.express.cm.processing

import com.express.cdw.MatchColumnAliases._
import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cm.criteria.RecordInfo.assignRecordInfoKey
import com.express.cm.criteria.{MatchTrait, NameAddressMatch, _}
import com.express.cm.spark.DataFrameUtils._
import com.express.cm.{AddressFormat, NameFormat, UDF}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.{Column, SaveMode}

/**
  * Created by aman.jain on 5/2/2017.
  */
object RentalListVenusFile extends CustomerMatchProcess with LazyLogging {

  val RentalListVenusSourceTable = s"$workDB.work_rentallist_venus_dataquality"
  val RentalListVenusCMResultTable = s"$workDB.work_rentallist_venus_cm"


  /**
    * Get the Source key for Input file
    *
    * @return [[Column]]
    */
  override def getSourceKey: Column = lit(2112)


  /**
    * Entry point for Rental list venus file
    *
    * @param args unused
    */
  def main(args: Array[String]): Unit = {

    logger.info("Starting Customer Matching process for Rental List Venus Data")
    val hiveContext = initCMContext

    //Read source Rental List JCrew Data
    logger.info("Reading Rental List Venus data from Work layer")
    val rentalListVenusData = hiveContext.table(RentalListVenusSourceTable)
      .withColumnRenamed("firstname", FirstNameAlias)
      .withColumnRenamed("lastname", LastNameAlias)
      .withColumnRenamed("address1", AddressLineOneAlias)
      .withColumnRenamed("address2", AddressLineTwoAlias)
      .withColumnRenamed("zipcode", ZipCodeAlias)
      .withColumnRenamed("zip4", ZipPlusFourAlias)

    // Initialize required UDFs
    val nameFormatUDF = UDF(udf(NameFormat("", _: String, _: String, _: String, "")),
      Seq(FirstNameAlias, "middlename", LastNameAlias))

    val addressFormatUDF = UDF(udf(AddressFormat(_: String, _: String, _: String, "", _: String, _: String, "", ""
      , "")), Seq(AddressLineOneAlias, AddressLineTwoAlias, "city", ZipCodeAlias, ZipPlusFourAlias))

    // Create function list
    val customerMatchFunctionList: Seq[MatchTrait] = Seq(new NameAddressMatch(nameFormatUDF, addressFormatUDF))

    // Apply the match functions to employee data
    val (Some(matched), unmatched) = rentalListVenusData.applyCustomerMatch(customerMatchFunctionList)

    val rentalListVenusCMTable = hiveContext.table(RentalListVenusCMResultTable)
    // Create new customers for matched records
    val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(unmatched)

    // Assign record information key
    val updatedWithRecordInfo = assignRecordInfoKey( matched.unionAll(unmatchedWithNewMemberKeys.select(matched.getColumns: _*)))

    val finalRentalListVenusRecords = updatedWithRecordInfo
      .withColumnRenamed(FirstNameAlias, "firstname")
      .withColumnRenamed(LastNameAlias, "lastname")
      .withColumnRenamed(AddressLineOneAlias, "address1")
      .withColumnRenamed(AddressLineTwoAlias, "address2")
      .withColumnRenamed(ZipCodeAlias, "zipcode")
      .withColumnRenamed(ZipPlusFourAlias, "zip4")
      .withColumn(SourceKeyColumn, getSourceKey)
      .select(rentalListVenusCMTable.getColumns: _*)

    // Write the Customer match results to output file
    finalRentalListVenusRecords.insertIntoHive(SaveMode.Overwrite, rentalListVenusData, RentalListVenusCMResultTable)

  }
}