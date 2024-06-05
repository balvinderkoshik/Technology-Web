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
  * Created by chennuri.gourish on 29/8/2018.
  */
object RentalListAnthemFile extends CustomerMatchProcess with LazyLogging {

  val RentalListAnthemSourceTable = s"$workDB.work_rentallist_anthem_dataquality"
  val RentalListAnthemCMResultTable = s"$workDB.work_rentallist_anthem_cm"



  private val CMAliasMap = Map("prefix" -> NamePrefixAlias, "firstname" -> FirstNameAlias, "lastname" -> LastNameAlias, "suffix" -> NameSuffixAlias, "address1" -> AddressLineOneAlias, "address2" -> AddressLineTwoAlias, "city" -> CityAlias, "state" -> StateNameAlias, "zip" -> ZipCodeAlias, "zip4" -> ZipPlusFourAlias)
  /**
    * Get the Source key for Input file
    *
    * @return [[Column]]
    */
  override def getSourceKey: Column = lit(2199)


  /**
    * Entry point for Rental list anthem file
    *
    * @param args unused
    */
  def main(args: Array[String]): Unit = {


    logger.info("Starting Customer Matching process for Rental List Anthem Data")
    println("Main started")
    val hiveContext = initCMContext


    //Read source Rental List Anthem Data
    logger.info("Reading Rental List Anthem data from Work layer")
    val rentalListAnthemData = hiveContext.table(RentalListAnthemSourceTable)
      .renameColumns(CMAliasMap)

    // Initialize required UDFs
    val nameFormatUDF = UDF(udf(NameFormat( _:String, _: String, "", _: String, _:String)),
      Seq(NamePrefixAlias,FirstNameAlias, LastNameAlias,NameSuffixAlias))

    val addressFormatUDF = UDF(udf(AddressFormat(_: String, _: String, _: String, _:String, _: String, _: String, "", ""
      , "")), Seq(AddressLineOneAlias, AddressLineTwoAlias, CityAlias, StateNameAlias, ZipCodeAlias, ZipPlusFourAlias))

    println(" Create function list")
    // Create function list
    val customerMatchFunctionList: Seq[MatchTrait] = Seq(new NameAddressMatch(nameFormatUDF, addressFormatUDF))

    // Apply the match functions to employee data
    val (Some(matched), unmatched) = rentalListAnthemData.applyCustomerMatch(customerMatchFunctionList)

    val rentalListAnthemCMTable = hiveContext.table(RentalListAnthemCMResultTable)
    // Create new customers for matched records
    val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(unmatched)

    // Assign record information key
    val updatedWithRecordInfo = assignRecordInfoKey( unmatchedWithNewMemberKeys.select(matched.getColumns: _*))
    println("matched count------------------------------------------------->")
    matched.count()
    println("unmatched count --------------------------->")
    unmatched.count()
    val finalRentalListAnthemRecords = updatedWithRecordInfo
      .renameColumns(CMAliasMap, reverse = true)
      .withColumn(SourceKeyColumn, getSourceKey)
      .select(rentalListAnthemCMTable.getColumns: _*)

    // Write the Customer match results to output file
    finalRentalListAnthemRecords.insertIntoHive(SaveMode.Overwrite, rentalListAnthemData, RentalListAnthemCMResultTable)

  }
}