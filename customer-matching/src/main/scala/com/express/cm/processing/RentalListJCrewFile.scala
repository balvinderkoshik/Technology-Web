package com.express.cm.processing

import com.express.cdw.SourceKeyColumn
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.MatchColumnAliases.{AddressLineOneAlias, AddressLineTwoAlias, StateNameAlias}
import com.express.cm.criteria.RecordInfo.assignRecordInfoKey
import com.express.cm.criteria._
import com.express.cm.spark.DataFrameUtils._
import com.express.cm.{AddressFormat, NameFormat, UDF}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Column, SaveMode}

/**
  * Created by aman.jain on 4/25/2017.
  */
object RentalListJCrewFile extends CustomerMatchProcess with LazyLogging {

  val RentalListJCrewSourceTable = s"$workDB.work_rentallist_jcrew_dataquality"
  val RentalListJCrewCMResultTable = s"$workDB.work_rentallist_jcrew_cm"


  /**
    * Get the Source key for Input file
    *
    * @return [[Column]]
    */
  override def getSourceKey: Column = sourceKeyGenderFunc(264, 263)(col("key_code"))


  /**
    * Entry point for Rental list jcrew file
    *
    * @param args unused
    */
  def main(args: Array[String]): Unit = {

    logger.info("Starting Customer Matching process for Rental List JCrew Data")
    val hiveContext = initCMContext

    //Read source Rental List JCrew Data
    logger.info("Reading Rental List JCrew data from Work layer")
    val rentalListJCrewData = hiveContext.table(RentalListJCrewSourceTable)
      .withColumnRenamed("address_1", AddressLineOneAlias)
      .withColumnRenamed("address_2", AddressLineTwoAlias)
      .withColumnRenamed("state", StateNameAlias)
    // Initialize required UDFs
    val nameFormatUDF = UDF(udf(NameFormat(_: String, _: String, "", _: String, _: String)),
      Seq("prefix_code", "first_name", "last_name", "suffix_code"))

    val addressFormatUDF = UDF(udf(AddressFormat(_: String, _: String, _: String, _: String, _: String, "", "", ""
      , "")), Seq(AddressLineOneAlias, AddressLineTwoAlias, "city", StateNameAlias, "zip_code"))

    // Create function list
    val customerMatchFunctionList: Seq[MatchTrait] = Seq(new NameAddressMatch(nameFormatUDF, addressFormatUDF))

    // Apply the match functions to employee data
    val (Some(matched), unmatched) = rentalListJCrewData.applyCustomerMatch(customerMatchFunctionList)

    val rentalListJCrewCMTable = hiveContext.table(RentalListJCrewCMResultTable)
    // Create new customers for matched records
    val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(unmatched)

    // Assign record information key
    val updatedWithRecordInfo = assignRecordInfoKey( matched.unionAll(unmatchedWithNewMemberKeys.select(matched.getColumns: _*)))

    val finalRentalListJCrewRecords = updatedWithRecordInfo
      .withColumnRenamed(AddressLineOneAlias, "address_1")
      .withColumnRenamed(AddressLineTwoAlias, "address_2")
      .withColumnRenamed(StateNameAlias, "state")
      .withColumn(SourceKeyColumn, getSourceKey)
      .select(rentalListJCrewCMTable.getColumns: _*)

    // Write the Customer match results to output file
    finalRentalListJCrewRecords.insertIntoHive(SaveMode.Overwrite, rentalListJCrewData, RentalListJCrewCMResultTable)
  }


}