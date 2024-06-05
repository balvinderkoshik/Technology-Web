package com.express.cm.processing

import com.express.cdw.MatchColumnAliases._
import com.express.cdw.SourceKeyColumn
import com.express.cdw.spark.DataFrameUtils._
import com.express.cm.criteria.RecordInfo.assignRecordInfoKey
import com.express.cm.criteria.{CreateNewCustomerRecords, MatchTrait, NameAddressMatch}
import com.express.cm.spark.DataFrameUtils._
import com.express.cm.{AddressFormat, NameFormat, UDF}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Column, SaveMode}

/**
  * Created by aman.jain on 5/2/2017.
  */
object RentalListExperianFile extends CustomerMatchProcess with LazyLogging {

  private val RentalListExperianSourceTable = s"$workDB.work_rentallist_experian_dataquality"
  private val RentalListExperianCMResultTable = s"$workDB.work_rentallist_experian_cm"

  /**
    * Get the Source key for Input file
    *
    * @return [[Column]]
    */
  override def getSourceKey: Column = sourceKeyGenderFunc(1561, 1560)(col("gender"))


  /**
    * Entry point for Rental List Experian File
    *
    * @param args unused
    */
  def main(args: Array[String]): Unit = {

    logger.info("Starting Customer Matching process for Rental List Experian Data")
    val hiveContext = initCMContext

    //Read source Rental List JCrew Data
    logger.info("Reading Rental List Venus data from Work layer")
    val rentalListExperianData = hiveContext.table(RentalListExperianSourceTable)
      .withColumnRenamed("address1", AddressLineOneAlias)
      .withColumnRenamed("address2", AddressLineTwoAlias)
      .withColumnRenamed("state", StateNameAlias)
      .withColumnRenamed("zip", ZipCodeAlias)
      .withColumnRenamed("zip4", ZipPlusFourAlias)

    // Initialize required UDFs
    val nameFormatUDF = UDF(udf(NameFormat(_: String, _: String, _: String, _: String, _: String)),
      Seq("prefix", "first_name", "middle_name", "last_name", "suffix"))

    val addressFormatUDF = UDF(udf(AddressFormat(_: String, _: String, _: String, _: String, _: String, _: String, "", ""
      , "")), Seq(AddressLineOneAlias, AddressLineTwoAlias, "city", StateNameAlias, ZipCodeAlias, ZipPlusFourAlias))

    // Create function list
    val customerMatchFunctionList: Seq[MatchTrait] = Seq(new NameAddressMatch(nameFormatUDF, addressFormatUDF))

    // Apply the match functions to employee data
    val (Some(matched), unmatched) = rentalListExperianData.applyCustomerMatch(customerMatchFunctionList)

    val rentalListExperianCMTable = hiveContext.table(RentalListExperianCMResultTable)
    // Create new customers for matched records
    val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(unmatched)

    // Assign record information key
    val updatedWithRecordInfo = assignRecordInfoKey(matched.unionAll(unmatchedWithNewMemberKeys.select(matched.getColumns: _*)))

    val finalRentalListExperianRecords = updatedWithRecordInfo
      .withColumnRenamed(AddressLineOneAlias, "address1")
      .withColumnRenamed(AddressLineTwoAlias, "address2")
      .withColumnRenamed(StateNameAlias, "state")
      .withColumnRenamed(ZipCodeAlias, "zip")
      .withColumnRenamed(ZipPlusFourAlias, "zip4")
      .withColumn(SourceKeyColumn, getSourceKey)
      .select(rentalListExperianCMTable.getColumns: _*)

    // Write the Customer match results to output file
    finalRentalListExperianRecords.insertIntoHive(SaveMode.Overwrite, rentalListExperianData, RentalListExperianCMResultTable)

  }
}