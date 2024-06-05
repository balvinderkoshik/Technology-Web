package com.express.cm.processing

import com.express.cdw.SourceKeyColumn
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.MatchColumnAliases._
import com.express.cm.criteria.RecordInfo.assignRecordInfoKey
import com.express.cm.criteria._
import com.express.cm.spark.DataFrameUtils._
import com.express.cm.{AddressFormat, NameFormat, UDF}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, udf}
import org.apache.spark.sql.{Column, SaveMode}
import org.apache.spark.sql.functions._

/**
  * Created by amruta.chalakh on 5/2/2017.
  */
object ADS400File extends CustomerMatchProcess with LazyLogging {

  val ADS400SourceTable = s"$workDB.work_ads_400_dataquality"
  val ADS400CMResultTable = s"$workDB.work_ads_400_cm"

  // Source to CM alias mapping
  private val CMAliasMap = Map("home_phone_number" -> PhoneAlias, "account_number" -> CardAlias,
    "street_address" -> AddressLineOneAlias, "secondary_street_address" -> AddressLineTwoAlias)

  private val sortTrimCols = Seq(CardAlias,FirstNameAlias,LastNameAlias,AddressLineOneAlias,PhoneAlias)
  /**
    * Get the Source key for Input file
    *
    * @return [[Column]]
    */
  override def getSourceKey: Column = lit(9)


  /**
    * Entry point for ADS 400 File
    *
    * @param args not used
    */
  def main(args: Array[String]): Unit = {

    logger.info("Starting Customer Matching process for ADS400 Data")
    val hiveContext = initCMContext

    //Create dataframes using hiveContext
    logger.info("Reading ADS400 data from Work layer")
    val Ads400data = hiveContext.table(ADS400SourceTable)
      .renameColumns(CMAliasMap)
      .withColumn("closed_date",when(col("closed_date")=== "" or col("closed_date")=== "9999-01-01", lit(null)).otherwise(col("closed_date"))) 
      .withColumn("rank",row_number() over Window.partitionBy(CardAlias).orderBy(asc("indicator"),desc("closed_date")))
      .filter("rank=1")
      .drop(col("rank"))

    // Initialize required UDFs
    val nameFormatUDF = UDF(udf(NameFormat("", _: String, _: String, _: String, "")),
      Seq("first_name", "middle_initial", "last_name"))

    val addressFormatUDF = UDF(udf(AddressFormat(_: String, _: String, _: String, _: String, _: String, _: String, "", "", "")),
      Seq(AddressLineOneAlias, AddressLineTwoAlias, "city", "state", "zip_code", "zip_plus_4"))

    //Create function list
    val customerMatchFunctionList: Seq[MatchTrait] = Seq(new PhoneEmailImpliedLoyaltyMatch(nameFormatUDF),
      new PLCCNameImpliedLoyaltyMatch(nameFormatUDF), PLCCMatch,
      new PLCCNameMatch(nameFormatUDF), new NameAddressMatch(nameFormatUDF, addressFormatUDF),
      new PhoneEmailNameMatch(nameFormatUDF), new PhoneEmailMatch(nameFormatUDF), new EmailNameMatch(nameFormatUDF),
      new EmailMatch, new PhoneNameMatch(nameFormatUDF), new PhoneMatch)

    // Apply the match functions to ADS400 data
    val (Some(matched), unmatched) = Ads400data.applyCustomerMatch(customerMatchFunctionList)

    val ads400CMTable = hiveContext.table(ADS400CMResultTable)

    // unmatched records -> create new customers (member update type 6 scd)
    val sortUnmatched = sortTrimDF(unmatched,sortTrimCols)
      //unmatched.sort(PhoneAlias,CardAlias,AddressLineOneAlias,AddressLineTwoAlias)
    val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(sortUnmatched)
    //val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(unmatched)

    // Assign record information key
    val updatedWithRecordInfo = assignRecordInfoKey(matched.unionAll(unmatchedWithNewMemberKeys.select(matched.getColumns: _*)))

    // matched customers + new customers -> work area
    val finalADS400Records = updatedWithRecordInfo
      .renameColumns(CMAliasMap, reverse = true)
      .withColumn(SourceKeyColumn, getSourceKey)
      .select(ads400CMTable.getColumns: _*)

    // Write the Customer match results to output file
    finalADS400Records.insertIntoHive(SaveMode.Overwrite, Ads400data, ADS400CMResultTable)
  }

}
