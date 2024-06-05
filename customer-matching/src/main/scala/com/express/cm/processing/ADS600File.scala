package com.express.cm.processing

import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.MatchColumnAliases._
import com.express.cm.criteria.RecordInfo.assignRecordInfoKey
import com.express.cm.criteria._
import com.express.cm.spark.DataFrameUtils._
import com.express.cm.{AddressFormat, NameFormat, UDF}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SaveMode}


/**
  * Created by amruta.chalakh on 5/2/2017.
  */
object ADS600File extends CustomerMatchProcess with LazyLogging {

  val ADS600SourceTable = s"$workDB.work_ads_600_pahp_dataquality"
  val ADS600AuthSourceTable = s"$workDB.work_ads_600_auth_user_dataquality"
  val ADS600CMResultTable = s"$workDB.work_ads_600_cm"

  // Source to CM alias mapping
  private val CMAliasMap = Map("account_number" -> CardAlias, "street_address" -> AddressLineOneAlias,
    "secondary_street_address" -> AddressLineTwoAlias
  )

  private val CMAuthAliasMap = Map("account_number" -> CardAlias,"authorized_user_street_address" -> AddressLineOneAlias,
    "authorized_user_first_name" -> FirstNameAlias,"authorized_user_middle_initial" -> MiddleInitialAlias,
    "authorized_user_last_name" -> LastNameAlias, "authorized_user_city" -> CityAlias,
    "authorized_user_state" -> StateNameAlias, "authorized_user_zip_code" -> ZipCodeAlias,
    "authorized_user_zip_plus_4" -> ZipPlusFourAlias, "authorized_user_home_phone" -> "home_phone",
    "account_number_auth" -> "account_number","division_number_auth"-> "division_number",
    "record_type_auth" -> "record_type","filler_auth"->"filler",
    "batch_id_auth" -> "batch_id","etl_unique_id_auth" -> "etl_unique_id"
  )

  private val sortTrimCols = Seq(CardAlias,FirstNameAlias,LastNameAlias,AddressLineOneAlias,PhoneAlias)
  /**
    * Get the Source key for Input file
    *
    * @return [[Column]]
    */
  override def getSourceKey: Column = lit(8)

  /**
    * Entry point for ADS 600 file
    *
    * @param args not used
    */
  def main(args: Array[String]): Unit = {

    val hiveContext = initCMContext
    logger.info("Reading ADS600 data from Work layer")

    // merge home_phone & mobile to phone alias column
    val ads600Data_pahp = hiveContext.table(ADS600SourceTable)
      .withColumn(PhoneAlias, array("home_phone", "mobile_phone"))
      .withColumn(PhoneAlias, explode(col(PhoneAlias)))
      .withColumn("filler",lit(null))
      .renameColumns(CMAliasMap)

    val ads600AuthDataset = hiveContext.table(ADS600AuthSourceTable)
      .withColumnRenamed("account_number","account_number_auth")
      .withColumnRenamed("division_number","division_number_auth")
      .withColumnRenamed("record_type","record_type_auth")
      .withColumnRenamed("filler","filler_auth")
      .withColumnRenamed("batch_id","batch_id_auth")
      .withColumnRenamed("etl_unique_id","etl_unique_id_auth")
      .drop("authorized_user_date_change")

    val memberInfoColumnsInpahp = ads600Data_pahp.columns

    /*  full outer join changes */

    val ads600JoinDataset = ads600Data_pahp.join(ads600AuthDataset,ads600Data_pahp.col(CardAlias)===ads600AuthDataset.col("account_number_auth"),"full")

    val ads600PahpInsert = ads600JoinDataset.filter(isnull(ads600JoinDataset.col("account_number_auth")) and not(isnull(ads600JoinDataset.col(CardAlias))))
      .select(memberInfoColumnsInpahp.head, memberInfoColumnsInpahp.tail: _*)

    val ads600AuthInsert = ads600JoinDataset.filter(isnull(ads600JoinDataset.col(CardAlias)) and not(isnull(ads600JoinDataset.col("account_number_auth"))))
      .select(ads600JoinDataset.getColumns(CMAuthAliasMap.values.toList):_*).renameColumns(CMAuthAliasMap).select(memberInfoColumnsInpahp.head, memberInfoColumnsInpahp.tail: _*)

    val ads600AuthPahpInsert = ads600JoinDataset.filter(not(isnull(ads600JoinDataset.col(CardAlias))) and not(isnull(ads600JoinDataset.col("account_number_auth")))).select(memberInfoColumnsInpahp.head, memberInfoColumnsInpahp.tail: _*)

    val ads600Dataset = ads600PahpInsert.unionAll(ads600AuthInsert).unionAll(ads600AuthPahpInsert).dropDuplicates()

    // Initialize required UDFs
    val nameFormatUDF = UDF(udf(NameFormat("", _: String, _: String, _: String, "")),
      Seq("first_name", "middle_initial", "last_name"))

    val addressFormatUDF = UDF(udf(AddressFormat(_: String, _: String, _: String, _: String, _: String, _: String, "", "", "")),
      Seq(AddressLineOneAlias, AddressLineTwoAlias, "city", "state", "zip_code", "zip_plus_4"))

    // Create function list
    val customerMatchFunctionList: Seq[MatchTrait] = Seq(new PhoneEmailImpliedLoyaltyMatch(nameFormatUDF),
      new PLCCNameImpliedLoyaltyMatch(nameFormatUDF), PLCCMatch,
      new PLCCNameMatch(nameFormatUDF), new NameAddressMatch(nameFormatUDF, addressFormatUDF),
      new PhoneEmailNameMatch(nameFormatUDF), new PhoneEmailMatch(nameFormatUDF), new EmailNameMatch(nameFormatUDF),
      new EmailMatch, new PhoneNameMatch(nameFormatUDF), new PhoneMatch)

    // Apply the match functions to ADS600 data
    val (Some(matched), unmatched) = ads600Dataset.applyCustomerMatch(customerMatchFunctionList)

    val ads600CMTable = hiveContext.table(ADS600CMResultTable)

    val rankWindowSpec = Window.partitionBy("etl_unique_id").orderBy(desc(MemberKeyColumn))

    // Rank over (latest) max member key and select it as best in case of tie
    val rankedDF = matched.unionAll(unmatched.select(matched.getColumns: _*))
      .withColumn("rank", rank().over(rankWindowSpec))
      .filter("rank = 1")
      .dropDuplicates(Seq("etl_unique_id"))
      .persist()

    // unmatched records -> create new customers (member update type 6 scd)
    val sortRankedDF = sortTrimDF(rankedDF.filter(s"$MatchStatusColumn = false"),sortTrimCols)

/*      rankedDF.filter(s"$MatchStatusColumn = false").sort(PhoneAlias,CardAlias,AddressLineOneAlias,FirstNameAlias,MiddleInitialAlias,LastNameAlias,CityAlias,StateNameAlias,ZipCodeAlias,ZipPlusFourAlias,AddressLineOneAlias,AddressLineTwoAlias)*/

    val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(sortRankedDF)
    //val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(rankedDF.filter(s"$MatchStatusColumn = false"))

    // Assign record information key
    val updatedWithRecordInfo = assignRecordInfoKey(rankedDF.filter(s"$MatchStatusColumn = true").select(matched.getColumns: _*)
      .unionAll(unmatchedWithNewMemberKeys.select(matched.getColumns: _*)))

    // matched customers + new customers -> work area
    val finalADS600Records = updatedWithRecordInfo
      .renameColumns(CMAliasMap, reverse = true)
      .withColumn(SourceKeyColumn, getSourceKey)
      .select(ads600CMTable.getColumns: _*)

    // Write the Customer match results to output file
    finalADS600Records.insertIntoHive(SaveMode.Overwrite, ads600Dataset, ADS600CMResultTable)
  }

}
