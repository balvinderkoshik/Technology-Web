package com.express.cm.processing

import com.express.cdw.MatchColumnAliases._
import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs.CheckNotEmptyUDF
import com.express.cm.criteria.RecordInfo.assignRecordInfoKey
import com.express.cm.criteria._
import com.express.cm.spark.DataFrameUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, SaveMode}

/**
  * Processing logic for Loyaltyware file
  *
  * Created by bhautik.patel on 10/07/17.
  */
object LoyaltyWareFile extends CustomerMatchProcess with LazyLogging {

  val LoyaltywareSourceTable = s"$workDB.work_bp_member_dataquality"
  val LoyaltywareCMResultTable = s"$workDB.work_bp_member_cm"
  val VirtualCardSourceTable = s"$workDB.work_bp_virtualcard_dataquality"
  val LoyaltyIdNumberCol = "loyalty_id_number"
  val LoyaltyIdCol = "loyalty_id"
  val IpCodeCol = "ip_code"
  val LoyaltyMemberKeyCol = "a_cdhmemberkey"
  val LoyaltyRejectRecordsLocation = "/apps/cdw/outgoing/exception_list/bpmemberExceptionList"

  // Source to CM alias mapping
  private val CMAliasMap = Map("primaryemailaddress" -> EmailAddressAlias,
    "primaryphonenumber" -> PhoneAlias, "firstname" -> FirstNameAlias, "lastname" -> LastNameAlias,
    "a_addresslineone" -> AddressLineOneAlias, "a_zipcode" -> ZipCodeAlias, "ipcode" -> IpCodeCol,
    LoyaltyIdNumberCol -> ImpliedLoyaltyIdAlias
  )

  private val NotNull = CheckNotEmptyUDF

  private val sortTrimCols = Seq(IpCodeCol,ImpliedLoyaltyIdAlias)

  /**
    * Get the Source key for Input file
    *
    * @return [[Column]]
    */
  override def getSourceKey: Column = lit(300)


  /**
    * Entry point for LoyaltyWareFile
    *
    * @param args not used
    */
  def main(args: Array[String]): Unit = {
    logger.info("Starting Loyaltyware file processing for creating new Customers")
    val hiveContext = initCMContext
    import hiveContext.implicits._
    logger.info("Reading Loyaltyware data from Work layer")
    val loyaltywareCMResultDF = hiveContext.table(LoyaltywareCMResultTable)
    val vcardData = hiveContext.table(VirtualCardSourceTable).selectExpr("member_id as ipcode", LoyaltyIdNumberCol).distinct
    val bpMemberData = hiveContext.table(LoyaltywareSourceTable)
    // Read source Loyaltyware data
    val loyaltywareData = bpMemberData
      .withColumn("rnk", row_number() /* remove duplicates on cdh_memberkey if found */
        .over(Window.partitionBy(LoyaltyMemberKeyCol, "ipcode").orderBy(desc("a_enrolldate"), desc("membercreatedate"))))
      .filter(s"rnk = 1").drop("rnk")
      .join(vcardData, Seq("ipcode"), "left")
      .renameColumns(CMAliasMap)
      .addCMMetaColumns()
      .filter("memberstatus != 5") /* filter out non-loyalty members from the input */


    val (matchedOnLoyalty, unmatchedOnLoyalty) = LoyaltyIDMatch.matchFunction(loyaltywareData).persist
      .partition($"$MatchStatusColumn" === true)
    val (rejectedMatchedOnLoyalty, validMatchedOnLoyalty) = matchedOnLoyalty
      .partition(NotNull($"$LoyaltyMemberKeyCol") and ($"$LoyaltyMemberKeyCol" !== $"$MemberKeyColumn"))
    val (rejectedUnmatchedOnLoyalty, validUnmatchedOnLoyalty) = unmatchedOnLoyalty.
      partition(NotNull($"$LoyaltyMemberKeyCol") and NotNull($"$ImpliedLoyaltyIdAlias"))
    val (createNewCustFromLoyalty, ipCodeMatchInput) = validUnmatchedOnLoyalty
      .partition(NotNull($"$ImpliedLoyaltyIdAlias"))


    logger.debug("Loyaltyware Match Input Count: {}", loyaltywareData.count.toString)
    logger.debug("Matched On Loyalty Id Count: {}", matchedOnLoyalty.count.toString)
    logger.debug("Unmatched On Loyalty Id Count: {}", unmatchedOnLoyalty.count.toString)
    logger.debug("Rejected from Loyalty match with Member key mismatch: {}", rejectedMatchedOnLoyalty.count.toString)
    logger.debug("Rejected from Loyalty match with unmatched member_key: {}", rejectedUnmatchedOnLoyalty.count.toString)
    logger.debug("New Customer from Loyalty Count: {}", createNewCustFromLoyalty.count.toString)

    val (matchedOnIpcode, unmatchedOnIpcode) = IPCodeMatch.matchFunction(ipCodeMatchInput).persist
      .partition($"$MatchStatusColumn" === true)
    val (rejectedMatchedOnIpcode, validMatchedOnIpcode) = matchedOnIpcode
      .partition(NotNull($"$LoyaltyMemberKeyCol") and ($"$LoyaltyMemberKeyCol" !== $"$MemberKeyColumn"))

    val matched = validMatchedOnLoyalty.unionAll(validMatchedOnIpcode)

      val sortCreateNewCustFromLoyalty = sortTrimDF(createNewCustFromLoyalty,sortTrimCols)

        //createNewCustFromLoyalty.sort(EmailAddressAlias,PhoneAlias,FirstNameAlias,LastNameAlias,AddressLineOneAlias,ZipCodeAlias,IpCodeCol,ImpliedLoyaltyIdAlias)

    val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(sortCreateNewCustFromLoyalty)
    //val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(createNewCustFromLoyalty)
    val rejectedLoyaltyRecords = Seq(rejectedMatchedOnLoyalty, rejectedUnmatchedOnLoyalty, rejectedMatchedOnIpcode,
      unmatchedOnIpcode).reduce(_ unionAll _)
    val rejectedRecordsCount = rejectedLoyaltyRecords.count

    logger.debug("IPcode Match Input Count: {}", ipCodeMatchInput.count.toString)
    logger.debug("Matched On Ipcode Count: {}", matchedOnIpcode.count.toString)
    logger.debug("Rejected from Ipcode match with Member key mismatch: {}", rejectedMatchedOnIpcode.count.toString)
    logger.debug("Rejected from Ipcode (unmatched): {}", unmatchedOnIpcode.count.toString)

    logger.debug("Total Matched Count: {}", matched.count.toString)
    logger.debug("Total New Customer Count: {}", unmatchedWithNewMemberKeys.count.toString)
    logger.debug("Total Rejected Records Count: {}", rejectedRecordsCount.toString)

    if (rejectedRecordsCount > 0) {
      logger.warn("Writing {} rejected records to : {}", rejectedRecordsCount.toString, LoyaltyRejectRecordsLocation)
      rejectedLoyaltyRecords
        .renameColumns(CMAliasMap, reverse = true)
        .castColumns((bpMemberData.columns :+ LoyaltyIdNumberCol).toSeq, StringType)
        .na.fill("")
        .withColumn("rejected_data", concat_ws("|", bpMemberData.getColumns :+ $"$LoyaltyIdNumberCol" :_*))
        .select("rejected_data")
        .write.mode(SaveMode.Overwrite)
        .text(LoyaltyRejectRecordsLocation + "/ExceptionList-" + java.time.LocalDate.now)
    }

    val dimStore = hiveContext.table("gold.dim_store_master").filter("status='current'")
      .select("store_id","store_key").persist

    val finalLoyaltywareRecords = matched
      .unionAll(unmatchedWithNewMemberKeys.select(matched.getColumns: _*))
      .transform(assignRecordInfoKey)
      .renameColumns(CMAliasMap, reverse = true)
      .withColumn(SourceKeyColumn, getSourceKey)
      .join(broadcast(dimStore) ,col("a_enrollmentstorenumber") === col("store_id"),"left")
      .drop("a_enrollmentstorenumber")
      .withColumn("a_enrollmentstorenumber", when(col("store_key").isNull, lit(-1)).otherwise(col("store_key")))
        .withColumn("membercreatedatetime",col("membercreatedate"))
      .select(loyaltywareCMResultDF.getColumns: _*)

    logger.debug("finalLoyaltywareRecords count: {}", finalLoyaltywareRecords.count.toString)
    finalLoyaltywareRecords.insertIntoHive(SaveMode.Overwrite, loyaltywareData, LoyaltywareCMResultTable)
    logger.info("Loyaltyware file processing complete")
  }

}