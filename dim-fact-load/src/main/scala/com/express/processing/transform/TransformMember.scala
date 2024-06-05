package com.express.processing.transform

import java.sql.Date

import com.express.cdw.CustomerMatchFileType._
import com.express.cdw.ThirdPartyType._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.express.cdw.{MemberKeyColumn, Settings, _}
import com.express.util.Settings.getMemberTransformColumnMapping
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{callUDF, _}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ArrayType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}


/**
  * Member Dimension Transformation Logic
  *
  * @author mbadgujar
  */
class TransformMember(sourceDF: DataFrame, cmFileType: String) extends LazyLogging {

  import TransformMember._
  import sourceDF.sqlContext.implicits._


  logger.info("Retrieving source to Member column mappings for CM file: {}", cmFileType)

  // Get transformations mappings & overlay process flag
  private val ((transformMapping, derivedMapping), overlayCandidate) = {
    val getMapping = getMemberTransformColumnMapping(_: String)
    cmFileType match {
      case PeopleSoft => (getMapping(PeopleSoft), true)
      case TlogCustomer => (getMapping(TlogCustomer), true)
      case ADS400 => (getMapping(ADS400), true)
      case ADS600 => (getMapping(ADS600), true)
      case RentalListVenus => (getMapping(RentalListVenus), false)
      case RentalListExperian => (getMapping(RentalListExperian), false)
      case RentalListJcrew => (getMapping(RentalListJcrew), false)
      case MobileConsent => (getMapping(MobileConsent), false)
      case RentalListAnthemFile=> (getMapping(RentalListAnthemFile), false)
      case MemberEmailUpdate => (getMapping(MemberEmailUpdate), true)
      case LoyaltyWare => (getMapping(LoyaltyWare), true)
      case ComprehensiveHygiene => (getMapping(ComprehensiveHygiene), false)
      case ExperianRea => (getMapping(ExperianRea), false)
      case AcxiomRea => (getMapping(AcxiomRea), false)
      case Rpa_acxiom => (getMapping(Rpa_acxiom), false)
      case Rpa_experian => (getMapping(Rpa_experian), false)
      case _ => throw new Exception("Invalid Customer Matching file name provided")
    }
  }

  //Source data for member transform
  private val sourceData = cmFileType match {
    case TlogCustomer =>
      val cardTypeDf = sourceDF.sqlContext.table(s"$GoldDB.dim_card_type").filter("status = 'current'")
        .select($"tender_type_code".as("tender_type"), $"$IsExpressPLCCColumn", $"$IsBankCardColumn")
      mergeMemberInfo(sourceDF.join(cardTypeDf, Seq("tender_type"), "left"), "transaction_date_iso")
    case _ =>
      mergeMemberInfo(sourceDF, "file_date")
  }

  // Member columns info to source mapping
  private val memberInfoColumnsInSource = transformMapping.values.toSeq

  // Address info variables
  private val otherAddressColumns = memberInfoColumnsInSource.filter(StaticAddressColumns.contains)


  /**
    * Merges Member information across different dates
    *
    * @param sourceDF       Source Dataframe
    * @param orderingColumn Date ordering columns
    * @return [[DataFrame]]
    */
  //noinspection ScalaDeprecation
  def mergeMemberInfo(sourceDF: DataFrame, orderingColumn: String): DataFrame = {
    logger.info("Merging member information across different dates if found, on column: {}", orderingColumn)
    val groupedColumn = "grouped_data"
    val mergedColumn = "merged"
    val withFileDateDF = orderingColumn match {
      case "file_date" => sourceDF.withColumn("file_date", udf(fileDateFunc _).apply(col(ETLUniqueIDColumn)))
      case _ => sourceDF
    }
    val columns = withFileDateDF.columns.filterNot(_.equals(MemberKeyColumn))
    val grouped = withFileDateDF.filter(s"$MemberKeyColumn != -1")
      .groupByAsList(Seq(MemberKeyColumn), columns.toSeq)
    val groupedSchema = grouped.schema.find(_.name == groupedColumn).get.dataType
      .asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    grouped.withColumn(mergedColumn, callUDF(mergeFunc(orderingColumn, _: Seq[Row]), groupedSchema, col(groupedColumn)))
      .select(MemberKeyColumn, mergedColumn)
      .unstruct(mergedColumn, groupedSchema)
  }

  /**
    * Create New Member Dimensions Records
    *
    * @return [[DataFrame]]
    */
  private def createNewMemberDimensionRecords: DataFrame = {
    logger.info("Initializing new member records")
    val customerIntroDateColumn = "customer_introduction_date"
    val customerAddDateColumn = "customer_add_date"
    val sqlContext = sourceData.sqlContext
    val memberColumns = sqlContext.table(s"$GoldDB.dim_member").filter("status = 'current'").columns
    // get eligible  Member records after filtering  the cm_work table based on match status =false and member_key <> -1
    logger.info("Filtering data for match_status = false and member_key <>-1 ")
    val newMemberInfoDF = sourceData.filter(s"$MatchStatusColumn = false and $MemberKeyColumn <> -1")

    //Select columns containing member information & rename to member column name
    val memberInfoDFRenamedWithAddress = newMemberInfoDF.renameColumns(transformMapping)
      .dropDuplicates(Seq(MemberKeyColumn))

    // Get the columns not in source
    val memberColumnsNotInSource = memberColumns.filterNot(memberInfoColumnsInSource.contains(_))

    //Set columns info not provided to null values
    val updatedWithNull = memberColumnsNotInSource.foldLeft(memberInfoDFRenamedWithAddress) {
      case (df, colName) => df.withColumn(colName, lit(null))
    }

    // Update customer intro and add dates, original record info, is_express_plcc flag and source key
    {
      cmFileType match {
        case TlogCustomer =>
          updatedWithNull.withColumn(customerIntroDateColumn, col("transaction_date_iso"))
        case _ =>
          updatedWithNull.withColumn(customerIntroDateColumn, current_date)
      }
    }
      .withColumn(customerAddDateColumn, current_date())
      .withColumn(OriginalRecordInfoKeyColumn, $"$RecordInfoKeyColumn")
      .withColumn(OriginalSourceKeyColumn, $"$CurrentSourceKeyColumn")
      .select(memberColumns.head, memberColumns.tail: _*)
  }

  /**
    * Update the Member Records according to data in source.
    * Overlay columns are updated based on 'Overlay' criteria.
    * Update columns are checked for not null values and are updated in Member records if not null
    *
    * @return [[DataFrame]] with updated member records
    *
    */
  private def updateRecords(): DataFrame = {

    logger.info("Updating existing member records")
    val updateColumns = {
      transformMapping.values.toSeq
        .filterNot(column => (OverlayColumns ++ StaticAddressColumns ++ Seq("phone_nbr", "email_address")).contains(column))
    }.distinct
    logger.info(s"Update (SCD) columns from {}: {}", cmFileType, updateColumns.mkString(","))

    // Get member dimension columns to updated along with unchanged columns for Overlayed Records
    val overlayColumns = cmFileType match {
      case TlogCustomer => OverlayColumns ++ otherAddressColumns :+ IsExpressPLCCColumn :+ IsBankCardColumn
      case _ => OverlayColumns ++ otherAddressColumns
    }

    val sourceColumns = if (overlayCandidate) updateColumns ++ overlayColumns else updateColumns

    val sqlContext = sourceDF.sqlContext
    // Get the member dimension current partition data
    val memberDF = sqlContext.table(s"$GoldDB.dim_member").filter("status = 'current'")
      .alias(targetAlias)

    val sourceInfoDF = sqlContext.table(s"$GoldDB.dim_source").filter("status = 'current'")
      .select("source_key", OverlayRankColumn)
      .persist()

    // Source transformation before update
    logger.info("Filtering data for match_status = true")
    val updateRecordsDF = sourceData.filter(s"$MatchStatusColumn = true")
    logger.info("Number of records to be checked for updates: {}", updateRecordsDF.count.toString)

    // Rename source columns to member columns
    val renamedDF = updateRecordsDF
      .renameColumns(transformMapping).dropDuplicates(Seq(MemberKeyColumn))
      .select(sourceColumns.toArray.map(col): _*)

    val aliasedDF = renamedDF.renameColumns(renamedDF.columns.map(column => column -> s"${sourceAlias}_$column").toMap)

    val joinedMemberData = (if (cmFileType == LoyaltyWare || cmFileType == ComprehensiveHygiene) aliasedDF else broadcast(aliasedDF))
      .join(memberDF, $"${sourceAlias}_$MemberKeyColumn" === $"$MemberKeyColumn")
      .join(broadcast(sourceInfoDF), $"$OverlayRankIdColumn" === $"source_key", "left")
      .withColumnRenamed(OverlayRankColumn, s"${targetAlias}_$OverlayRankColumn")
      .drop("source_key")

    // Update the member records
    val updatedRecords = updateColumns.foldLeft(joinedMemberData) {
      case (df, column) => df.withColumn(column,
        when(CheckNotEmptyUDF($"${sourceAlias}_$column"), $"${sourceAlias}_$column").otherwise($"$column"))
    }.persist()

    // If not to be overlayed, return the updated member data
    if (!overlayCandidate)
      return updatedRecords
        .filter(updateColumns.map(col => CheckNotEmptyUDF($"$col")).reduce(_ or _))
        .select(memberDF.getColumns: _*)
        .withColumn("action_flag", lit("U"))

    val joinedMemberDataWithSourceInfo = updatedRecords
      .join(broadcast(sourceInfoDF), $"${sourceAlias}_$CurrentSourceKeyColumn" === $"source_key", "left")
      .withColumnRenamed(OverlayRankColumn, s"${sourceAlias}_$OverlayRankColumn")
      .na.fill(-1, Seq(s"${targetAlias}_$OverlayRankColumn", s"${sourceAlias}_$OverlayRankColumn"))

    // Perform overlaying
    val (overlayed, sCDed) = overlayMemberDimensionRecords(joinedMemberDataWithSourceInfo)
    logger.debug("Overlayed record count: {}", overlayed.count.toString)
    logger.debug("SCDed record count: {}", sCDed.count.toString)

    sCDed
      .filter(updateColumns.map(col => CheckNotEmptyUDF($"$col")).reduce(_ or _))
      .select(memberDF.getColumns: _*)
      .unionAll(overlayed.select(memberDF.getColumns: _*))
      .withColumn("action_flag", lit("U"))

  }


  /**
    * Overlay/Update the existing Members records based on the Name & Address overlay Criteria
    *
    * @return [[DataFrame]] with records overlayed
    */
  private def overlayMemberDimensionRecords(sourceDF: DataFrame): (DataFrame, DataFrame) = {
    logger.info("Overlaying existing member records")
    // For tlog retail transactions, compare if is plcc or bank card for overlay
    val cardTrxnRankUDF = if (cmFileType == TlogCustomer)
      CardTrxnRankCompareUDF($"${sourceAlias}_$OverlayRankColumn", $"${sourceAlias}_$IsExpressPLCCColumn",
        $"${sourceAlias}_$IsBankCardColumn", $"$targetAlias.$IsExpressPLCCColumn")
    else
      lit(true)

    val (overlayed, sCDed) = sourceDF
      .partition {
        /* Filter out records which are candidate for overlay */
        OverlayColumns.map(column => s"${sourceAlias}_$column").map(column => CheckNotEmptyUDF($"$column")).reduce(_ and _) and
          /* If target is empty, overlay irrespective of the overlay rank else consider source overlay rank*/
          (OverlayColumns.map(column => CheckEmptyUDF($"$column")).reduce(_ and _) or
            SourceOverlayRankCompareUDF($"${sourceAlias}_$OverlayRankColumn", $"${targetAlias}_$OverlayRankColumn") and cardTrxnRankUDF)
      }
    val OverlayColumnsWithoutZipcode = Seq("first_name", "last_name", Address1Column)

    val overlayRecordsUpdated = (OverlayColumns ++ otherAddressColumns)
      .foldLeft(overlayed.withColumn("overlay_date_flag",when(OverlayColumnsWithoutZipcode.map(column => trim(upper($"${sourceAlias}_$column")) === trim(upper($"$column")))
        .reduce(_ and _) && substring(col(ZipCodeColumn),0,5) === substring($"${sourceAlias}_$ZipCodeColumn",0,5) , true) .otherwise(false))) {
        case (df, column) => df.withColumn(column, $"${sourceAlias}_$column")
      }.withColumn(OverlayRankIdColumn, $"${sourceAlias}_$CurrentSourceKeyColumn".cast(LongType))
      .withColumn(OverlayLoadIdColumn, $"${sourceAlias}_$BatchIDColumn".cast(LongType))
      .withColumn(OverlayDateColumn, when(col("overlay_date_flag") === true ,$"$OverlayDateColumn").otherwise(lit(current_date)))
    (overlayRecordsUpdated, sCDed)
  }

  /**
    * Updates Zip4 and Zipfull columns based on original Zip code provided
    *
    * @param dfToUpdate [[DataFrame]] to update
    * @return [[DataFrame]]
    */
  private def updateZipCodeInfo(dfToUpdate: DataFrame): DataFrame = {
    // Creates new struct column for zip4 and zip_full info
    val zipCodeUpdateUDF = udf((zipCode: String) => {
      Option(zipCode) match {
        case None => zip_info(null, null)
        case Some(_) =>
          val zip4 = if (zipCode.length > 5) zipCode.substring(5) else null
          val zipFull = if (zipCode.length > 5) s"${
            zipCode.substring(0, 5)
          }-$zip4"
          else zipCode
          zip_info(zip4, zipFull)
      }
    })

    if (memberInfoColumnsInSource.contains(ZipCodeColumn)) {
      dfToUpdate
        .withColumn("zip_info", zipCodeUpdateUDF(col(ZipCodeColumn)))
        .withColumn(Zip4Column, when(CheckNotEmptyUDF(col(Zip4Column)), col(Zip4Column)).otherwise($"zip_info.zip4"))
        .withColumn(ZipFullColumn, $"zip_info.zip_full")
        .withColumn(ZipCodeColumn, substring(col(ZipCodeColumn), 0, 5))
        .drop("zip_info")
    } else
      dfToUpdate
  }


  /**
    * Get Insert Update Transformed records for member table
    *
    * @return [[DataFrame]] with new member records & member records to be udpated
    */
  def getInsertUpdateTransformedRecords: DataFrame = {
    sourceData.persist()
    logger.info("Number on input records to be transformed: {}", sourceData.count.toString)
    val createRecordsDF = createNewMemberDimensionRecords
      .withColumn("action_flag", lit("I"))
      .withColumn("process", lit(cmFileType))
    createRecordsDF.persist
    logger.info("Number of transformed insert Member records: {}", createRecordsDF.count.toString)
    val updateRecordsDF = updateRecords()
      .withColumn("process", lit(cmFileType))
      .persist
    logger.info("Number of transformed update & overlayed records: {}", updateRecordsDF.count.toString)
    updateZipCodeInfo(createRecordsDF.unionAll(updateRecordsDF).applyExpressions(derivedMapping))
  }
}

object TransformMember {

  // Constants
  private val Address1Column = "address1"
  private val Address2Column = "address2"
  private val OriginalSourceKeyColumn = "original_source_key"
  private val CurrentSourceKeyColumn = "current_source_key"
  private val OriginalRecordInfoKeyColumn = "original_record_info_key"
  private val OverlayRankIdColumn = "overlay_rank_key"
  private val OverlayLoadIdColumn = "overlay_load_id"
  private val OverlayDateColumn = "overlay_date"
  private val IsExpressPLCCColumn = "is_express_plcc"
  private val IsBankCardColumn = "is_credit_card"
  private val OverlayRankColumn = "source_overlay_rank"
  private val ZipCodeColumn = "zip_code"
  private val Zip4Column = "zip4"
  private val ZipFullColumn = "zip_full"
  private val State = "state"
  private val City = "city"
  private val sourceAlias = "source_member_alias"
  private val targetAlias = "target_member_alias"

  // Columns used for overlay process
  private val OverlayColumns = Seq("first_name", "last_name", Address1Column, ZipCodeColumn)

  private val StaticAddressColumns = Seq(Address2Column, State, City, Zip4Column)

  /*
   *  UDFs
   */
  // Source Rank compare UDF
  private val SourceOverlayRankCompareUDF = udf((sourceOverlayRank: Int, targetOverlayRank: Int) =>
    if (targetOverlayRank <= -1) true else sourceOverlayRank <= targetOverlayRank)

  // Card Transactions Rank compare UDF (for TLog file)
  private val CardTrxnRankCompareUDF = udf((overlayRank: Int, sourceIsPlCC: String, sourceIsBankCard: String, targetIsPlcc: String) => {
    val yesFlag = "yes"
    (overlayRank == 4 && sourceIsPlCC != null && sourceIsPlCC.toLowerCase == yesFlag && targetIsPlcc != null &&
      sourceIsPlCC.toLowerCase == targetIsPlcc.toLowerCase) ||
      (overlayRank == 4 && sourceIsBankCard != null && sourceIsBankCard.toLowerCase == yesFlag && targetIsPlcc != null &&
        targetIsPlcc.toLowerCase != yesFlag) ||
      (overlayRank == 3)
  })

  // DB settings
  private val GoldDB = Settings.getGoldDB
  private val WorkDB = Settings.getWorkDB

  // Zip information case class
  case class zip_info(zip4: String, zip_full: String)

  // Ordering for sql.Date
  private implicit val dateOrdering: Ordering[Date] = new Ordering[Date] {
    def compare(x: Date, y: Date): Int = x compareTo y
  }.reverse


  /**
    * Merge Function for merging records with information across different dates
    * If value is present for latest date, it is considered or not null value for next date is considered
    *
    * @param orderingColumn Date Column for orderingmm
    * @param rows           Rows to be merged
    * @return Merged Row
    */
  private def mergeFunc(orderingColumn: String, rows: Seq[Row]): Row = {
    val sorted = rows.map(row => (row.getAs[Date](orderingColumn), row)).sortBy(_._1)
    val merged = sorted.map(_._2.toSeq).foldLeft(Seq[Any]()) { case (row1, row2) =>
      row1 match {
        case Nil => row2
        case _ => (row1 zip row2).map { case (value1, value2) => if (value1 == null || value1.toString.isEmpty) value2 else value1 }
      }
    }
    Row.fromSeq(merged)
  }

  /**
    * Transform Member entry point
    *
    * @param args {1. customer matching file type, 2. source table}
    */
  def main(args: Array[String]): Unit = {

    if (args.length < 1)
      throw new Exception("Please provide Customer matching file type as argument")

    val cmFileType = args(0)

    val sparkConf = Settings.sparkConf
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sparkContext)

    // Set dynamic partitioning to true
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")


    val tempDimMemberTable = s"$WorkDB.dim_member"
    val tempDimMemberDF = sqlContext.table(s"$WorkDB.dim_member")


    val transformMemberObj = new TransformMember(sqlContext.table(s"$WorkDB.work_${cmFileType}_cm"), cmFileType)
    // Get insert and update member records
    val transformedMemberData = transformMemberObj.getInsertUpdateTransformedRecords
      .select(tempDimMemberDF.getColumns: _*)


    // Write output to temp dim_member table
    transformedMemberData.withColumn("last_updated_date", current_timestamp)
      .insertIntoHive(SaveMode.Overwrite, tempDimMemberTable, Some("process"), null)
  }
}
