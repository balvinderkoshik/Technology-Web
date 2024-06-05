package com.express.dedup

import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.express.cdw.{CDWContext, isNotEmpty, _}
import com.express.dedup.utils.DedupUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Contains Deduplication processing flow
  *
  * Created by aman.jain on 6/21/2017.
  */
object DeDuplicationProcess extends CDWContext with LazyLogging {

  // Tables used for Dedup process
  val DimSourceTable = s"$goldDB.dim_source"
  val DimMemberSourceTable = s"$goldDB.dim_member"
  val DimCardTypeTable = s"$goldDB.dim_card_type"
  val DimTimeTable = s"$goldDB.dim_time"
  val FactCardHistorySourceTable = s"$goldDB.fact_card_history"
  val DimMemberMultiEmailSourceTable = s"$goldDB.dim_member_multi_email"
  val DimMemberMultiPhoneSourceTable = s"$goldDB.dim_member_multi_phone"
  val FactTransactionDetailSourceTable = s"$goldDB.fact_transaction_detail"
  val DimMemberDedupResultsTable = s"$workDB.dim_member"
  val DimDedupInfoLookUpTable = s"$goldDB.dim_dedupe_info"
  val FactDedupTargetTable = s"$goldDB.fact_dedupe_member_history"

  /**
    * Created Dedup fact records from the Resolution details
    *
    * @param resolvedRecords [[DataFrame]] with resolution applied
    * @return [[DataFrame]] with fact records
    */
  def getDedupFactRecords(resolvedRecords: DataFrame): DataFrame = {

    val dimDedupTable = hiveContext.table(DimDedupInfoLookUpTable).filter("status='current'")
    val factDedupMemberHistTable = hiveContext.table(FactDedupTargetTable)

    val factDedupDF = resolvedRecords
      .select(MemberKeyColumn, s"$ResolvedColumn.$ResolvedMemberIDColumn", GroupIDColumn, GroupingCriteriaIDColumn,
        s"$ResolvedColumn.$CollectionIDColumn", isLoyalResolved, InvalidGroupIDStatusColumn)
      .filter(col(isLoyalResolved) === false)
      .distinct()

    val factDedupDedupInfoKey = factDedupDF.join(dimDedupTable,
      factDedupDF(GroupingCriteriaIDColumn) === dimDedupTable("id_condition") &&
        factDedupDF(CollectionIDColumn) === dimDedupTable("collection_type"), "left")
      .selectExpr(s"$MemberKeyColumn as old_member_key", s"$ResolvedMemberIDColumn as new_member_key",
        GroupIDColumn, "dedupe_info_key", s"$InvalidGroupIDStatusColumn")
      .filter("old_member_key != new_member_key")

    val maxDedupMemberHistId = factDedupMemberHistTable.maxKeyValue(col("dedupe_member_hist_id"))

    // Filtering Out Invalid groups
    val groupingIDList = factDedupDedupInfoKey
      .withColumn("old_mem_cnt", count("old_member_key").over(Window.partitionBy("old_member_key")))
      .filter("old_mem_cnt > 1")
      .select(GroupIDColumn)
      .renameColumns(Map(GroupIDColumn -> "grouping_id_list"))
      .drop("old_mem_cnt")
      .distinct()


    val factDedupRef = factDedupDedupInfoKey
      .join(groupingIDList, factDedupDedupInfoKey(GroupIDColumn) === groupingIDList("grouping_id_list"), "left")
      .withColumn(InvalidGroupIDStatusColumn, when(col("grouping_id_list").isNotNull, lit(true)).otherwise(col(InvalidGroupIDStatusColumn)))
      .drop(col("grouping_id_list"))
      .withColumn(LastUpdatedDateColumn, current_timestamp())

    factDedupRef.generateSequence(maxDedupMemberHistId, Some("dedupe_member_hist_id")).persist
  }


  /**
    * Deduplication entry point
    *
    * @param args Input arguments (batch id)
    */
  def main(args: Array[String]): Unit = {

    val batch_id = args(0)
    // Register UDFs
    hiveContext.udf.register[Boolean, String]("not_empty", isNotEmpty)
    hiveContext.udf.register[Boolean, String]("empty", !isNotEmpty(_: String))

    /** Dataframes used for Dedup data   **/

    val regExBankCard = "^0+(?!$)"

    val dimSource = hiveContext.table(DimSourceTable)
      .filter("status='current'")
      .select("source_key", "source_overlay_rank")
      .persist

    val dimMember = hiveContext.table(DimMemberSourceTable)
      .filter("status='current'")
      .join(broadcast(dimSource), col("overlay_rank_key") === col("source_key"), "left")
      .drop("source_key")


    val dimCardType = hiveContext.table(DimCardTypeTable)
      .filter("status='current' and (is_express_plcc='YES' or is_credit_card='YES')")
      .drop(LastUpdatedDateColumn)
      .persist()

    val factCardHistory = hiveContext.table(FactCardHistorySourceTable)
      .filter("member_key<>-1 and tokenized_cc_nbr is not null")
      .withColumn("tokenized_cc_nbr", regexp_replace(col("tokenized_cc_nbr"), regExBankCard, ""))
      .filter("tokenized_cc_nbr != '0'")
      .join(broadcast(dimCardType), Seq("card_type_key"), "left")
      .select("member_key", "tokenized_cc_nbr", "open_close_ind", "last_updated_date")
      .withColumnRenamed("tokenized_cc_nbr", "bank_card")
      .withColumn("rank", row_number() over Window.partitionBy("member_key", "bank_card").orderBy(desc("last_updated_date")))
      .filter("rank=1")
      .groupByAsList(Seq("member_key"), Seq("bank_card", "open_close_ind"))

    val dimMemberMultiEmail = hiveContext.table(DimMemberMultiEmailSourceTable)
      .filter("status='current' and member_key != 1134577523 and member_key != 116628031 and email_address != '@'")
      .select("member_key", "email_address")
      .withColumnRenamed("email_address", "dedup_email_address")
      .groupBy("member_key").agg(collect_set(trim(col("dedup_email_address"))).alias("dedup_email_address"))

    val dimMemberMultiPhone = hiveContext.table(DimMemberMultiPhoneSourceTable)
      .withColumn("phone_number", regexp_replace(col("phone_number"), regExBankCard, ""))
      .filter("phone_number != '0'")
      .filter("status='current' and member_key !='1134577523' and " +
        "phone_number not in('UNKNOWN','0','0000000000','8888888888','9999999999','5555555555','1111111111','0000000001','7777777777','2222222222')")
      .select("member_key", "phone_number")
      .withColumnRenamed("phone_number", "dedup_phone_number")
      .groupBy("member_key").agg(collect_set("dedup_phone_number").alias("dedup_phone_number"))

    val timeDimension = hiveContext.table(DimTimeTable)
      .filter("status = 'current'")
      .select("time_in_24hr_day", "time_key")
      .persist()

    val factTransactionDetail = hiveContext.table(FactTransactionDetailSourceTable)
      .filter("member_key <> -1")
      .select("member_key", "trxn_date", "trxn_time_key")
      .join(broadcast(timeDimension), col("trxn_time_key") === col("time_key"), "left")
      .withColumn("rank", row_number().over(Window.partitionBy("member_key").orderBy(desc("trxn_date"), desc("time_in_24hr_day"))))
      .filter("rank = 1")
      .select("member_key", "trxn_date")


    val dimMemberDedupResultsDF = hiveContext.table(DimMemberDedupResultsTable)

    val dedupSource = dimMember
      .join(factTransactionDetail, Seq(MemberKeyColumn), "left")
      .join(factCardHistory, Seq(MemberKeyColumn), "left")
      .join(dimMemberMultiEmail, Seq(MemberKeyColumn), "left")
      .join(dimMemberMultiPhone, Seq(MemberKeyColumn), "left")
      .select(dimMember.getColumns ++
        Seq("grouped_data.bank_card", "grouped_data.open_close_ind", "dedup_email_address", "dedup_phone_number", "trxn_date").map(col): _*)


    // Identify grouping
    val identifiedGroupsDF = dedupSource.transform(identifyGroups).persist


    if (logger.underlying.isDebugEnabled) {
      logger.debug("total count: {}", identifiedGroupsDF.count.toString)
      logger.debug("Grouping criteria counts:- ")
      identifiedGroupsDF.groupBy(GroupingCriteriaIDColumn).count.show
    }

    // Apply resolution for grouped records
    val resolvedGroupDF = identifiedGroupsDF.transform(resolveGroups)
      .withColumn(isLoyalResolved, when(col(s"$ResolvedColumn.$ResolvedFlagColumn") && CheckEmptyUDF(col(s"$MemberLoyaltyIDColumn")), lit(false)).otherwise(lit(true)))
      .cache
    logger.debug("resolvedGroupDF count :- {}", resolvedGroupDF.count.toString)


    val dedupFactRecords = getDedupFactRecords(resolvedGroupDF)
    logger.info("Fact dedup record counts to inserted: {}", dedupFactRecords.count.toString)

    dedupFactRecords
      .withColumn("batch_id", lit(batch_id))
      .select(hiveContext.table(FactDedupTargetTable).getColumns: _*)
      .insertIntoHive(SaveMode.Overwrite, dedupSource, FactDedupTargetTable, Some("batch_id"))

    logger.info("Fact dedup valid reocrd counts per group and collection_id : ")

    val dedupGrpCnt = hiveContext
      .table(FactDedupTargetTable)
      .filter(s"batch_id = $batch_id and is_invalid = false")
      .groupBy("dedupe_info_key")
      .agg(count(col("dedupe_info_key")).as("dedupe_grp_cnt"))
      .select("dedupe_info_key", "dedupe_grp_cnt")
    val dimDedupInfo = hiveContext
      .table(DimDedupInfoLookUpTable)
      .filter("status='current'")
      .persist

    dedupGrpCnt
      .join(broadcast(dimDedupInfo), Seq("dedupe_info_key"))
      .select("dedupe_info_key", "id_condition_desc", "collection_type_descr", "dedupe_grp_cnt")
      .orderBy(col("dedupe_info_key").cast("long"))
      .show(false)


    //Creating DataFrame for Dim Member
    val (resolved, unresolved) = resolvedGroupDF
      .withColumn(ResolutionInfoColumn, concat_ws(":", col(GroupingCriteriaIDColumn), col(GroupIDColumn), col("grouped_count")))
      .drop(InvalidGroupIDStatusColumn)
      .join(dedupFactRecords.select(InvalidGroupIDStatusColumn, GroupIDColumn)
        .filter(s"$InvalidGroupIDStatusColumn = true"), Seq(GroupIDColumn), "left")
      .partition(col(InvalidGroupIDStatusColumn).isNull && col(isLoyalResolved) === false)

    val resolvedMember = resolved
      .select(s"$ResolvedColumn.$CollapsedMemberColumn", ResolutionInfoColumn)
      .unstruct(CollapsedMemberColumn, dedupSource.schema)
      .withColumn(LastUpdatedDateColumn, current_timestamp)
      .withColumn("process", lit(ProcessType))
      .withColumn(actionFlag, concat_ws(":", lit("U"), col(ResolutionInfoColumn)))

    val dimMemColumns = dimMemberDedupResultsDF.getColumns(List("action_flag", "process"))
    val finalUnresolved = unresolved
      .withColumn(actionFlag,
        when(
          col(isLoyalResolved) === true && col(InvalidGroupIDStatusColumn) === false,
          concat_ws(":", lit("loyalty"), col(GroupingCriteriaIDColumn), col(GroupIDColumn), col("grouped_count"))
        ).otherwise(concat_ws(":", lit("invalid"), col(ResolutionInfoColumn))))
      .select(dimMemColumns :+ col(actionFlag): _*)
      .distinct
      .unionAll(identifiedGroupsDF.filter(s"$GroupingCriteriaIDColumn = 0")
        .select(dimMemColumns: _*).distinct.withColumn("action_flag", lit("ungrouped")))
      .withColumn("process", lit(ProcessType))

    val finalDimMemberDedupRecords =
      resolvedMember.select(dimMemberDedupResultsDF.getColumns: _*).distinct()
        .unionAll(finalUnresolved.select(dimMemberDedupResultsDF.getColumns: _*))

    val finalDimMemberDedupRecordsNonDuplicates = finalDimMemberDedupRecords
      .withColumn("rnk", row_number().over(Window.partitionBy(MemberKeyColumn).orderBy(desc(LastUpdatedDateColumn))))
      .filter("rnk = 1")

    // Updated records for Dim Member
    finalDimMemberDedupRecordsNonDuplicates.select(dimMemberDedupResultsDF.getColumns: _*)
      .insertIntoHive(SaveMode.Overwrite, dedupSource, DimMemberDedupResultsTable, Some("process"))


  }

}
