package com.express.dedup

import com.express.cdw.CDWContext
import com.express.cdw.spark.DataFrameUtils._
import com.express.dedup.utils.Settings
import com.typesafe.config.ConfigException
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{row_number, _}
import org.apache.spark.sql.expressions.Window


/**
  * Created by Gaurav.Maheshwari on 7/6/2017.
  */
object UpdateDimFact extends CDWContext with LazyLogging {

  import hiveContext.implicits._

  private def updateDimension(inputDF: DataFrame, tableName: String): DataFrame = {
    try {
      val tableConfig = Settings.getTableConfig(tableName)
      val windowSpec = Window.partitionBy(tableConfig.partition_columns.map(col): _*)
        .orderBy(tableConfig.order_columns.map(desc): _*)
      inputDF.withColumn("rn", row_number.over(windowSpec)).where($"rn" === 1).drop("rn")
    }
    catch {
      case e: ConfigException.Missing => inputDF
    }
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 3)
      throw new Exception("Please provide three arguments ::  table name, loadtype and batch_id ")

    val tableName = args(0) // Table name for which dedup to be run
    val loadType = args(1) //fact or dim
    val batch_id = args(2)

    logger.info("Dedup processing for tableName:: " + tableName + "  type of load(dim|fact):: " + loadType + " For Batch_ID:: " + batch_id)

    val goldTable = loadType match {
      case "dim" => hiveContext.sql(s"select * from $goldDB.$tableName where status='current'")
      case "fact" => hiveContext.table(s"$goldDB.$tableName")
    }

    logger.info("loading dedup member table")
    /**
      * TO DO :   * need to change dedup work table name and need to add filter
      * condition on batch id (for dim_employee handle filter condition in case block)
      *
      */
    val dedupMember = hiveContext.sql(s"select old_member_key, new_member_key from $goldDB.fact_dedupe_member_history" +
      s" where batch_id='$batch_id' and is_invalid = false").persist
    logger.info("joining member dedup table with target table")

    //val new_member_key_list = dedupMember.select("new_member_key").collect().map(_ (0)).toList
    val new_member_key_list = dedupMember.select("new_member_key").persist

    //Handle -1 member_keys
    val (goldTableWithUnknownMem, goldTableWithknownMem) = goldTable.partition(expr("member_key = -1"))

    val joinedDF = goldTableWithknownMem.join(dedupMember, $"member_key" === $"old_member_key", "left")

    val unmatched = joinedDF.filter(isnull($"old_member_key")).drop($"old_member_key").drop($"new_member_key")
      .select(goldTable.getColumns: _*)
    logger.info("replacing the old member key with new for matched record")

    val matched = joinedDF.filter(not(isnull($"old_member_key")))
      .select(goldTable.getColumns(List("member_key")) :+ $"new_member_key": _*)
      .withColumnRenamed("new_member_key", "member_key")
      .select(goldTable.getColumns: _*)

    logger.info("matched record count after joining with old_member_key :==========>" + matched.count())
    logger.info("switch case execution")

    val (processedRecords, targetTable) = loadType match {

      case "dim" =>
        logger.info(s"Processing Started for $tableName table")
        val matchedDFWithOldMemberCol = joinedDF
          .withColumn("member_key", expr("case when old_member_key is not null then new_member_key else member_key end "))
          .withColumn("action_flag", expr("case when old_member_key is not null then 'U' else 'NCD' end "))

/*

        val updateInsertDF = updateDimension(matchedDFWithOldMemberCol,tableName.toLowerCase)

        val finalDF= updateInsertDF
          .dropColumns(Seq("batch_id", "status"))
          .withColumn("last_updated_date",when(col("action_flag") === "U",current_date()).otherwise(col("last_updated_date")))
          .withColumn("batch_id", lit(s"$batch_id"))
          .withColumn("process", lit("dedup"))

        (finalDF,s"$workDB.$tableName")
*/


        tableName match {

          /**
            * -	Update the Member_Key of the duplicate records with the survivor’s Member_Key.
            * Remove the records from Member_Multi_Email which are duplicates based on the Member_Key + Email Address / Phone Number combination
            * choose only with email_consent/phone_consent =Y then N followed
            * by most recent last_updated_date
            */
          case "dim_member_multi_email" =>
            val windowSpec = Window.partitionBy($"member_key", $"email_address")
              .orderBy($"is_loyalty_email".desc,$"email_consent".desc, $"last_updated_date".desc)
            val updateDF = matchedDFWithOldMemberCol.dropColumns(Seq("batch_id", "status"))
            val updateInsertDF = updateDF.withColumn("rn", row_number.over(windowSpec)).where($"rn" === 1)
              .drop("rn")
              .withColumn("process", lit("dedup"))
              .withColumn("batch_id", lit(s"$batch_id"))
            logger.info("Insert Update count :: " + updateInsertDF.count())
            (updateInsertDF, s"$workDB.$tableName")


          case "dim_member_multi_phone" =>
            val windowSpec = Window.partitionBy($"member_key", $"phone_number")
              .orderBy($"is_loyalty_flag".desc,$"phone_consent".desc, $"last_updated_date".desc)
            val updateDF = matchedDFWithOldMemberCol.dropColumns(Seq("batch_id", "status"))
            val updateInsertDF = updateDF.withColumn("rn", row_number.over(windowSpec)).where($"rn" === 1).drop("rn")
              .withColumn("process", lit("dedup"))
              .withColumn("batch_id", lit(s"$batch_id"))
            (updateInsertDF, s"$workDB.$tableName")

          case "dim_member_consent" =>
            val windowSpec = Window.partitionBy($"member_key", $"consent_type")
              .orderBy($"consent_date".desc, $"consent_history_id".desc)
            val updateDF = matchedDFWithOldMemberCol.dropColumns(Seq("batch_id", "status"))
            val updateInsertDF = updateDF.withColumn("rn", row_number.over(windowSpec)).where($"rn" === 1).drop("rn")
              .withColumn("process", lit("dedup"))
              .withColumn("batch_id", lit(s"$batch_id"))
            (updateInsertDF, s"$workDB.$tableName")


          case "dim_employee" =>
            (matchedDFWithOldMemberCol
              .dropColumns(Seq("batch_id", "status"))
              .withColumn("batch_id", lit(s"$batch_id")), s"$workDB.$tableName")

          case _ =>
            (matchedDFWithOldMemberCol
              .dropColumns(Seq("batch_id", "status"))
              .withColumn("batch_id", lit(s"$batch_id"))
              .withColumn("process", lit("dedup")), s"$workDB.$tableName")
        }

      /*       /**
               * -	Update the Member_Key of the duplicate records with the survivor’s Member_Key.
               * Remove the records from Member_Multi_Email which are duplicates based on the Member_Key + Email Address / Phone Number combination
               * choose only with email_consent/phone_consent =Y then N followed
               * by most recent last_updated_date
               *
               */
             val updateInsertDF = updateDimension(matchedDFWithOldMemberCol,tableName.toLowerCase)

             val finalDF= updateInsertDF
               .dropColumns(Seq("batch_id", "status"))
               .withColumn("last_updated_date",when(col("action_flag") === "U",current_timestamp()).otherwise(col("last_updated_date")))
               .withColumn("batch_id", lit(s"$batch_id"))
               .withColumn("process", lit("dedup"))

             (finalDF,s"$workDB.$tableName")*/

      case "fact" =>
        //Fact loading

        tableName match {
          case "fact_card_history" =>
            logger.info("Processing Started for fact_card_history table")
            val windowSpec = Window
              .partitionBy($"member_key", $"tokenized_cc_nbr")
              .orderBy($"last_updated_date".desc)
            val (unmatched_new, unmatched_not_new) = unmatched
              .join(broadcast(new_member_key_list), $"member_key" === $"new_member_key", "left")
              .partition($"new_member_key".isNotNull)
            val deduped_records = matched
              .unionAll(unmatched_new.dropColumns(Seq("new_member_key")))
              .withColumn("rn", row_number.over(windowSpec))
              .withColumn("batch_id", min(col("batch_id")).over(Window
                .partitionBy($"member_key", $"tokenized_cc_nbr")))
              .where($"rn" === 1).drop("rn")
              .select(matched.getColumns: _*)

            (deduped_records.unionAll(unmatched_not_new.dropColumns(Seq("new_member_key"))), s"$goldDB.$tableName")
          case _ =>
            logger.info("union the unmatched with matched")
            val finalRecords = unmatched.unionAll(matched.withColumn("last_updated_date", current_timestamp()))
              .select(goldTable.getColumns: _*)
            (finalRecords, s"$goldDB.$tableName")
        }

    }


    logger.info("inserting the data into table  " + s"$targetTable")
    loadType match {
      case "fact" =>
        val finalProcessedRecords = processedRecords
          .unionAll(goldTableWithUnknownMem.select(goldTable.getColumns: _*))
        tableName match {
          case "fact_card_history" =>
            finalProcessedRecords.insertIntoHive(SaveMode.Overwrite, targetTable, Some("status"), batch_id)
          case _ =>
            finalProcessedRecords.insertIntoHive(SaveMode.Overwrite, targetTable, None, batch_id)
        }

      case "dim" =>
        val workDF = hiveContext.table(s"$workDB.$tableName")
        val goldTableWithUnknownMemAllCols = goldTableWithUnknownMem
          .withColumn("process",lit("dedup"))
          .withColumn("action_flag",lit("NCD"))
          .select(workDF.getColumns: _*)
        val finalProcessedRecords = processedRecords
          .select(workDF.getColumns: _*)
          .unionAll(goldTableWithUnknownMemAllCols)
        tableName match {
          case "dim_employee" =>
            finalProcessedRecords
              .insertIntoHive(SaveMode.Overwrite, targetTable, None, batch_id)
          case _ =>
            finalProcessedRecords
              .insertIntoHive(SaveMode.Overwrite, targetTable, Some("process"), batch_id)
        }
    }
  }
}
