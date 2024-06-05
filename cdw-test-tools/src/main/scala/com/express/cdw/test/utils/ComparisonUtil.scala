package com.express.cdw.test.utils

import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Table comparison utility for the CDH and Gold tables
  *
  * @author mbadgujar
  */
class ComparisonUtil(cdhTable: String, goldTable: String, primaryKeys: Seq[String], columnsToSkip: Seq[String]) extends
  CDWContext with LazyLogging {

  import hiveContext.implicits._
  import ComparisonUtil._

  private val cdhDb = cdhTable.split("\\.").head
  private val goldDb = goldTable.split("\\.").head
  private val goldTableName = goldTable.split("\\.").last
  private val MD5NonKeyColumn = "md5_non_key"
  private val Separator = ":"
  private val CompareDB = "cdh_compare"


  private val cdhDF = hiveContext.table(cdhTable)
  private var goldDF = hiveContext.table(goldTable)
  goldDF = if (goldDF.columns.contains("status")) goldDF.filter("status = 'current'") else goldDF

  private val cdhNkColumns = cdhDF.columns
    .filterNot((primaryKeys ++ columnsToSkip ++ Seq("ingest_date", "status")).contains)
    .intersect(goldDF.columns).distinct

  // apply column expressions
  private def applyExprs(map: Map[String, Column]): DataFrame => DataFrame = {
    (dataFrame: DataFrame) =>
      map.keys.foldLeft(dataFrame) {
        case (df, column) => df.withColumn(column, map(column))
      }
  }

  // converts empty strings to null
  def convertEmptyStrToNull(df: DataFrame): DataFrame = {
    val stringColsCheck = df.dtypes
      .filter { case (_, cType) => cType.equals("StringType") }
      .map(_._1)
      .map(column => column -> when(CheckEmptyUDF(col(column)), null).otherwise(col(column)))
      .toMap
    df.transform(applyExprs(stringColsCheck))
  }

  // compare and write results to hive
  def compare(): Unit = {

    val cdhTableCount = cdhDF.select(primaryKeys.map(col): _*).count
    val goldTableCount = goldDF.select(primaryKeys.map(col): _*).count
    logger.info(s"$cdhTable count : {}", cdhTableCount.toString)
    logger.info(s"$goldTable count : {}", goldTableCount.toString)
    logger.info("Matching Columns found in CDH and Gold Table that will be compared: - {}", cdhNkColumns.mkString(", "))
    val cdhDFWithMD5 = cdhDF.transform(convertEmptyStrToNull)
      .withColumn(s"${cdhDb}_concat", lower(concat_ws(Separator, cdhNkColumns.map(col): _*)))
      .withColumn(s"${cdhDb}_$MD5NonKeyColumn", md5(col(s"${cdhDb}_concat")))
      .renameColumns(cdhNkColumns.map(col => col -> s"${cdhDb}_$col").toMap)

    val goldDFWithMD5 = goldDF.transform(convertEmptyStrToNull)
      .select((primaryKeys ++ cdhNkColumns).map(col): _*)
      .withColumn(s"${goldDb}_concat", lower(concat_ws(Separator, cdhNkColumns.map(col): _*)))
      .withColumn(s"${goldDb}_$MD5NonKeyColumn", md5(col(s"${goldDb}_concat")))
      .renameColumns(cdhNkColumns.map(col => col -> s"${goldDb}_$col").toMap)

    val joinResult = cdhDFWithMD5.join(goldDFWithMD5, primaryKeys).persist
    val matchedPrimaryKeyCount = joinResult.count
    logger.info(s"Primary key matched count : {}", matchedPrimaryKeyCount.toString)

    val notMatched = joinResult.filter(col(s"${cdhDb}_$MD5NonKeyColumn") !== col(s"${goldDb}_$MD5NonKeyColumn"))
    val nonMatchingRecordsCount = notMatched.count
    logger.info(s"Count of records with non-matching columns : {}", nonMatchingRecordsCount.toString)

    val diffFuncMap = cdhNkColumns
      .map(column => column -> when((col(s"${cdhDb}_$column").isNull and col(s"${goldDb}_$column").isNull) or
        col(s"${cdhDb}_$column") === col(s"${goldDb}_$column"), null).otherwise(
        concat_ws(Separator, col(s"${cdhDb}_$column"), col(s"${goldDb}_$column"))))
      .toMap

    val differenceResult = notMatched.transform(applyExprs(diffFuncMap))
    val countFuncList = diffFuncMap.map { case (column, func) => count(func).as(column) }.toSeq
    val differenceResultCount = notMatched.select(countFuncList: _*).persist
    val countStats = CountStats(cdhTableCount, goldTableCount, matchedPrimaryKeyCount, nonMatchingRecordsCount)
    Seq(countStats).toDF.write.mode("overwrite").saveAsTable(s"$CompareDB.${goldTableName}_count_stats")
    differenceResult.select((primaryKeys ++ cdhNkColumns).map(col): _*).write.format("orc")
      .mode("overwrite").saveAsTable(s"$CompareDB.${goldTableName}_non_matching_records")
    differenceResultCount.show(false)
    differenceResultCount.write.mode("overwrite").saveAsTable(s"$CompareDB.${goldTableName}_column_count_stats")
  }


}

object ComparisonUtil {
  private val cdhTableOption = "cdh_table"
  private val goldTableOption = "gold_table"
  private val primaryKeysOption = "primary_keys"
  private val skipColumnsOption = "skip_columns"
  private val options = new Options().addOption(cdhTableOption, true, "CDH table")
    .addOption(goldTableOption, true, "Gold table")
    .addOption(primaryKeysOption, true, "Primary Keys")
    .addOption(skipColumnsOption, true, "Columns to Skip")

  case class CountStats(cdhTableCount: Long, goldTableCount: Long, matchedPrimaryKeyCount: Long, nonMatchingRecordsCount: Long)

  def main(args: Array[String]): Unit = {
    val cmdLine = new BasicParser().parse(options, args, true)
    val optionMap = cmdLine.getOptions.map(option => option.getOpt.trim -> option.getValue.trim).toMap
    new ComparisonUtil(optionMap(cdhTableOption), optionMap(goldTableOption), optionMap(primaryKeysOption).split(",").map(_.trim),
      optionMap(skipColumnsOption).split(",").map(_.trim)).compare()
  }
}
