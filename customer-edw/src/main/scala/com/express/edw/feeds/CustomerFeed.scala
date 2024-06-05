package com.express.edw.feeds

import com.express.edw.util.Settings
import com.express.cdw.{CDWContext, CDWOptions, EDW}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Calendar

object CustomerFeed extends CDWContext with CDWOptions with LazyLogging {

  // Adding input parameter list
  addOption("moduleName")
  addOption("batchID")
  addOption("startDate")
  addOption("endDate")

  def main(args: Array[String]): Unit = {

    //printing input parameters
    println("ARGS:" + args.mkString(","))

    //importing input parameters
    val options = parse(args)
    val moduleName = options("moduleName")
    val batchID = options("batchID")
    val startDate = options("startDate")
    val endDate = options("endDate")
    val filename = s"${moduleName}_feeds"
    val date = Calendar.getInstance.getTime
    val date_format = new SimpleDateFormat("yyyy-MM-dd")


    // filter condition to get data from edw tables
    val filterCondition = {
      if (endDate == "na")
        s"run_date = '$startDate'"
      else
        s"run_date between '$startDate' and '$endDate'"
    }


    //getting data from config file
    val tableConfig = Settings.getFeedMapping(filename)
    val joinColumn = tableConfig.joinColumnConfig
    val finalColumn = tableConfig.finalColumnConfig.seq
    val smithMainTable = tableConfig.tableMainNameConfig
    val smithLookupTable = tableConfig.tableLookupNameConfig
    val regexReplaceColumn = tableConfig.regexReplaceColumnsConfig
    val selectColumn = finalColumn.mkString(",")

    import hiveContext.implicits._

    // function for multiple joins
    def trimJoinList(strings: List[String]) = strings.map(_.trim)

    def applyJoinCondition(columns: Map[String, Seq[String]], MainTable: DataFrame, keyTable: DataFrame, joinColumn: String, valueColumn: String, JoinColumnValue: String): DataFrame = {
      columns.keys.toSeq.foldLeft(MainTable) {
        (df, column) =>

          var columnJoinConditionHead: List[String] = List("")
          if (columns(column).size == 1) {
            columnJoinConditionHead = columns(column).map(x => x.replaceAll("[\\[]", "").replaceAll("[\\]]", "")).mkString(",").split(",").toList :+ "NA"
          }
          else {
            columnJoinConditionHead = columns(column).map(x => x.replaceAll("[\\[]", "").replaceAll("[\\]]", "")).mkString(",").split(",").toList
                        //val columnJoinConditionTail= columns(column).tail.mkString("").replaceAll("[\\[\\]]", "")
          }
          val joinCondition = trimJoinList(columnJoinConditionHead)
          df
            .join(broadcast(keyTable.select(joinColumn, valueColumn, JoinColumnValue)), trim(upper(col(column))) === trim(upper(col(joinColumn))) && trim(upper(col(JoinColumnValue))).isin(joinCondition: _*), "left")
            .withColumn(s"$column", col(valueColumn))
            .drop(joinColumn).drop(valueColumn).drop(JoinColumnValue)
      }.na.fill(-1, columns.keys.toSeq)
    }


    // function to replace pipe from column values in dataframe
    def applyRegexExpressions(exprMap: Seq[String], table: DataFrame): DataFrame = {
      if (exprMap.size > 0){
      exprMap.foldLeft(table) {
        (df, key) => df.withColumn(key, regexp_replace(col(key), "[|]", ""))
      }
    }
      else {
        logger.info("Processing not requierd")
        table
      }
    }


    // function to replace string null to null in dataframe
    def applyNullExpressions(exprMap: Seq[String], table: DataFrame): DataFrame = {
      exprMap.foldLeft(table) {
        (df, key) =>
          df
            .withColumn(key, when(lower(col(key)) === lit("null"), lit("")).otherwise(col(key)))
      }
    }


//    def concatStringExpression(table: DataFrame): DataFrame = {
//              table.withColumn("concat_card",concat_ws("|",table.columns.map(c => col(c)): _*))
//          }


    val finalDF =  moduleName match {

      case EDW.CustomerFeed | EDW.HouseHoldFeed | EDW.TransactionSummaryFeed | EDW.transactionDetailFeed =>

        // loading smith tables
        val edwMainTableDF = hiveContext.sql(s"select $selectColumn from $smithDB.$smithMainTable where $filterCondition ")
        val constantMaxDate: String = hiveContext.sql(s"select max(run_date) date from $smithDB.$smithLookupTable").collect.mkString(" ").replaceAll("[\\[\\]]", "")
        val edwConstantsDF = hiveContext.table(s"$smithDB.$smithLookupTable").filter(s"run_date = '$constantMaxDate'")

        // Joining smith tables
        val joinDF = applyJoinCondition(joinColumn, edwMainTableDF, edwConstantsDF, "constant_value", "constant_id", "constant_type")

        // replacing pipe from column values in dataframe
        val cleansedDF = applyRegexExpressions(regexReplaceColumn, joinDF)


        // replacing string null to null in dataframe
         applyNullExpressions(finalColumn, cleansedDF)
                      .orderBy(asc("ingest_date"))


      case EDW.CustomerDedupe | EDW.CustomerDedupeTrnx =>

        val edwMainTableDF = hiveContext.sql(s"select $selectColumn from $smithDB.$smithMainTable where $filterCondition ")

        val regexReplaceDF = applyRegexExpressions(regexReplaceColumn, edwMainTableDF)

        val cleansedDF = applyNullExpressions(finalColumn, regexReplaceDF)

        cleansedDF.select(finalColumn.head, finalColumn.tail: _*)


      case EDW.ConstantsFeed| EDW.LookupScoringModelFeed | EDW.LookupTransactionTypeFeed | EDW.LookupDiscountTypeFeed | EDW.LookupModelSegmentFeed | EDW.LookupLoyaltyFeed =>
        // loading smith tables
        val constantMaxDate: String = hiveContext.sql(s"select max(run_date) date from $smithDB.$smithMainTable").collect.mkString(" ").replaceAll("[\\[\\]]", "")

        val edwMainTableDF = hiveContext.sql(s"select $selectColumn from $smithDB.$smithMainTable where run_date = '$constantMaxDate' ")
                                        .withColumn("run_date",lit(s"$startDate"))


        val regexReplaceDF = applyRegexExpressions(regexReplaceColumn, edwMainTableDF)

        val cleansedDF = applyNullExpressions(finalColumn, regexReplaceDF)

        cleansedDF.select(finalColumn.head, finalColumn.tail: _*)

    }


    finalDF.select(finalColumn.head, finalColumn.tail: _*)
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("delimiter", "|")
      .option("nullValue", "")
      .option("quoteMode","NONE")
      .option("escape","\"")
      .option("dateFormat", "yyyy-MM-dd")
      .save(s"/apps/cdw/outgoing/edw_feeds/$moduleName/$batchID")

  }
}