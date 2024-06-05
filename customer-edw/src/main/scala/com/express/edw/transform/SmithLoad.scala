package com.express.edw.transform

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.{Column, ColumnName, DataFrame, SaveMode}
import com.express.cdw.{CDWContext, CDWOptions}
import org.apache.spark.sql.functions._
import com.express.cdw.spark.DataFrameUtils._
import com.express.edw.util.Settings
import org.apache.spark.sql.expressions.Window
import org.joda.time.DateTime

/**
  * SmithLoad trait that is to be extended for all EDW table loads
  */
trait SmithLoad extends LazyLogging with CDWContext with CDWOptions {

  addOption("businessdate",false)
  addOption("rundate",false)

  /**
    * Get target edw table name
    */
  def tableName: String


  /**
    * Get primary gold source table
    */
  def sourcetable: String

  /**
    * Get primary filter condition on source table
    */
  def filterCondition: String = "true"

  /**
    * Get Target Table columns
    */
  def targetColumnList: Array[ColumnName] = hiveContext.table(s"$smithDB.$tableName").getColumns


  /**
    * Get list of required columns from primary source table
    */
  def sourceColumnList: Seq[String] = Seq()

  /**
    * Get configuration properties for the table
    */
  val tableConfig = Settings.getEDWConfigMapping(tableName)

  /**
    * Get source data from primary source table
    * @return source dataframe
    */
  def sourceData: DataFrame = {
    sourceColumnList match {
      case Nil =>
        hiveContext.table(sourcetable).filter(filterCondition)
      case _ =>
        hiveContext.table(sourcetable).filter(filterCondition).select(sourceColumnList.head, sourceColumnList.tail: _*)
    }
  }

  /**
    * Perform transformations on sourceData
    * @param A_C_check Column for determining the inserted('A') and updated records('C'). Pass ingest_date column in case of all inserted 'A' records.
    *                  Not Applicable for lookup tables.None by default.
    * @param sourceDF Source Data on which transformations have to be applied.equal to sourceData by default.
    * @param business_date previous run date of EDW Incremental Load
    * @return
    */
  def transformData( A_C_check : Option[Column] = None, sourceDF : DataFrame = sourceData, run_date : String = DateTime.now().toDate.toString, business_date : String = DateTime.now().toDate.toString ) : DataFrame = {

    val to_transformDF = A_C_check match {
      case None => sourceDF
      case Some(column) => sourceDF
        .withColumn("ingest_date", to_date(col("last_updated_date")))
        .withColumn("cc_flag", when(col("ingest_date") <= to_date(lit(business_date)), lit("NC")).otherwise(when(column === col("ingest_date"), lit("A"))
          .otherwise(lit("C"))))
    }

    to_transformDF
      .renameColumns(tableConfig.renameConfig)
      .applyExpressions(tableConfig.transformConfig.map{case (k,v) => k -> s"trim($v)"})
      .withColumn("run_date", to_date(lit(run_date)) )
      .select(targetColumnList: _*)
  }


  def getStoreId(DF: DataFrame, StoreDF: DataFrame, JoinCol: String, RenameCol: String): DataFrame = {
    DF.join(broadcast(StoreDF),
      DF.col(JoinCol) === StoreDF.col("store_key"), "left")
      .select("store_id", DF.columns: _*)
      .withColumnRenamed("store_id", RenameCol)
      .drop(col("store_key"))
      .drop(col(JoinCol))
  }

  def getExpOrderId(DF: DataFrame, OrderDF: DataFrame, JoinCol: String, PartCol: String): DataFrame = {
    DF.join(OrderDF,
      DF.col(JoinCol) === OrderDF.col("trxn_id_order"), "left")
      .withColumn("rank", row_number() over Window.partitionBy(PartCol))
      .filter("rank = 1")
      .select("exp_order_id", DF.columns: _*)
      .drop(col("trxn_id_order"))
  }

  def leftJoin(DF1: DataFrame, DF2: DataFrame, DF1JoinCol: String, DF2JoinCol: String): DataFrame = {
    DF1.join(DF2,
      DF1.col(DF1JoinCol) === DF2.col(DF2JoinCol), "left")
      .drop(col(DF2JoinCol))
  }

  def rightJoin(DF1: DataFrame, DF2: DataFrame, DF1JoinCol: String, DF2JoinCol: String): DataFrame = {
    DF1.join(DF2,
      DF1.col(DF1JoinCol) === DF2.col(DF2JoinCol), "right")
      .drop(col(DF1JoinCol))
  }


  def innerJoin(DF1: DataFrame, DF2: DataFrame, DF1JoinCol: String, DF2JoinCol: String): DataFrame = {
    DF1.join(DF2,
      DF1.col(DF1JoinCol) === DF2.col(DF2JoinCol), "inner")
  }


  /**
    * Overwrite transformed DF into given target table
    * @param targetTable table into which the data has to be loaded
    * @param DF transformed DF
    */
  def load(targetTable : String, DF : DataFrame)= {
    DF.write.mode(SaveMode.Overwrite).insertInto(targetTable)
  }

  /**
    * Append transformed DF into EDW table tableName
    * @param DF transformed DF
    */
  def load(DF : DataFrame ) ={
    DF.write.partitionBy("run_date").mode(SaveMode.Overwrite).insertInto(s"$smithDB.$tableName")
  }

}