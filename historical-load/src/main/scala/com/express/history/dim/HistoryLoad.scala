package com.express.history.dim

import com.express.cdw.CDWContext
import com.express.cdw.CDWOptions
import com.express.cdw.spark.DataFrameUtils._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions._
import com.express.util.Settings
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.LongType

/**
  * Created by Mahendranadh.Dasari on 26-10-2017.
  */
object HistoryLoad extends CDWContext with CDWOptions {

  addOption("tableName", true)
  addOption("filePath", true)
  addOption("filePath2",false)


  def transformColumns(transformations: Map[String, String], df: DataFrame): DataFrame = {
    transformations.keys.foldLeft(df) {
      (df, key) => df.withColumn(key, expr(transformations(key)))
    }
  }

  def createDF(filePath: String): DataFrame ={
    hiveContext.read.format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .option("delimiter", "|")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls","true")
      .load(filePath)
  }


  def addDefaultValues(dataFrame: DataFrame, batch_id: String): DataFrame = {
    val sqc = dataFrame.sqlContext
    val nullValues = dataFrame.columns.tail.map { c => null }.toSeq
    val defaultRows = sqc.sparkContext.parallelize(Seq(Row.fromSeq(-1l +: nullValues), Row.fromSeq(0l +: nullValues)))
    dataFrame.unionAll(dataFrame.sqlContext.createDataFrame(defaultRows, dataFrame.schema))
      .withColumn("last_updated_date", current_timestamp())
      .withColumn("batch_id", lit(batch_id))
  }

  def main(args: Array[String]): Unit = {
    // arg -> tablename, filepath
    // read via spark csv
    //get the config using tablename
    // df rename
    // df transform

    println("ARGS:" + args.mkString(","))
    val options = parse(args)


    val tablename = options("tableName")
    val filePath = options("filePath")
    val filePath2= options.getOrElse("filePath2", "")
    val batch_id = options("batch_id")


    //val tablename = args(0)
    //val filePath = args(1)

    //    println("tablename, " + tablename)
    //  println("filepath =" +  filePath)

    val isDimension = tablename.toLowerCase.startsWith("dim")
    val goldTable = s"$goldDB.$tablename"
    val tableConfig = Settings.getHistoryMapping(tablename)
    val goldDF = hiveContext.table(goldTable)

    val surrogateKey = tableConfig.surrogateKey
    val generateSK = tableConfig.generateSK

   /* val rawData = hiveContext.read.format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .option("delimiter", "|")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls","true")
      .load(filePath)
   */


    val rawData = createDF(filePath)




    val renamedDF = rawData.renameColumns(tableConfig.renameConfig)

    val transformDF = transformColumns(tableConfig.transformConfig, renamedDF)
      //.generateSequence(0, Some("col")).distinct()
      // .drop(tableConfig.surrogateKey)
      .persist()

    //val (defaultValuesDF, totalValuesDF) = transformDF.partition(col(surrogateKey) === -1 or col(surrogateKey) === 0)

    //  val mainDF = totalValuesDF.drop(surrogateKey)

    val rankFunc = Window.partitionBy(tableConfig.grpColumns.map(col): _*)
      .orderBy(tableConfig.ordColumns.map(desc): _*)

    val rankedData = transformDF.withColumn("rank", row_number() over rankFunc)

    val currentData = rankedData.filter("rank = 1")
    val historyData = rankedData.filter("rank != 1")


    val (currentDataWithKeys, historyDataWithKeys) = if (generateSK) {

      val historyDataWithKeys = historyData.drop(surrogateKey).generateSequence(0, Some(tableConfig.surrogateKey))
      val maxHistoryCnt = historyDataWithKeys.maxKeyValue(col(tableConfig.surrogateKey))
      val currentDataWithKeys = currentData.drop(surrogateKey).generateSequence(maxHistoryCnt, Some(tableConfig.surrogateKey))
      (currentDataWithKeys, historyDataWithKeys.unionAll(currentDataWithKeys))
    } else {
      val (defaultValuesDF, totalValuesDF) = rankedData.partition(col(surrogateKey) === "-1" or col(surrogateKey) === "0")

      val currentData = totalValuesDF.filter("rank = 1")
      val historyData = totalValuesDF.filter("rank  != 1")
      (currentData.castColumns(Seq(surrogateKey), LongType)
        , historyData.unionAll(currentData).castColumns(Seq(surrogateKey), LongType))
    }
    val currentDataWithDefaultValues = addDefaultValues(currentDataWithKeys, batch_id)
    val historyDataWithDefaultValues = addDefaultValues(historyDataWithKeys, batch_id)


    if (isDimension) {
      val finalDF = currentDataWithDefaultValues.withColumn("status", lit("current"))
        .unionAll(historyDataWithDefaultValues.withColumn("status", lit("history")))

      finalDF.printSchema()
      finalDF.select(goldDF.getColumns: _*).show()
      finalDF.select(goldDF.getColumns: _*).insertIntoHive(SaveMode.Overwrite, Nil, goldTable, Some("status"), null)
    } else
      transformDF.select(goldDF.getColumns: _*).insertIntoHive(SaveMode.Overwrite, Nil, goldTable, None, null)
  }


}
