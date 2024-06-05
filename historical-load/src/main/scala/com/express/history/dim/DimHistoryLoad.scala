package com.express.history.dim

import java.sql.Date

import com.express.cdw.{CDWContext, CDWOptions, strToDate}
import com.express.cdw.spark.DataFrameUtils._
import com.express.util.Settings
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, UserDefinedFunction}
import com.express.cdw.spark.udfs.strToDateUDF

/**
  * Created by Mahendranadh.Dasari on 26-10-2017.
  */
object DimHistoryLoad extends CDWContext with CDWOptions {

  addOption("tableName", true)
  addOption("filePath", true)
  addOption("filePath2",false)



  hiveContext.udf.register[Date, String, String]("parse_date", strToDate)


  def transformColumns(transformations: Map[String, String], df: DataFrame): DataFrame = {
    transformations.keys.foldLeft(df) {
      (df, key) => df.withColumn(key, expr(transformations(key)))
    }
  }

  def createDF(filePath: String,nullValue: String = ""): DataFrame ={
    hiveContext.read.format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .option("delimiter", "|")
      .option("nullValue", nullValue)
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

/*
  def addDefaultValues(dataFrame: DataFrame, grpColumns: Array[String], batch_id: String): DataFrame = {
    val sqc = dataFrame.sqlContext
    val f1 = grpColumns.map(col).reduce{case (c1, c2) => c1 === "-1" and c2 === "-1"}
    val f2 = grpColumns.map(col).reduce{case (c1, c2) => c1 === "0" and c2 === "0"}
    val filterSqc = dataFrame.filter(f1 or f2)
    val nonNkValues = dataFrame.columns.diff(grpColumns)
    val nullValues = nonNkValues.map { c => null }.toSeq
    val zeronkValues = grpColumns.map(col => 0l)
    val minus1 = grpColumns.map(col => -1l)
    val defaultRows = sqc.sparkContext.parallelize(Seq(Row.fromSeq(zeronkValues ++ nullValues), Row.fromSeq(minus1 ++ nullValues)))
    filterSqc.unionAll(dataFrame.sqlContext.createDataFrame(defaultRows, dataFrame.select((grpColumns ++ nonNkValues).map(col):_*).schema)
      .withColumn("last_updated_date", current_timestamp())
      .withColumn("batch_id", lit(batch_id)))
  }
  
*/

  def main(args: Array[String]): Unit = {

    println("ARGS:" + args.mkString(","))
    val options = parse(args)


    val tablename = options("tableName")
    val filePath = options("filePath")
    val filePath2= options.getOrElse("filePath2", "")
    val batch_id = options("batch_id")

    val goldTable = s"$goldDB.$tablename"
    val tableConfig = Settings.getHistoryMapping(tablename)
    val goldDF = hiveContext.table(goldTable)


    val surrogateKey = tableConfig.surrogateKey
    val generateSK = tableConfig.generateSK
    val groupColumns = tableConfig.grpColumns.toArray




    val rawData = if(tablename.equalsIgnoreCase("dim_member_multi_phone")){
      val df1 =createDF(filePath,"NULL")
      val df=df1.withColumnRenamed("phone_nbr","phone_number")
          .filter("member_key is not null and phone_number is not null")
             .withColumnRenamed("create_date","ph_create_date")
             .withColumnRenamed("loyalty_flag","ph_loyalty_flag")
      val df2=createDF(filePath2,"NULL").filter("member_key is not null and phone_number is not null")
              .withColumnRenamed("create_date","mb_create_date")
              .withColumnRenamed("loyalty_flag","mb_loyalty_flag")
      df.join(df2,Seq("member_key","phone_number"),"full")
    }else if(tablename.equalsIgnoreCase("dim_member_multi_email")){
      val df1=createDF(filePath)
      val df2=hiveContext.table(s"$goldDB.adobe_mmeid_mapping").withColumnRenamed("email","email_address")
      df1.join(df2,Seq("member_key","email_address"),"left_outer")
    }else if(tablename.equalsIgnoreCase("dim_household")){

      val filedata=createDF(filePath)
        .withColumn("first_trxn_date",strToDateUDF(col("first_trxn_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("add_date",strToDateUDF(col("add_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("introduction_date",strToDateUDF(col("introduction_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("last_store_purch_date",strToDateUDF(col("last_store_purch_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("last_web_purch_date",strToDateUDF(col("last_web_purch_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("account_open_date",strToDateUDF(col("account_open_date"),lit("MM/dd/yyyy HH:mm:ss")));
      val dimdate=hiveContext.table(s"$goldDB.dim_date").select(col("date_key"),col("sdate"),col("status")).filter("status='current'").drop(col("status"))
      val dim_member=hiveContext.table(s"$goldDB.dim_member").select(col("member_key"),col("household_key"),col("status")).filter("status='current'").drop(col("status")).groupBy(col("household_key")).agg(collect_list(col("member_key")) as "associated_member_key")
      val withfirst_date=filedata.join(dimdate.withColumnRenamed("date_key","first_trxn_date_key").withColumnRenamed("sdate","first_trxn_date"),Seq("first_trxn_date"),"left_outer")
      val withadd_date=withfirst_date.join(dimdate.withColumnRenamed("date_key","add_date_key").withColumnRenamed("sdate","add_date"),Seq("add_date"),"left_outer")
      val withintro_date=withadd_date.join(dimdate.withColumnRenamed("date_key","introduction_date_key").withColumnRenamed("sdate","introduction_date"),Seq("introduction_date"),"left_outer")
      val withstore_purch=withintro_date.join(dimdate.withColumnRenamed("date_key","last_store_purch_date_key").withColumnRenamed("sdate","last_store_purch_date"),Seq("last_store_purch_date"),"left_outer")
      val withweb_purch=withstore_purch.join(dimdate.withColumnRenamed("date_key","last_web_purch_date_key").withColumnRenamed("sdate","last_web_purch_date"),Seq("last_web_purch_date"),"left_outer")
      val with_alldatekeys=withweb_purch.join(dimdate.withColumnRenamed("date_key","account_open_date_key").withColumnRenamed("sdate","account_open_date"),Seq("account_open_date"),"left_outer")
      with_alldatekeys.join(dim_member,Seq("household_key"),"left_outer")
    }
    else{
      createDF(filePath)
    }

    val renamedDF = rawData.renameColumns(tableConfig.renameConfig)

    val transformDF = transformColumns(tableConfig.transformConfig, renamedDF)
      .persist()


    val rankFunc = Window.partitionBy(tableConfig.grpColumns.map(col): _*)
      .orderBy(tableConfig.ordColumns.map(desc): _*)

    val rankedData = transformDF.withColumn("rank", row_number() over rankFunc)

    val currentData = rankedData.filter("rank = 1")
    val historyData = rankedData.filter("rank != 1")

    //val currentDataWithDefaultValues = addDefaultValues(currentData, batch_id)
    //val historyDataWithDefaultValues = addDefaultValues(historyData, batch_id)



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

    val currentDataWithDefaultValues = currentDataWithKeys
      .withColumn("last_updated_date", current_timestamp())
      .withColumn("batch_id", lit(batch_id))
    val historyDataWithDefaultValues = historyDataWithKeys
      .withColumn("last_updated_date", current_timestamp())
      .withColumn("batch_id", lit(batch_id))


    val finalDF = currentDataWithDefaultValues.withColumn("status", lit("current"))
      .unionAll(historyDataWithDefaultValues.withColumn("status", lit("history")))

      finalDF.select(goldDF.getColumns: _*).insertIntoHive(SaveMode.Overwrite, Nil, goldTable, Some("status"), null)

  }

}
