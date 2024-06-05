package com.express.history.dim

import java.sql.Date

import com.express.cdw.{CDWContext, CDWOptions, strToDate}
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs.strToDateUDF
import com.express.history.dim.DimHistoryLoad.{createDF, goldDB, hiveContext}
import com.express.util.Settings
import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, UserDefinedFunction}
import org.apache.spark.storage.StorageLevel
/**
  * Created by Mahendranadh.Dasari on 26-10-2017.
  */
object DimNoDefaultValues extends CDWContext with CDWOptions {

  addOption("tableName", true)
  addOption("filePath", true)
  addOption("filePath2",false)

  hiveContext.udf.register[Date, String, String]("parse_date", strToDate)

  val arrayToString :UserDefinedFunction = udf((associatedMember: Array[StringType]) => associatedMember.toString.mkString(","))

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
//      .option("escape", "")
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
    val doNotIncrSK = tableConfig.doNotIncrSK


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
        .withColumn("phone_type", when(isnull(col("consent_status")),lit("UNKNOWN")).otherwise(lit("MOBILE")))

    }else if(tablename.equalsIgnoreCase("dim_member_multi_email")){
      /*val df1=createDF(filePath)
      //val df2=hiveContext.table("cmhpcdw.adobe_mmeid_mapping").withColumnRenamed("email","email_address")
      val adobe_mmeid_temp = "select distinct member_key, email, mme_id from cmhpcdw.adobe_mmeid_mapping"
      val df2 = hiveContext.sql(adobe_mmeid_temp).withColumnRenamed("email","email_address")
      df1.join(df2,Seq("member_key","email_address"),"left_outer")*/
      val mme_file = hiveContext.sql(s"select a.*,b.email,b.mme_id, row_number() over (partition by a.member_key,a.email_address order by b.ingest_date desc) as rnk  from work.temp_mme a left join cmhpcdw.adobe_mmeid_mapping b on a.member_key=b.member_key and a.EMAIL_ADDRESS=b.email")

      mme_file.filter("rnk = 1")

    }else if(tablename.equalsIgnoreCase("dim_member_consent")){
      val df1=createDF(filePath)
      val memberConsent = df1.filter("consent_type='DM'")

      val dimMemberDF = hiveContext.sql(s"select loyalty_id,member_key from $goldDB.dim_member where status='current'")

      memberConsent.join(dimMemberDF,Seq("member_key"),"left_outer")

    }else if(tablename.equalsIgnoreCase("dim_household")){
      val filedata=createDF(filePath)
        .withColumn("first_trxn_date",strToDateUDF(col("first_trxn_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("add_date",strToDateUDF(col("add_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("introduction_date",strToDateUDF(col("introduction_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("last_store_purch_date",strToDateUDF(col("last_store_purch_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("last_web_purch_date",strToDateUDF(col("last_web_purch_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("account_open_date",strToDateUDF(col("account_open_date"),lit("MM/dd/yyyy HH:mm:ss")))
      val dimdate=hiveContext.table(s"$goldDB.dim_date").select(col("date_key"),col("sdate"),col("status")).filter("status='current'").drop(col("status")).persist
      val dim_member=hiveContext.table(s"$goldDB.dim_member")
        .filter("status='current' and household_key<>-1 and household_key is not null")
        .select(col("member_key"),col("household_key"),col("status"))
        .drop(col("status"))
        .groupBy(col("household_key"))
        .agg(collect_set(col("member_key").cast("String")) as "associated_member_key")
        .persist()

      //.withColumn("associated_member_key",concat_ws(",", col("associated_member_key").cast(StringType)))
      //.agg(concat_ws(",",collect_set(col("member_key").cast(StringType))) as "associated_member_key")

      val withfirst_date=filedata.join(broadcast(dimdate).withColumnRenamed("date_key","first_trxn_date_key").withColumnRenamed("sdate","first_trxn_date"),Seq("first_trxn_date"),"left_outer")
      val withadd_date=withfirst_date.join(broadcast(dimdate).withColumnRenamed("date_key","add_date_key").withColumnRenamed("sdate","add_date"),Seq("add_date"),"left_outer")
      val withintro_date=withadd_date.join(broadcast(dimdate).withColumnRenamed("date_key","introduction_date_key").withColumnRenamed("sdate","introduction_date"),Seq("introduction_date"),"left_outer")
      val withstore_purch=withintro_date.join(broadcast(dimdate).withColumnRenamed("date_key","last_store_purch_date_key").withColumnRenamed("sdate","last_store_purch_date"),Seq("last_store_purch_date"),"left_outer")
      val withweb_purch=withstore_purch.join(broadcast(dimdate).withColumnRenamed("date_key","last_web_purch_date_key").withColumnRenamed("sdate","last_web_purch_date"),Seq("last_web_purch_date"),"left_outer")
      val with_alldatekeys=withweb_purch.join(broadcast(dimdate).withColumnRenamed("date_key","account_open_date_key").withColumnRenamed("sdate","account_open_date"),Seq("account_open_date"),"left_outer")
      val householdDf = with_alldatekeys.join(dim_member,Seq("household_key"),"left_outer")
      householdDf.withColumn("associated_member_key",concat_ws(",", col("associated_member_key")))

    }else if(tablename.equalsIgnoreCase("dim_member")){
        val filedata=createDF(filePath)
        .withColumn("member_acct_open_date",strToDateUDF(col("member_acct_open_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("member_acct_close_date",strToDateUDF(col("member_acct_close_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("member_acct_enroll_date",strToDateUDF(col("member_acct_enroll_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("printed_card_req_date",strToDateUDF(col("printed_card_req_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("printed_cert_opt_in_date",strToDateUDF(col("printed_cert_opt_in_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("e_cert_opt_in_date",strToDateUDF(col("e_cert_opt_in_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("customer_add_date",strToDateUDF(col("customer_add_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("customer_introduction_date",strToDateUDF(col("customer_introduction_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("first_trxn_date",strToDateUDF(col("first_trxn_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("last_store_purch_date",strToDateUDF(col("last_store_purch_date"),lit("MM/dd/yyyy HH:mm:ss")))
        .withColumn("last_web_purch_date",strToDateUDF(col("last_web_purch_date"),lit("MM/dd/yyyy HH:mm:ss")))
      val dimdate=hiveContext.table(s"$goldDB.dim_date").select(col("date_key"),col("sdate"),col("status")).filter("status='current'").drop(col("status")).persist()
      filedata.updateKeys(Seq("member_acct_open_date", "member_acct_close_date", "member_acct_enroll_date", "last_store_purch_date",
      "last_web_purch_date", "printed_card_req_date", "printed_cert_opt_in_date", "e_cert_opt_in_date",
      "customer_add_date", "customer_introduction_date", "first_trxn_date"), dimdate, "sdate", "date_key")

        /*
        .join(broadcast(dimdate).withColumnRenamed("date_key","member_acct_open_date_key").withColumnRenamed("sdate","member_acct_open_date"),Seq("member_acct_open_date"),"left_outer")
        .join(broadcast(dimdate).withColumnRenamed("date_key","member_acct_close_date_key").withColumnRenamed("sdate","member_acct_close_date"),Seq("member_acct_close_date"),"left_outer")
        .join(broadcast(dimdate).withColumnRenamed("date_key","member_acct_enroll_date_key").withColumnRenamed("sdate","member_acct_enroll_date"),Seq("member_acct_enroll_date"),"left_outer")
        .join(broadcast(dimdate).withColumnRenamed("date_key","last_store_purch_date_key").withColumnRenamed("sdate","last_store_purch_date"),Seq("last_store_purch_date"),"left_outer")
        .join(broadcast(dimdate).withColumnRenamed("date_key","last_web_purch_date_key").withColumnRenamed("sdate","last_web_purch_date"),Seq("last_web_purch_date"),"left_outer")
        .join(broadcast(dimdate).withColumnRenamed("date_key","printed_card_req_date_key").withColumnRenamed("sdate","printed_card_req_date"),Seq("printed_card_req_date"),"left_outer")
        .join(broadcast(dimdate).withColumnRenamed("date_key","printed_cert_opt_in_date_key").withColumnRenamed("sdate","printed_cert_opt_in_date"),Seq("printed_cert_opt_in_date"),"left_outer")
        .join(broadcast(dimdate).withColumnRenamed("date_key","e_cert_opt_in_date_key").withColumnRenamed("sdate","e_cert_opt_in_date"),Seq("e_cert_opt_in_date"),"left_outer")
        .join(broadcast(dimdate).withColumnRenamed("date_key","customer_add_date_key").withColumnRenamed("sdate","customer_add_date"),Seq("customer_add_date"),"left_outer")
        .join(broadcast(dimdate).withColumnRenamed("date_key","customer_introduction_date_key").withColumnRenamed("sdate","customer_introduction_date"),Seq("customer_introduction_date"),"left_outer")
        .join(broadcast(dimdate).withColumnRenamed("date_key","first_trxn_date_key").withColumnRenamed("sdate","first_trxn_date"),Seq("first_trxn_date"),"left_outer")*/
    }
    /*else if(tablename.equalsIgnoreCase("dim_scoring_model")){

      val dimScoringModel = hiveContext.sql(s"select scoring_model_key,model_id,model_version_id,model_source,marketing_brand_model_code,model_description,model_level_code,model_level,lbi_segment_processed_flag,scored_record_count,current_model_ind,effective_date,expiration_date,data_through_date from cmhpcdw.dim_scoringmodel")
       dimScoringModel
    }*/
    else if(tablename.equalsIgnoreCase("dim_employee")){
      val filedata=createDF(filePath)
        .withColumn("employee_id",lpad(col("employee_id"),11,"0"))
      filedata
    }
    else{
      createDF(filePath)
    }

    val renamedDF = rawData.renameColumns(tableConfig.renameConfig)

    val transformDF = transformColumns(tableConfig.transformConfig, renamedDF)

    val rankFunc = Window.partitionBy(tableConfig.grpColumns.map(col): _*)
      .orderBy(tableConfig.ordColumns.map(desc): _*)

    val rankedData = transformDF.withColumn("rank", row_number() over rankFunc)
    rankedData.repartition(2000).persist()

    val currentData = rankedData.filter("rank = 1")
    val historyData = rankedData.filter("rank != 1")


    val (currentDataWithKeys, historyDataWithKeys) = if (generateSK && doNotIncrSK ) {
      val dfForMaxSK = hiveContext.table(goldTable).filter("status='history'")
      val maxSurrogateKey = dfForMaxSK.maxKeyValue(col(tableConfig.surrogateKey))
      val skGeneratedData = rankedData.groupByAsList(tableConfig.grpColumns)
        .generateSequence(maxSurrogateKey, Some(tableConfig.surrogateKey))
        .withColumn("data", explode(col("grouped_data")))
        .unstruct("data", rankedData.schema)
      val currentData = skGeneratedData.filter("rank = 1")
      val historyData = skGeneratedData.filter("rank != 1")
      (currentData, historyData.unionAll(currentData))
    }
    else if (generateSK) {
      val dfForMaxSK=hiveContext.table(goldTable).filter("status='history'")
      val maxSurrogateKey=dfForMaxSK.maxKeyValue(col(tableConfig.surrogateKey))
      val historyDataWithKeys = historyData.drop(surrogateKey).generateSequence(maxSurrogateKey, Some(tableConfig.surrogateKey))
      val maxHistoryCnt=if(historyData.count()==0){
        maxSurrogateKey
      }else {
        historyDataWithKeys.maxKeyValue(col(tableConfig.surrogateKey))
      }
      val currentDataWithKeys = currentData.drop(surrogateKey).generateSequence(maxHistoryCnt, Some(tableConfig.surrogateKey))
      (currentDataWithKeys, historyDataWithKeys.unionAll(currentDataWithKeys))
    } else {
      val (defaultValuesDF, totalValuesDF) = rankedData.partition(col(surrogateKey) === "-1" or col(surrogateKey) === "0")

      val currentData = totalValuesDF.filter("rank = 1")
      val historyData = totalValuesDF.filter("rank  != 1")
      (currentData.castColumns(Seq(surrogateKey), LongType)
        , historyData.unionAll(currentData).castColumns(Seq(surrogateKey), LongType))
    }
    /*
    val currentDataWithDefaultValues = addDefaultValues(currentDataWithKeys, batch_id)
    val historyDataWithDefaultValues = addDefaultValues(historyDataWithKeys, batch_id)
*/

    val currentDataWithDefaultValues = currentDataWithKeys
      .withColumn("last_updated_date", current_timestamp())
      .withColumn("batch_id", lit(batch_id))
      .persist()

    val historyDataWithDefaultValues = historyDataWithKeys
      .withColumn("last_updated_date", current_timestamp())
      .withColumn("batch_id", lit(batch_id))
      .persist()

    val finalDF = currentDataWithDefaultValues.withColumn("status", lit("current"))
      .unionAll(historyDataWithDefaultValues.withColumn("status", lit("history")))

    finalDF.repartition(2000)
    //finalDF.select(goldDF.getColumns: _*).insertIntoHive(SaveMode.Overwrite, Nil, goldTable, Some("status"), null)
    finalDF.select(goldDF.getColumns: _*).write.partitionBy("status").mode(SaveMode.Append).insertInto(goldTable)

//      .select(goldDF.getColumns: _*).write.mode(SaveMode.Append).insertInto(goldTable)
  }

}
