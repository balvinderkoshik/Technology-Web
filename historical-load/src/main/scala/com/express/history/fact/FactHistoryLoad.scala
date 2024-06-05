package com.express.history.fact


import java.sql.Date

import com.express.cdw.{CDWContext, CDWOptions, strToDate}
import com.express.cdw.spark.DataFrameUtils._
import com.express.util.Settings
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, UserDefinedFunction}
import com.express.cdw.spark.udfs.strToDateUDF
import org.apache.spark.storage.StorageLevel

/**
  * Created by Mahendranadh.Dasari on 17-11-2017.
  */
object FactHistoryLoad extends CDWContext with CDWOptions {

  addOption("tableName", true)
  addOption("filePath", true)
  addOption("filePath2",false)

  hiveContext.udf.register[Date, String, String]("parse_date", strToDate)

  def transformColumns(transformations: Map[String, String], df: DataFrame): DataFrame = {
    transformations.keys.foldLeft(df) {
      (df, key) => df.withColumn(key, trim(expr(transformations(key))))
    }
  }

  def createDF(filePath: String,nullValue: String = "", header: Boolean = true, headerStr: String = ""): DataFrame ={
    //headerStr.split("|").map(_.trim).map(col => StructField(col, StringType))
    val reader = hiveContext.read.format("com.databricks.spark.csv")
      .option("header", header.toString) // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .option("delimiter", "|")
      .option("nullValue", nullValue)
      .option("treatEmptyValuesAsNulls","true")

      if(header)
       reader.load(filePath)
    else
        reader.schema(StructType(headerStr.split("\\|").map(_.trim).map(col => StructField(col, StringType))))
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

    val rawData = if(tablename.equalsIgnoreCase("fact_transaction_summary")) {
                     //val filedata=createDF(filePath).withColumn("trxn_date",strToDateUDF(col("trxn_date"),lit("MM/dd/yyyy HH:mm:ss")))
                     val filedata=createDF(filePath).withColumn("trxn_date",strToDateUDF(col("trxn_date"),lit("yyyy-MM-dd")))
                     val fact_tender_history=hiveContext.table(s"$goldDB.fact_tender_history").select(col("trxn_id"),col("fact_tender_hist_id")).groupBy(col("trxn_id")).agg(max("fact_tender_hist_id") as "max_tender_id")
                     val dim_date=hiveContext.table(s"$goldDB.dim_date").select(col("date_key"),col("sdate"),col("status")).filter("status='current'").drop(col("status"))
                     val dim_store=hiveContext.table(s"$goldDB.dim_store_master").select(col("store_key"),col("currency_code"),col("status")).filter("status='current'").drop(col("status"))
                     val dim_currency=hiveContext.table(s"$goldDB.dim_currency").select(col("currency_code"),col("currency_key"),col("status")).filter("status='current'").drop(col("status"))
                      val with_currencykeys=filedata.join(dim_store,Seq("store_key"),"left_outer").join(dim_currency,Seq("currency_code"),"left_outer").drop(col("currency_code"))
                      val with_datekeys=with_currencykeys.join(dim_date.withColumnRenamed("sdate","trxn_date").withColumnRenamed("date_key","trxn_date_key"),Seq("trxn_date"),"left_outer")
                      with_datekeys.join(fact_tender_history,Seq("trxn_id"),"left_outer")

                  }else if(tablename.equalsIgnoreCase("fact_transaction_detail")){
                      //val filedata=createDF(filePath).withColumn("trxn_date",strToDateUDF(col("trxn_date"),lit("MM/dd/yyyy HH:mm:ss")))
                      val filedata=createDF(filePath).withColumn("trxn_date",strToDateUDF(col("trxn_date"),lit("yyyy-MM-dd")))
                      val dim_date=hiveContext.table(s"$goldDB.dim_date").select(col("date_key"),col("sdate"),col("status")).filter("status='current'").drop(col("status"))
                      filedata.join(dim_date.withColumnRenamed("sdate","trxn_date").withColumnRenamed("date_key","trxn_date_key"),Seq("trxn_date"),"left_outer")
                   }else if(tablename.equalsIgnoreCase("fact_scoring_history")){
                      val filedata=createDF(filePath)
                      val dim_scoring_model_segment=hiveContext.table(s"$goldDB.dim_scoring_model_segment").select(col("scoring_model_segment_key"),col("segment_rank"),col("model_id"),col("status")).filter("model_id=104 and status='current'").drop(col("model_id")).drop(col("status"))
                      filedata.join(dim_scoring_model_segment,Seq("scoring_model_segment_key"),"left_outer")
                  }
    else if(tablename.equalsIgnoreCase("fact_tender_history")){

      val filedata=createDF(filePath, header = false, headerStr = "FACT_TENDER_HIST_ID|MEMBER_KEY|RID|TRXN_ID|TRXN_NBR|REGISTER_NBR|TRXN_TENDER_SEQ_NBR|STORE_KEY|TRXN_DATE|TRXN_DATE_KEY|DIVISION_ID|ASSOCIATE_KEY|CASHIER_ID|SALESPSERSON|ASSOCIATE_SALES_FLAG|ORIG_TRXN_NBR|REISSUE_FLAG|TENDER_AMOUNT|CHANGE_DUE|TOKENIZED_CC_NBR|TOKENIZED_CC_KEY|TENDER_TYPE_KEY|CHECK_AUTH_KEY|POST_VOID_IND|CHECK_AUTH_NBR|LOAD_ID|STAGE_ID|CREATE_DATE|UPDATE_DATE|CURRENCY_KEY|CAPTURED_LOYALTY_ID|IMPLIED_LOYALTY_ID|LBI_PAYMENT_TYPE_CODE|TENDER_TYPE_CODE|TENDER_TYPE_DESCRIPTION|IS_CASH|IS_CHECK|IS_CREDIT_CARD|IS_EXPRESS_PLCC|IS_GIFT_CARD|IS_BANK_CARD|CHECK_AUTH_KEY_2|CHECK_AUTH_ENABLE_FLAG|CHECK_SWIPED_FLAG|CHECK_AUTHORIZATION_TYPE")

      val factTenderHistoryWindow = Window.partitionBy("fact_tender_hist_id")

      val factTenderHistoryDF = filedata.withColumn("rank",row_number().over(factTenderHistoryWindow.orderBy(desc("trxn_date"))))
        .filter("rank=1")
        .drop(col("rank"))

      factTenderHistoryDF
    }
    else if(tablename.equalsIgnoreCase("fact_tier_history")){

      val filedata=createDF(filePath).withColumn("from date",strToDateUDF(col("from date"),lit("yyyyMMddHHmmss")))
                                     .withColumn("to date",strToDateUDF(col("to date"),lit("yyyyMMddHHmmss")))

      val dimDateDF = hiveContext.sql(s"select sdate,date_key from $goldDB.dim_date where status='current'")

      val dimTierDF = hiveContext.sql(s"select tier_id,tier_key from $goldDB.dim_tier where status='current'")

      val dimMemberDF = hiveContext.sql(s"select ip_code,member_key from $goldDB.dim_member where status='current'")

      val joinedfromdateWithDate = filedata.join(dimDateDF,
        filedata.col("from date") === dimDateDF.col("sdate"), "left")
        .select("date_key", filedata.columns: _*)
        .withColumnRenamed("date_key", "tier_begin_date_key")
        .drop(col("sdate"))

      val joinedtodatewithDate = joinedfromdateWithDate.join(dimDateDF,
        joinedfromdateWithDate.col("to date") === dimDateDF.col("sdate"), "left")
        .select("date_key", joinedfromdateWithDate.columns: _*)
        .withColumnRenamed("date_key", "tier_end_date_key")
        .withColumnRenamed("member id", "ipcode")
        .withColumnRenamed("FROM DATE", "tier_begin_date")
        .withColumnRenamed("TO DATE", "tier_end_date")
        .drop(col("sdate"))

       val joinedtodimTierdf = joinedtodatewithDate.join(dimTierDF,
        joinedtodatewithDate.col("tier id") === dimTierDF.col("tier_id"), "left")

      joinedtodimTierdf.join(dimMemberDF,
        joinedtodimTierdf.col("ipcode") === dimMemberDF.col("ip_code"), "left")
          .drop(col("ip_code"))
    }

    else if(tablename.equalsIgnoreCase("fact_point_history")){

      val filedata=createDF(filePath)
        //.withColumn("pointawarddate",strToDateUDF(col("pointawarddate"),lit("yyyyMMddHHmmss")))
        //.withColumn("transactiondate",strToDateUDF(col("transactiondate"),lit("yyyyMMddHHmmss")))
        //.withColumn("expirationdate",strToDateUDF(col("expirationdate"),lit("yyyyMMddHHmmss")))


      val factCardHistoryDF = hiveContext.sql(s"select * from $goldDB.fact_card_history")
        .select("vc_key",
          "member_key",
          "ip_code"
        )

      /*val factRewardHistoryDim = hiveContext.sql(s"select * from $goldDB.fact_reward_history")

      val factRewardHistoryWindow = Window.partitionBy("id")

      val factRewardHistoryDF = factRewardHistoryDim
        .withColumn("rank",row_number().over(factRewardHistoryWindow.orderBy(desc("trxn_redemption_date"),desc("api_redemption_date"),desc("reward_fulfillment_date"))))
        .filter("rank=1")
        .drop(col("rank"))
        .select("reward_history_id","id")*/

      val dimDateDF = hiveContext.sql(s"select sdate,date_key from $goldDB.dim_date where status='current'")

      val joinedCardHistoryDFWithfiledata = filedata.join(factCardHistoryDF,
        filedata.col("vckey") === factCardHistoryDF.col("vc_key"), "left")
        .drop(col("vc_key"))

      /*val joinedRewardHistoryDFWithfiledata = joinedCardHistoryDFWithfiledata.join(factRewardHistoryDF,
        joinedCardHistoryDFWithfiledata.col("pointtransactionid") === factRewardHistoryDF.col("id"), "left")
        .withColumnRenamed("reward_history_id", "associated_reward_history_id")*/

      val joinedPointawarddateWithDate = joinedCardHistoryDFWithfiledata.join(broadcast(dimDateDF),
        joinedCardHistoryDFWithfiledata.col("pointawarddate") === dimDateDF.col("sdate"), "left")
        //.select("date_key", joinedRewardHistoryDFWithfiledata.columns: _*)
        .withColumnRenamed("date_key", "points_awarded_date_key")
        .drop(col("sdate"))

      val joinedTxnDateWithDate = joinedPointawarddateWithDate.join(broadcast(dimDateDF),
        joinedPointawarddateWithDate.col("transactiondate") === dimDateDF.col("sdate"), "left")
        .select("date_key", joinedPointawarddateWithDate.columns: _*)
        .withColumnRenamed("date_key", "txn_date_key")
        .drop(col("sdate"))

      joinedTxnDateWithDate.join(broadcast(dimDateDF),
        joinedTxnDateWithDate.col("expirationdate") === dimDateDF.col("sdate"), "left")
        .select("date_key", joinedTxnDateWithDate.columns: _*)
        .withColumnRenamed("date_key", "expiration_date_key")
        .drop(col("sdate"))
    }

    else if(tablename.equalsIgnoreCase("fact_reward_trxn_history")){

      val factRewardtrxnhistory = hiveContext.sql(s"select certificate_nbr,trxn_id,trxn_date,store_id,trxn_register,trxn_nbr,cashier_id from cmhpcdw.fact_rewardtrxnhistory")

      factRewardtrxnhistory
    }
    else if(tablename.equalsIgnoreCase("fact_reward_history")){

      val filedata=createDF(filePath).withColumn("dateissued",strToDateUDF(col("dateissued"),lit("yyyyMMddHHmmss")))
        .withColumn("expiration",strToDateUDF(col("expiration"),lit("yyyyMMddHHmmss")))
        .withColumn("fulfillmentdate",strToDateUDF(col("fulfillmentdate"),lit("yyyyMMddHHmmss")))
        .withColumn("redemptiondate",strToDateUDF(col("redemptiondate"),lit("yyyyMMddHHmmss")))

      val factRewardtrxnHistory = hiveContext.sql(s"select * from $goldDB.fact_reward_trxn_history")
        .select("certificate_nbr","trxn_id","trxn_date")

      val factRewardtrxnHistoryWindow = Window.partitionBy("certificate_nbr")

      val factRewardtrxnHistoryDF = factRewardtrxnHistory .withColumn("rank",row_number().over(factRewardtrxnHistoryWindow.orderBy(desc("trxn_date"))))
        .filter("rank=1")
        .drop(col("rank"))

      val dimDateDF = hiveContext.sql(s"select sdate,date_key from $goldDB.dim_date where status='current'")

      val dimRewardDefDF = hiveContext.sql(s"select reward_def_id,reward_def_key from $goldDB.dim_reward_def where status='current'")

      val dimMemberDF = hiveContext.sql(s"select ip_code,member_key from $goldDB.dim_member where status='current'")

      val joinedRewardtrxnHistoryWithfiledata = filedata.join(factRewardtrxnHistoryDF,
        filedata.col("certificatenmbr") === factRewardtrxnHistoryDF.col("certificate_nbr"), "left")
        .select("trxn_id", filedata.columns: _*)
        .drop(col("certificate_nbr"))


      val joineddateissuedWithDate = joinedRewardtrxnHistoryWithfiledata.join(dimDateDF,
        joinedRewardtrxnHistoryWithfiledata.col("dateissued") === dimDateDF.col("sdate"), "left")
        .select("date_key", joinedRewardtrxnHistoryWithfiledata.columns: _*)
        .withColumnRenamed("date_key", "reward_issue_date_key")
        .drop(col("sdate"))

      val joinedexpirationWithDate = joineddateissuedWithDate.join(dimDateDF,
        joineddateissuedWithDate.col("expiration") === dimDateDF.col("sdate"), "left")
        .select("date_key", joineddateissuedWithDate.columns: _*)
        .withColumnRenamed("date_key", "reward_expiration_date_key")
        .drop(col("sdate"))

      val joinedfulfillmentdateWithDate = joinedexpirationWithDate.join(dimDateDF,
        joinedexpirationWithDate.col("fulfillmentdate") === dimDateDF.col("sdate"), "left")
        .select("date_key", joinedexpirationWithDate.columns: _*)
        .withColumnRenamed("date_key", "reward_fulfillment_date_key")
        .drop(col("sdate"))

      val joinedredemptiondatewithdimrewarddef = joinedfulfillmentdateWithDate.join(dimDateDF,
        joinedfulfillmentdateWithDate.col("redemptiondate") === dimDateDF.col("sdate"), "left")
        .select("date_key", joinedfulfillmentdateWithDate.columns: _*)
        .withColumnRenamed("date_key", "api_redemption_date_key")
        .drop(col("sdate"))

       val joinedwithRewardDef = joinedredemptiondatewithdimrewarddef.join(dimRewardDefDF,
            joinedredemptiondatewithdimrewarddef.col("rewarddefid") === dimRewardDefDF.col("reward_def_id"), "left")

      joinedwithRewardDef.join(dimMemberDF,
        joinedwithRewardDef.col("memberid") === dimMemberDF.col("ip_code"), "left")
        .drop(col("ip_code"))



    }
    else if(tablename.equalsIgnoreCase("fact_member_coupons")){

      val filedata=createDF(filePath)

      val dimMemberDF = hiveContext.sql(s"select ip_code,member_key from $goldDB.dim_member where status='current'")

      filedata.join(dimMemberDF,
        filedata.col("memberid") === dimMemberDF.col("ip_code"), "left")
        .drop(col("ip_code"))
    }
    else if(tablename.equalsIgnoreCase("fact_campaign_history")){

      val filedata=createDF(filePath)

      val dimMemberDF = hiveContext.sql(s"select ip_code,member_key from $goldDB.dim_member where status='current'")

      filedata.join(dimMemberDF,
        filedata.col("memberid") === dimMemberDF.col("ip_code"), "left")
        .drop(col("ip_code"))
    }
    else if(tablename.equalsIgnoreCase("fact_cs_notes_history")){

      val filedata=createDF(filePath)

      val dimMemberDF = hiveContext.sql(s"select ip_code,member_key from $goldDB.dim_member where status='current'")

      filedata.join(dimMemberDF,
        filedata.col("memberid") === dimMemberDF.col("ip_code"), "left")
        .drop(col("ip_code"))
    }
    else
      createDF(filePath)

    val renamedDF = rawData.renameColumns(tableConfig.renameConfig)
    val transformDF = transformColumns(tableConfig.transformConfig, renamedDF).persist()

    if (generateSK) {
      val dfForMaxSK=hiveContext.table(goldTable)
      val maxSurrogateKey=dfForMaxSK.maxKeyValue(col(tableConfig.surrogateKey))
      print(maxSurrogateKey)
      val finalFactDF = transformDF.drop(surrogateKey).generateSequence(maxSurrogateKey, Some(tableConfig.surrogateKey))
        .withColumn("last_updated_date", current_timestamp()).withColumn("batch_id", lit(batch_id))
        //.withColumn("consumes_point_history_id",lit(null))
      if(tablename.equalsIgnoreCase("fact_point_history")){
       // val df = finalFactDF.select(col("parent_transaction_id"),col("point_history_id"))
        // .withColumnRenamed("point_history_id","consumes_point_history_id").withColumnRenamed("parent_transaction_id","point_transaction_id")
        //finalFactDF.repartition(1000)
       /*val finalFactDFWindow = Window.partitionBy("point_transaction_id")
       val df = finalFactDF
         .filter("point_transaction_id<>-1")
         .withColumn("rank",row_number().over(finalFactDFWindow.orderBy(desc("txn_date"),desc("point_history_id"))))
         .filter("rank=1")
         .select(col("point_transaction_id"),col("point_history_id"))
         .withColumnRenamed("point_history_id","consumes_point_history_id")
         .withColumnRenamed("point_transaction_id","parent_transaction_id")
        val finaldf=finalFactDF.join(df,Seq("parent_transaction_id"),"left_outer")
        finaldf.repartition(1000)*/

        finalFactDF.select(goldDF.getColumns: _*).write.mode(SaveMode.Append).insertInto(goldTable)
          /*.withColumn("consumes_point_history_id",expr("CASE WHEN parent_transaction_id = -1 THEN null ELSE consumes_point_history_id END"))
          .select(goldDF.getColumns: _*).write.mode(SaveMode.Append).insertInto(goldTable)*/
      }
      else {
        finalFactDF.select(goldDF.getColumns: _*).write.mode(SaveMode.Append).insertInto(goldTable)
      }
        //.insertIntoHive(SaveMode.Overwrite, Nil, goldTable, None, null)

    } else {

      if(tablename.equalsIgnoreCase("fact_card_history")){

        val factDF = transformDF.withColumn("last_updated_date", current_timestamp()).withColumn("batch_id", lit(batch_id))
        val finalFactDF = factDF.withColumn("status", lit("updated"))

        //finalFactDF.select(goldDF.getColumns: _*).insertIntoHive(SaveMode.Overwrite, Nil, goldTable, Some("status"), null)
          finalFactDF.select(goldDF.getColumns: _*).write.partitionBy("status").mode(SaveMode.Append).insertInto(goldTable)
      } else {
        transformDF.persist(StorageLevel.MEMORY_AND_DISK_2)
        transformDF.withColumn("last_updated_date", current_timestamp()).withColumn("batch_id", lit(batch_id))
          .select(goldDF.getColumns: _*).write.mode(SaveMode.Append).insertInto(goldTable)
          //.insertIntoHive(SaveMode.Overwrite, Nil, goldTable, None, null)
      }

    }

  }

}
