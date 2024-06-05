package com.express.processing.dim.history

import com.express.cdw.Settings
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
/**
  * Created by neha.mahajan on 7/24/2017.
  */
object HouseholdHistoryLoad extends  LazyLogging{

  def main (args: Array[String]): Unit = {

    val gold_db = Settings.getGoldDB
    val hist_db = args(0)
    val batch_id = args(1)
    val from_date = args(2)
    val to_date = args(3)
    val household_id = "household_id"

    val sparkConf = Settings.sparkConf
    val sparkContext = new SparkContext(sparkConf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)

    logger.info("Reading " + hist_db + ".dim_household data from cmhdcd layer")

    val dimDateDF = hiveContext.sql("select sdate,date_key from " + hist_db + ".dim_date").persist()
    val dimHouseholdDF = hiveContext.sql("select * from " + hist_db + ".dim_household where ingest_date BETWEEN '"+from_date+"' AND '"+to_date+"'")

    println("=========dimHousehold table records================\n" + dimHouseholdDF.schema.toString)

    val joinOnFirstTrxnDate = dimHouseholdDF.join(dimDateDF,dimDateDF.col("sdate") === dimHouseholdDF.col("first_trxn_date"),"left")
                                            .withColumn("first_trxn_date_key",when(isnull(col("first_trxn_date")), -1).otherwise(col("date_key")))
                                            .drop(col("date_key"))
                                            .drop(col("sdate"))
                                            .drop(col("status"))

    val joinOnAddDate = joinOnFirstTrxnDate.join(dimDateDF,dimDateDF.col("sdate") === joinOnFirstTrxnDate.col("add_date"),"left")
                                            .withColumn("add_date_key",when(isnull(col("add_date")), -1).otherwise(dimDateDF.col("date_key")))
                                            .drop(col("date_key"))
                                            .drop(col("sdate"))

    val joinOnIntroductionDate = joinOnAddDate.join(dimDateDF,dimDateDF.col("sdate") === joinOnAddDate.col("introduction_date"),"left")
                                               .withColumn("introduction_date_key",when(isnull(col("introduction_date")), -1).otherwise(col("date_key")))
                                               .drop(col("date_key"))
                                               .drop(col("sdate"))

    val joinOnLastStorePurchaseDate = joinOnIntroductionDate.join(dimDateDF,dimDateDF.col("sdate") === joinOnIntroductionDate.col("last_store_purch_date"),"left")
                                                            .withColumn("last_store_purch_date_key",when(isnull(col("last_store_purch_date")), -1).otherwise(col("date_key")))
                                                            .drop(col("date_key"))
                                                            .drop(col("sdate"))

    val joinOnLastWebPurchaseDate = joinOnLastStorePurchaseDate.join(dimDateDF,dimDateDF.col("sdate") === joinOnLastStorePurchaseDate.col("last_web_purch_date"),"left")
                                                               .withColumn("last_web_purch_date_key",when(isnull(col("last_web_purch_date")), -1).otherwise(col("date_key")))
                                                                .drop(col("date_key"))
                                                                .drop(col("sdate"))

    val joinOnAccountOpenDate = joinOnLastWebPurchaseDate.join(dimDateDF,dimDateDF.col("sdate") === joinOnLastWebPurchaseDate.col("account_open_date"),"left")
                                          .withColumn("account_open_date_key",when(isnull(col("account_open_date")), -1).otherwise(col("date_key")))
                                         .select("household_key","member_count","first_trxn_date_key","first_trxn_date","is_express_plcc","add_date_key","add_date",
                                           "introduction_date_key","introduction_date","last_store_purch_date_key","last_store_purch_date","last_web_purch_date_key",
                                           "last_web_purch_date","account_open_date_key","account_open_date","zip_code","scoring_model_key",
                                           "scoring_model_segment_key","valid_address","record_type","ingest_date")

    val  generateSK = joinOnAccountOpenDate.rdd
                                           .zipWithIndex()
                                           .map { case (row, id) => Row.fromSeq((id +  1) +: row.toSeq) }

    val generateSKSchema = StructType(StructField(household_id,LongType,nullable = false) +: joinOnAccountOpenDate.schema)
    val finalInsertRecords = joinOnAccountOpenDate.sqlContext.createDataFrame(generateSK,generateSKSchema)

    val groupByHouseholdKey = finalInsertRecords.groupBy("household_key")
                                       .agg(max("ingest_date").alias("ingest_date_gb"))
                                         .withColumnRenamed("household_key","household_key_gb")

    val finalCurrentPartitionData = groupByHouseholdKey.join(finalInsertRecords,groupByHouseholdKey.col("household_key_gb") === finalInsertRecords.col("household_key")
                                      && groupByHouseholdKey.col("ingest_date_gb") === finalInsertRecords.col("ingest_date"),"left")
                                  .drop("household_key_gb")
                                  .drop("ingest_date_gb")
                                  .drop("ingest_date")
                                  .withColumn("associated_member_key",lit(null))
                                  .withColumn("last_updated_date",current_timestamp())
                                  .withColumn("batch_id",lit(batch_id))

    finalCurrentPartitionData.registerTempTable("currentPartition_table")
    logger.info("Current Partition Record Count :: " + finalCurrentPartitionData.count())

    val finalHistoryPartitionData = finalInsertRecords.drop("ingest_date")
                                     .withColumn("associated_member_key",lit(null))
                                     .withColumn("last_updated_date",current_timestamp())
                                     .withColumn("batch_id",lit(batch_id))

    finalHistoryPartitionData.registerTempTable("historyPartition_table")
    logger.info("History Partition Record Count :: " + finalHistoryPartitionData.count())

    hiveContext.sql("insert overwrite table "+gold_db+".dim_household  partition(status='current') select * from currentPartition_table")
    println("current partition data loaded successfully")

    hiveContext.sql("insert overwrite table "+gold_db+".dim_household partition(status='history') select * from historyPartition_table")
    println("history partition data loaded successfully")



  }

}
