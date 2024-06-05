package com.express.processing.enrichment

import com.express.cdw.Settings
import com.express.cdw.spark.DataFrameUtils._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SaveMode, sources}
import org.apache.spark.sql.expressions._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions._


/**
  * Created by poonam.mishra on 7/21/2017.
  */
object EnrichHousehold extends LazyLogging{
  //Configurations
  val conf = Settings.sparkConf
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  val workDB = Settings.getWorkDB
  val goldDB = Settings.getGoldDB
  val outputTempTable = hiveContext.table(s"$workDB.dim_household")
  val targetCols = outputTempTable.columns

  def main(args: Array[String]): Unit = {
    System.setProperty("hive.exec.dynamic.partition.mode", "nonstrict")
    import sqlContext.implicits._
    val batch_id = args {0}

    //Aliases created for various input tables
    val Member = "dimMember"
    val Household = "dimHouseHold"
    val MemberWithHouseholds = "joinWithMember"


    //Read tables that are required for Enrichment
    val dimHouseHold = hiveContext.sql(s"select * from $goldDB.dim_household where status ='current' and household_id not in (0,-1)").alias(Household)
    val dimMember = hiveContext.sql(s"select * from $goldDB.dim_member where status ='current'").filter("household_key != -1").alias(Member)
    val factScoringHistory = hiveContext.sql(s"select household_key,min(segment_rank)"+
      s" as segment_rank from $goldDB.fact_scoring_history where segment_rank is not null and member_key is not null group by household_key")
    val dimScoreModelSeg = hiveContext.sql(s"select scoring_model_segment_key,segment_rank,model_id from $goldDB.dim_scoring_model_segment where"+
      s" model_id=108 and segment_in_use = 'YES' and status='current' and segment_grouping_rank is not null")
      .join(hiveContext.sql(s"select scoring_model_key,model_id from $goldDB.dim_scoring_model where model_id = 108 and status='current'"),Seq("model_id"),"left")

    val joinFactDimScoreModelSeg = factScoringHistory.join(dimScoreModelSeg,Seq("segment_rank"),"left")
      .withColumnRenamed("scoring_model_segment_key","fact_scoring_model_segment_key")
      .withColumnRenamed("scoring_model_key","fact_scoring_model_key")
      .select("household_key","fact_scoring_model_segment_key","fact_scoring_model_key")


    logger.info("Enrichment: Audience classification based on record_type")
    val grpHouseholdKeyRecType = dimMember.select("household_key","record_type").distinct()


    val grpHouseholdKey = grpHouseholdKeyRecType.select("household_key","record_type")
      .withColumn("count",count("record_type").over(org.apache.spark.sql.expressions.Window.partitionBy("household_key")))
      .withColumnRenamed("record_type","record_type_member")

    val singleRecType = grpHouseholdKey.filter("count = 1")
    val multiRecType  = grpHouseholdKey.filter("count > 1").withColumn("record_type_member",lit("Unknown")).distinct
    val finalRecType = singleRecType.unionAll(multiRecType)

    val enrichedAudClass = dimHouseHold.join(finalRecType,Seq("household_key"),"left")
      .withColumn("action_flag",lit("U")).withColumn("record_type",when(col("count") === 1,col("record_type_member")).otherwise(lit(null)))
      .withColumn("last_updated_date",current_timestamp()).drop("count")



    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    logger.info("Enrichment: Identification of Segment Rank for Each household")

    val enrichedSegmentRank = enrichedAudClass.join(joinFactDimScoreModelSeg,Seq("household_key"),"left")
      .withColumn("action_flag",when(col("scoring_model_segment_key") === col("fact_scoring_model_segment_key"),lit("NCD")).otherwise(lit("U")))
      .withColumn("scoring_model_segment_key",col("fact_scoring_model_segment_key"))
      .withColumn("batch_id",lit(batch_id))
      .withColumn("scoring_model_key",col("fact_scoring_model_key")).withColumn("process",lit("enrich"))
      .select(targetCols.head,targetCols.tail:_*)
      .sort("household_key")
      .coalesce(Settings.getCoalescePartitions)

    enrichedSegmentRank.insertIntoHive(SaveMode.Overwrite, s"$workDB.dim_household", Some("process"), null)

  }

}