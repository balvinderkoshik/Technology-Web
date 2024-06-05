package com.express.processing.transform

import com.express.cdw.CustomerMatchFileType._
import com.express.cdw.Settings
import com.express.util.Settings._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DateType, LongType}
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import com.express.cdw.spark.udfs._
import org.apache.spark.sql.expressions.Window

/**
  * Created by bhautik.patel on 11/07/17.
  */


class TransformMemberConsent (sourceDF: DataFrame, cmFileType: String) extends LazyLogging{

  import TransformMemberConsent._
  import sourceDF.sqlContext.implicits._


  val consentColumnList = Seq("member_key_cm","rid_cm","consent_date_cm","consent_type_cm","consent_subtype_cm","consent_value_cm",
    "is_loyalty_cm","source_key_cm","original_source_key_cm")




  val batch_id = sourceDF.select("batch_id").take(1).head.getAs[String]("batch_id")


  def populateNonEmptyColumn(column1: Column , column2: Column ) : Column = {
    when((not(isnull(column1))) && CheckNotEmptyUDF(column1),column1).otherwise( column2)
  }


  def generateSourceData() : DataFrame = {

    val dimMemberDF =sqlContext.sql(s"select member_key,loyalty_id,is_loyalty_member from $lookupDB.dim_member where status='current'")
    val sourceDataColumnExpr = com.express.util.Settings.getMemberConsentTransformColumnMapping(cmFileType)
    val sourceDFjoinMember=sourceDF.join(dimMemberDF,Seq("member_key"),"left")
    val dimMemberConsentDF = sqlContext.table(s"$lookupDB.dim_member_consent").where("status = 'current' and consent_type = 'DM'")
                                       .withColumn("rank", row_number() over Window.partitionBy("member_key").orderBy(desc("consent_date")))
                                       .filter("rank = 1")
                                       .select("member_key", "consent_date", "consent_value")

    val joinedDFMemberConsent = sourceDFjoinMember.join(dimMemberConsentDF,Seq("member_key"),"left")
                                     .withColumnRenamed("consent_value","dim_consent_value")
                                     .withColumnRenamed("consent_date","dim_consent_date")

    //Generate source columns
    val sourceData  =joinedDFMemberConsent.withColumn("consent_history_id_cm", expr(sourceDataColumnExpr("CONSENT_HISTORY_ID")))
      .withColumn("member_key_cm", expr(sourceDataColumnExpr("MEMBER_KEY")))
      .withColumn("rid_cm", expr(sourceDataColumnExpr("RID")))
      .withColumn("consent_date_cm", expr(sourceDataColumnExpr("CONSENT_DATE")).cast(DateType))
      .withColumn("consent_type_cm", expr(sourceDataColumnExpr("CONSENT_TYPE")))
      .withColumn("consent_subtype_cm", expr(sourceDataColumnExpr("CONSENT_SUBTYPE")))
      .withColumn("consent_value_cm", expr(sourceDataColumnExpr("CONSENT_VALUE")))
      .withColumn("is_loyalty_cm",when(upper(trim(dimMemberDF.col("is_loyalty_member"))) === "YES" or ((trim(dimMemberDF.col("loyalty_id")) !== "") and not(isnull(trim(dimMemberDF.col("loyalty_id"))))),lit("YES")).otherwise(lit("NO")))
      .withColumn("source_key_cm", expr(sourceDataColumnExpr("SOURCE_KEY")).cast(LongType))
      .withColumn("original_source_key_cm", expr(sourceDataColumnExpr("SOURCE_KEY")).cast(LongType))
      .select("consent_history_id_cm",consentColumnList: _*)

    // to handle tiebreaker condition
    val sourceDedupData = sourceData.withColumn("rank",row_number().over(org.apache.spark.sql.expressions.Window.partitionBy("member_key_cm").orderBy(desc("consent_date_cm")))).filter("rank=1").drop("rank")



    val targetDataDup = sqlContext.table(lookupDB + ".dim_member_consent").filter("status='current'").withColumn("rank",row_number().over(org.apache.spark.sql.expressions.Window.partitionBy("member_key").orderBy(desc("consent_date"))))
    val targetData = targetDataDup.filter("rank=1").drop("rank")
    // Join to find Insert Update
    val joinedDF = sourceDedupData.join(targetData,sourceDedupData("member_key_cm") === targetData(naturalKeyCol)
      //&& (trim(lower(sourceDedupData("consent_type_cm"))) === trim(lower(targetData("consent_type"))))
      //&& (trim(lower(sourceDedupData("consent_subtype_cm"))) === trim(lower(targetData("consent_subtype"))))
      ,"left")


    // union indert and update Dataframe
    findInsertUpdate(joinedDF)


  }



  def findInsertUpdate(joinedDF : DataFrame) : DataFrame = {

    val joinedExistingRecordDF = joinedDF.filter((not(isnull(col(naturalKeyCol)))))


    // Insert new email_address
    val insertDF = joinedDF.filter((isnull(col(naturalKeyCol))))
      .select("consent_history_id_cm", consentColumnList: _*)
      .withColumn("action_cd", lit("I"))
      .withColumn("last_updated_date", current_timestamp)
      .withColumn("batch_id", lit(batch_id))
      .withColumn("process", lit(cmFileType))


    val joinedMD5 = generateMD5(joinedExistingRecordDF)

    val updateData = joinedMD5.filter(col("md5_source") !== col("md5_target"))

    logger.info("Total changed consent values are :---->" + updateData.filter(lower(trim(col("consent_value_cm"))) !== lower(trim(col("consent_value")))).count())

    val updateDF = updateData
      .withColumn("rid_update",populateNonEmptyColumn(col("rid_cm"),col("rid")))
      .withColumn("consent_date_update", when((trim(col("consent_value_cm")) !== "") and (not(isnull(col("consent_value_cm"))))
        and (lower(trim(col("consent_value_cm"))) !== lower(trim(col("consent_value")))), populateNonEmptyColumn(col("consent_date_cm"),col("consent_date")))
        .otherwise(col("consent_date")))
      .withColumn("consent_type_update",populateNonEmptyColumn(col("consent_type_cm"),col("consent_type")))
      .withColumn("consent_subtype_update",populateNonEmptyColumn(col("consent_subtype_cm"),col("consent_subtype")))
      .withColumn("consent_value_update",populateNonEmptyColumn(col("consent_value_cm"),col("consent_value")))
      .withColumn("is_loyalty_update",populateNonEmptyColumn(col("is_loyalty_cm"),col("is_loyalty")))
      .withColumn("source_key_update",populateNonEmptyColumn(col("source_key_cm"),col("source_key")))
      .select("consent_history_id_cm","member_key_cm","rid_update","consent_date_update","consent_type_update","consent_subtype_update",
        "consent_value_update","is_loyalty_update","source_key_update","original_source_key_cm")
      .withColumn("action_cd", lit("U"))
      .withColumn("last_updated_date", current_timestamp)
      .withColumn("batch_id", lit(batch_id))
      .withColumn("process", lit(cmFileType))

    insertDF.unionAll(updateDF)

  }

  def generateMD5(sourceDF: DataFrame) : DataFrame={
    val concate_Source = concat_ws("",
      sourceDF.col("rid_cm"),
      sourceDF.col("consent_date_cm"),
      lower(trim(sourceDF.col("consent_value_cm"))),
      lower(trim(sourceDF.col("is_loyalty_cm"))),
      sourceDF.col("source_key_cm"),
      sourceDF.col("original_source_key_cm")
    )
    val concate_target = concat_ws("",
      sourceDF.col("rid"),
      sourceDF.col("consent_date"),
      lower(trim(sourceDF.col("consent_value"))),
      lower(trim(sourceDF.col("is_loyalty"))),
      sourceDF.col("source_key"),
      sourceDF.col("original_source_key")
    )
    sourceDF.withColumn("md5_source",md5(concate_Source)).withColumn("md5_target",md5(concate_target))
  }

}

object  TransformMemberConsent {

  val workDB: String = Settings.getWorkDB
  val lookupDB: String = Settings.getGoldDB
  val sparkConf = Settings.sparkConf
  val coalescePartitionNum = Settings.getCoalescePartitions
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new HiveContext(sparkContext)
  sqlContext.setConf("hive.exec.dynamic.partition", "true")
  sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

  val targetData = sqlContext.table(lookupDB + ".dim_member_consent")
  val naturalKeyCol = "member_key"
  val TraansformCMResultTable = "dim_member_consent"


  def main(args: Array[String]): Unit = {

    if (args.length < 1)
      throw new Exception("Please provide Customer matching file type as argument")
    val cmFileType = args(0).toLowerCase.trim

    val sourceColumnList = Seq("consent_history_id","member_key","rid","consent_date","consent_type","consent_subtype","consent_value","is_loyalty","source_key","original_source_key")



    val sourceTable = if (cmFileType == "dma") {
      sqlContext.table(s"$workDB.work_mps_dataquality")
    }
    else
    {
      if (cmFileType == "bp_emdmconsent") {
        sqlContext.table(s"$workDB.work_${cmFileType}_dataquality").filter("channel = '2'")
          .withColumnRenamed("memberkey","member_key")
          .withColumnRenamed("emailaddress","email_address")
      }
      else {
        sqlContext.table(s"$workDB.work_${cmFileType}_cm")
      }
    }


    val transformMemberObj = new TransformMemberConsent(sourceTable, cmFileType)
    // Load the source: work data
    val sourceData = transformMemberObj.generateSourceData()

/*
    val targetDataDup = sqlContext.table(lookupDB + ".dim_member_consent").filter("status='current'").withColumn("rank",row_number().over(org.apache.spark.sql.expressions.Window.partitionBy("member_key").orderBy(desc("consent_value"))))
    val targetData = targetDataDup.filter("rank=1").drop("rank")
    // Join to find Insert Update
    val joinedDF = sourceData.join(targetData,(trim(lower(sourceData("member_key_cm"))) === trim(lower(targetData(naturalKeyCol)))) &&
      (trim(lower(sourceData("consent_type_cm"))) === trim(lower(targetData("consent_type"))))
      && (trim(lower(sourceData("consent_subtype_cm"))) === trim(lower(targetData("consent_subtype"))))
      ,"left")


    // union indert and update Dataframe
    val unionedDF = transformMemberObj.findInsertUpdate(joinedDF)

    */
    // insert into temporary table
    sourceData.write.mode(SaveMode.Overwrite).partitionBy("process").insertInto(workDB +"."+ TraansformCMResultTable)

  }
}
