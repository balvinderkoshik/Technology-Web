package com.express.processing.transform

import com.express.cdw.CDWContext
import com.express.cdw.ThirdPartyType._
import com.express.cdw.spark.DataFrameUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Created by poonam.mishra on 10/26/2017.
  */

object TransformRea extends CDWContext with LazyLogging {

  val tempDimMemberTable = s"$workDB.dim_member"
  val tempDimMemberDF : DataFrame = hiveContext.table(s"$workDB.dim_member")
  val tempDimMmeTable = s"$workDB.dim_member_multi_email"
  val tempDimMmeDF : DataFrame = hiveContext.table(s"$workDB.dim_member_multi_email")

  def bestRecordForUpdate(emailData: DataFrame): DataFrame = {

    import emailData.sqlContext.implicits._

    val bestRecordForUpdate = emailData
      .withColumn("function_rank", when($"is_loyalty_email" === "YES", lit(1))
        .when($"email_consent" === "YES", lit(2))
        .when($"best_email_flag" === "YES", lit(3))
        .otherwise(lit(99)))
      .withColumn("best_rec_for_update", row_number() over Window.partitionBy("member_key").orderBy(asc("function_rank")))
      .filter("best_rec_for_update = 1")
      .drop(col("function_rank"))
      .drop(col("best_rec_for_update"))

    bestRecordForUpdate

  }

  def main(args: Array[String]): Unit = {
    import hiveContext.implicits._

    if (args.length < 1)
      throw new Exception("Please provide Third party type as argument")
    val process_file = args(0).trim.toLowerCase

    /*Fetching data from gold.dim_member_multi_email table,Renaming email_address and member_key to align with Transform MME standards*/
    val memberMultiEmailDF = hiveContext.table(s"$goldDB.dim_member_multi_email").filter("status='current' ")
    val memberDF = if (process_file == "rea_experian") {hiveContext.table(s"$goldDB.dim_member").filter("status='current' and last_name_scrubbed is null and address1_scrubbed is null and zip_code_scrubbed is null")
      .withColumnRenamed("first_name","first_name_mem")
      .withColumnRenamed("last_name","last_name_mem")
      .withColumnRenamed("city","city_mem")
      .withColumnRenamed("state","state_mem")
      .withColumnRenamed("batch_id","batch_id_mem")
      .withColumnRenamed("email_consent","email_consent_mem")}
    else
    {hiveContext.table(s"$goldDB.dim_member").filter("status='current' and last_name_scrubbed is null and address1_scrubbed is null and zip_code_scrubbed is null")
      .withColumnRenamed("first_name","first_name_mem")
      .withColumnRenamed("last_name","last_name_mem")
      .withColumnRenamed("city","city_mem")
      .withColumnRenamed("state","state_mem")
      .withColumnRenamed("batch_id","batch_id_mem")
      .withColumnRenamed("email_consent","email_consent_mem")}





    val thirdPartyReaDF = hiveContext.sql(s"select * from $workDB.fact_${process_file}_in_temp")
      .withColumnRenamed("member_key","member_key_list")
      .withColumnRenamed("batch_id","batch_id_rea")
      .drop(col("last_updated_date"))


    //Source data for member multi email transform
    val thirdPartyInputDF = process_file match {

      case ExperianRea =>

        val experianDf = thirdPartyReaDF.withColumnRenamed("email","email_experian").filter("""(last_name is not null and last_name <>'NULL' and trim(last_name)<>'') and
                                                                                       (address_1 is not null and  address_1 <>'NULL' and trim(address_1)<>'' ) and
                                                                                       (postal_code is not null and postal_code<>'NULL' and trim(postal_code)<>'')""")
        val REAExplodedDF = hiveContext.sql(s"select * from $workDB.fact_${process_file}_in_temp lateral view explode(split(member_key,',')) tbl as mbr")
        REAExplodedDF.write.mode(SaveMode.Overwrite).insertInto(s"$workDB.fact_${process_file}_exploded")

        val REAExperianDF  = experianDf.withColumn("member_key_rea", explode(split($"member_key_list", ",")))
          .castColumns(Seq("member_key_rea"),LongType)
          .join(memberMultiEmailDF,col("member_key_rea") === col("member_key") and lower(trim(col("email_address"))) === lower(trim(col("email_experian"))),"left")
          .drop(col("member_key"))
          .withColumnRenamed("member_key_rea","member_key")
          .distinct
          //.select(experianDf.getColumns: _*)
          .select("fact_rea_experian_out_key","first_name","last_name","address_1","address_2","city","state",
          "postal_code","zip4_code","email_experian","match_level","email_client_data","member_key","batch_id","member_key_list","is_loyalty_email","email_consent","best_email_flag")

        val SourceDF = bestRecordForUpdate(REAExperianDF)

        /*Input data for Member transform*/
        SourceDF.join(memberDF,Seq("member_key"),"inner")
          .drop(memberDF.col("member_key"))
          .select(REAExperianDF.getColumns:_*)
          .withColumn("etl_unique_id",lit(null))
          .withColumn("current_source_key",lit(1237))

      case AcxiomRea =>

        val acxiomDf = thirdPartyReaDF.withColumnRenamed("email","email_acxiom").filter("""(last_name is not null and last_name <>'NULL' and trim(last_name)<>'') and
                                                                                       (address_1 is not null and  address_1 <>'NULL' and trim(address_1)<>'' ) and
                                                                                       (zip is not null and zip<>'NULL' and trim(zip)<>'')""")

        val REAExplodedDF = hiveContext.sql(s"select * from $workDB.fact_${process_file}_in_temp lateral view explode(split(member_key,',')) tbl as mbr")
        REAExplodedDF.write.mode(SaveMode.Overwrite).insertInto(s"$workDB.fact_${process_file}_exploded")

        val REAAcxiomDF = acxiomDf.withColumn("member_key_acxiom", explode(split($"member_key_list", ",")))
          .castColumns(Seq("member_key_acxiom"),LongType)
          .join(memberMultiEmailDF,col("member_key_acxiom") === col("member_key") and col("email_address") === col("email_acxiom"),"left")
          .drop(col("member_key"))
          .withColumnRenamed("member_key_acxiom","member_key")
          //.withColumn("current_source_key",lit(1065))
          .distinct
          .select("fact_rea_acxiom_out_key","email_acxiom","ddq_cleansed_email","first_name","last_name","address_1","address_2","city","state","zip","member_key","batch_id","member_key_list","is_loyalty_email","email_consent","best_email_flag")

        val SourceDF = bestRecordForUpdate(REAAcxiomDF)

        /*Input data for Member transform*/
        SourceDF.join(memberDF,Seq("member_key"),"inner")
          .drop(memberDF.col("member_key"))
          .select(REAAcxiomDF.getColumns:_*)
          .withColumn("etl_unique_id",lit(null))
          .withColumn("current_source_key",lit(1065))
    }

    /**
      * Transform Member entry point
      */
    val inputForTransformMemberDF = thirdPartyInputDF.withColumn("match_status",lit(true))
    val transformMemberObj = new TransformMember(inputForTransformMemberDF, process_file)

    // Get insert and update member records
    val transformedMemberData = transformMemberObj.getInsertUpdateTransformedRecords
      .select(tempDimMemberDF.getColumns: _*)

    // Write output to temp dim_member table
    transformedMemberData
      .withColumn("last_updated_date", current_timestamp)
      .insertIntoHive(SaveMode.Overwrite, tempDimMemberTable, Some("process"), null)

  }
}
