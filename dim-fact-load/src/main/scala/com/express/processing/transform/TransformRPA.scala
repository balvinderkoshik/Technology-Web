package com.express.processing.transform

import com.express.cdw.CDWContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import com.express.cdw.spark.DataFrameUtils._
import org.apache.spark.sql.expressions.Window


/**
  * Created by bhautik.patel on 27/10/17.
  */
object TransformRPA  extends  CDWContext
{

  def main(args: Array[String]): Unit = {
    import hiveContext.implicits._

    val factAlias = "factRPA"
    //val process_file = args(0).trim.toLowerCase
    val tempDimMemberDF = hiveContext.table(s"$workDB.dim_member")
    val tempDimMemberTable = s"$workDB.dim_member"
    val dimMemberMultiPhone = hiveContext.sql(s"select * from $goldDB.dim_member_multi_phone where status='current'")
    //val RPAAcxiomDF = hiveContext.sql(s"select * from $workDB.fact_rpa_acxiom_temp")

    if (args.length < 1)
      throw new Exception("Please provide Third party type as argument")

    val process_file = args(0).trim.toLowerCase
    val thirdPartyRpaDF = hiveContext.sql(s"select * from $workDB.fact_${process_file}_temp")
      .drop(col("batch_id"))
      .drop(col("last_updated_date"))
    // .drop("etl_unique_id")

    def bestRecordForUpdate(phoneData: DataFrame): DataFrame = {

      import phoneData.sqlContext.implicits._

      val bestRecordForUpdate = phoneData.withColumn("function_rank", when($"is_loyalty_flag" === "YES", lit(1))
        .when($"phone_consent" === "YES", lit(2)).otherwise(lit(99)))
        .withColumn("best_rec_for_update", row_number() over Window.partitionBy("member_key").orderBy(asc("function_rank")))
        .filter("best_rec_for_update = 1")
        .drop(col("function_rank"))
        .drop(col("best_rec_for_update"))

      bestRecordForUpdate

    }

    val mbrDF = if (process_file == "rpa_experian") {hiveContext.table(s"$goldDB.dim_member").filter("status='current' and last_name_scrubbed is null and address1_scrubbed is null and zip_code_scrubbed is null")
      .withColumnRenamed("first_name","first_name_mem")
      .withColumnRenamed("last_name","last_name_mem")
      .withColumnRenamed("city","city_mem")
      .withColumnRenamed("state","state_mem")
      .withColumnRenamed("zip_code","zip_code_mem")
      .withColumnRenamed("batch_id","batch_id_mem")
      .withColumnRenamed("phone_consent","phone_consent_mem")}
    else
    {
      hiveContext.table(s"$goldDB.dim_member").filter("status='current' and last_name_scrubbed is null and address1_scrubbed is null and zip_code_scrubbed is null")
        .withColumnRenamed("first_name","first_name_mem")
        .withColumnRenamed("last_name","last_name_mem")
        .withColumnRenamed("city","city_mem")
        .withColumnRenamed("state","state_mem")
        .withColumnRenamed("zip_code","zip_code_mem")
        .withColumnRenamed("batch_id","batch_id_mem")
        .withColumnRenamed("phone_consent","phone_consent_mem")
    }

    val columnList = Seq("member_key", "first_name", "last_name", "address_1", "address_2", "city", "state", "zip_code", "zip4_code", "batch_id")


    val RpaExplodedDF = hiveContext.sql(s"select * from $workDB.fact_${process_file}_temp lateral view explode(split(member_key,',')) tbl as mbr")
    RpaExplodedDF.write.mode(SaveMode.Overwrite).insertInto(s"$workDB.fact_${process_file}_exploded")

    val factRPAThrdpDF = if (process_file == "rpa_acxiom") {
      hiveContext.table(s"$workDB.fact_${process_file}_temp")
        .filter("""(last_name is not null and last_name <>'NULL' and trim(last_name)<>'') and
                      (address_1 is not null and  address_1 <>'NULL' and trim(address_1)<>'' ) and
                       (zip_code is not null and zip_code<>'NULL' and trim(zip_code)<>'')""")
        .withColumn("member_key", explode(split($"member_key", ",")))
        .withColumnRenamed("member_key","member_key_rpa")
        .withColumnRenamed("phone_number","phone_rpa")
        .withColumn("address_2",concat($"unit_designator", $"unit_number"))
        //.withColumnRenamed("address_line_1","address_1")
        //.withColumnRenamed("city_name","city")
        //.withColumnRenamed("zip_4_code","zip4_code")
        .withColumnRenamed("batch_id","batch_id_rpa")
        .join(dimMemberMultiPhone,col("member_key_rpa") === col("member_key") and col("phone_rpa") === col("phone_number"),"left")
        .drop(col("member_key"))
        .withColumnRenamed("member_key_rpa","member_key")
        //.select(thirdPartyRpaDF.getColumns:_*)
        .select("first_name","last_name","address_1","address_2","city","state","zip_code","zip4_code","batch_id","member_key","is_loyalty_flag","phone_consent")
    }
    else                                                    {
      hiveContext.table(s"$workDB.fact_${process_file}_temp")
        .filter("""(last_name is not null and last_name <>'NULL' and trim(last_name)<>'') and
                      (address_1 is not null and  address_1 <>'NULL' and trim(address_1)<>'' ) and
                       (zip_code is not null and zip_code<>'NULL' and trim(zip_code)<>'')""")
        .withColumnRenamed("phone_number","phone_rpa")
        .withColumn("member_key", explode(split($"member_key", ",")))
        .withColumnRenamed("member_key","member_key_rpa").distinct
        // .withColumnRenamed("last_updated_date","last_updated_date_rpa")
        .withColumnRenamed("batch_id","batch_id_rpa")
        .join(dimMemberMultiPhone,col("member_key_rpa") === col("member_key") and trim(col("phone_rpa")) === trim(col("phone_number")),"left")
        .drop(col("member_key"))
        .withColumnRenamed("member_key_rpa","member_key")
        // .select(thirdPartyRpaDF.getColumns:_*)
        .select("first_name","last_name","address_1","address_2","city","state","zip_code","zip4_code","batch_id","member_key","is_loyalty_flag","phone_consent")

    }


    val SourceDF = bestRecordForUpdate(factRPAThrdpDF)

    val joinSourceDF = if (process_file == "rpa_experian") {SourceDF.join(mbrDF,Seq("member_key"),"inner")
      .drop(mbrDF.col("member_key"))
      .select(factRPAThrdpDF.getColumns:_*)
      .withColumn("match_status",lit(true))
      .withColumn("etl_unique_id",lit(null))
      .withColumn("current_source_key",lit(1236))}
    else
    {SourceDF.join(mbrDF,Seq("member_key"),"inner")
      .drop(mbrDF.col("member_key"))
      .select(factRPAThrdpDF.getColumns:_*)
      .withColumn("match_status",lit(true))
      .withColumn("etl_unique_id",lit(null))
      .withColumn("current_source_key",lit(1064))}


    val memberTransformDF = new TransformMember(joinSourceDF,process_file)
    val transformedMemberData = memberTransformDF.getInsertUpdateTransformedRecords
      .select(tempDimMemberDF.getColumns: _*)

    // Write output to temp dim_member table
    transformedMemberData.withColumn("last_updated_date", current_timestamp)
      .insertIntoHive(SaveMode.Overwrite, tempDimMemberTable, Some("process"), null)
  }
}