package com.express.processing.transform

import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.Settings
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import java.sql.Date
import org.joda.time.DateTime

/**
  * Created by manasee.kamble on 7/29/2017.
  */
object TransformHousehold extends LazyLogging {


  val scrubbedcols = Seq("last_name_scrubbed", "address1_scrubbed", "address2_scrubbed", "city_scrubbed", "state_scrubbed", "zip_code_scrubbed")
  System.setProperty("hive.exec.dynamic.partition.mode", "nonstrict")
  private val sparkConf = Settings.sparkConf
  val sparkContext = new SparkContext(sparkConf)
  val hiveContext = new HiveContext(sparkContext)
  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
  private val workDB = Settings.getWorkDB
  private val goldDB = Settings.getGoldDB
  val householdkeyCol = "household_key"
  val householdkeyColTransform = "household_key_transf"
  val TraansformHHResultTable = s"$workDB.dim_household"

  val targetColumnList = Seq(householdkeyCol, "member_count", "first_trxn_date_key", "first_trxn_date", "is_express_plcc", "add_date_key", "add_date",
    "introduction_date_key", "introduction_date", "last_store_purch_date_key", "last_store_purch_date", "last_web_purch_date_key",
    "last_web_purch_date", "account_open_date_key", "account_open_date", "zip_code", "scoring_model_key", "scoring_model_segment_key",
    "valid_address", "record_type", "associated_member_key")

  val sourceColumnList = Seq(householdkeyColTransform, "member_count_transf", "first_trxn_date_key_transf", "first_trxn_date_transf", "is_express_plcc_transf", "add_date_key_transf", "add_date_transf",
    "introduction_date_key_transf", "introduction_date_transf", "last_store_purch_date_key_transf", "last_store_purch_date_transf", "last_web_purch_date_key_transf",
    "last_web_purch_date_transf", "account_open_date_key_transf", "account_open_date_transf", "zip_code_transf", "scoring_model_key_transf", "scoring_model_segment_key_transf",
    "valid_address_transf", "record_type_transf", "associated_member_key_transf")

  object HouseholdIdentifier extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
    private val schema = StructType(Seq(StructField("household_key", LongType), StructField("overlay_date", DateType)))

    override def inputSchema: org.apache.spark.sql.types.StructType = schema

    // A StructType represents data types of values in the aggregation buffer
    override def bufferSchema: StructType = schema

    override def dataType: DataType = LongType

    override def deterministic: Boolean = true

    private def resolveHouseKey(row1: Row, row2: Row): Row = {
      if (row1.get(1) == null) return row2
      if (row2.get(1) == null) return row1

      if (row1.getAs[Date](1).equals(row2.getAs[Date](1))) {
        if (row2.getAs[Long](0) > row1.getAs[Long](0))
          row1
        else
          row2
      }
      else {
        if (row1.getAs[Date](1).after(row2.getAs[Date](1)))
          row2
        else
          row1
      }
    }

    // This is the initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = -1l
      buffer(1) = Date.valueOf(DateTime.now().plusDays(2).toLocalDate.toString)
    }

    // This is how to update your buffer schema given an input.
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val row = resolveHouseKey(buffer, input)
      buffer(0) = row.get(0)
      buffer(1) = row.get(1)
    }

    // This is how to merge two objects with the bufferSchema type.
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val row = resolveHouseKey(buffer1, buffer2)
      buffer1(0) = row.get(0)
      buffer1(1) = row.get(1)
    }

    // This is where you output the final value, given the final value of your bufferSchema.
    override def evaluate(buffer: Row): Any = {
      buffer.get(0)
    }
  }


  def main(args: Array[String]): Unit = {
    val processing_file = args {
      1
    }.toLowerCase.trim()
    val coalescePartitionNum = Settings.getCoalescePartitions

    val dimDateDF = hiveContext.sql(s"select sdate,nvl(date_key,-1) as date_key from $goldDB.dim_date where status ='current'").persist()

    val dim_memberDF = hiveContext.table(s"$goldDB.dim_member")
      .where("status = 'current'")
    val memberDF = scrubbedcols.foldLeft(dim_memberDF) {
      (df, column) =>
        df
          .withColumn(column,
            if(column != "zip_code_scrubbed") {
              when(upper(trim(col(column))) === lit("") or isnull(upper(trim(col(column)))) or upper(trim(col(column))) === lit("NULL"), coalesce(upper(trim(col(column.dropRight(9)))), lit(""))).otherwise(upper(trim(col(column))))
            }
            else
              col(column)
          )
    }
      .withColumn("zip_code_scrubbed", when(upper(trim(col("zip_code_scrubbed"))) === lit("") or isnull(upper(trim(col("zip_code_scrubbed")))) or upper(trim(col("zip_code_scrubbed"))) === lit("NULL"), upper(trim(lpad(col("zip_code"), 5, "0")))).otherwise(upper(trim(lpad(col("zip_code_scrubbed"), 5, "0")))))
      .withColumn("overlay_date", when((col("household_key").equalTo(lit(-1)) or isnull(col("household_key"))) and (trim(col("overlay_date")) === lit("") or isnull(trim(col("overlay_date")))), lit(current_date))
        .otherwise(when(col("household_key").gt(lit(-1)) and isnull(trim(col("overlay_date"))), lit("1900-01-01")).otherwise(col("overlay_date"))))
      .selectExpr("member_key", "household_key", "first_trxn_date_key", "first_trxn_date", "is_express_plcc", "customer_introduction_date",
        "last_store_purch_date", "last_web_purch_date", "member_acct_open_date", "last_name_scrubbed", "address1_scrubbed", "address2_scrubbed", "city_scrubbed",
        "state_scrubbed", "zip_code_scrubbed", "overlay_date", "upper(trim(dwelling_type)) as dwelling_type")
      .persist()


    val HHmemberDF = scrubbedcols.foldLeft(memberDF) {
      (df, column_name) => df.withColumn(column_name, when(upper(trim(col(column_name))) === lit("NULL"), lit("")).otherwise(col(column_name)))
    }

    logger.info("dim_member loaded successfully!!")

    val batch_id = hiveContext.table(s"$goldDB.dim_member").where("status = 'current'")
      .select("batch_id").take(1).head.getAs[String]("batch_id")

    logger.info("filtering out members with null last_name and address columns.")

    val HHmemberDF_Final = HHmemberDF
      .filter("ascii(last_name_scrubbed) !=0 and ascii(address1_scrubbed) != 0 and ascii(city_scrubbed) != 0  and ascii(state_scrubbed)  != 0 and ascii(zip_code_scrubbed) != 0")

    val dimNotNullDFRenamed = HHmemberDF_Final
      .withColumnRenamed("last_name_scrubbed", "last_name_scrubbed_NN")
      .withColumnRenamed("address1_scrubbed", "address1_scrubbed_NN")
      .withColumnRenamed("address2_scrubbed", "address2_scrubbed_NN")
      .withColumnRenamed("city_scrubbed", "city_scrubbed_NN")
      .withColumnRenamed("state_scrubbed", "state_scrubbed_NN")
      .withColumnRenamed("zip_code_scrubbed", "zip_code_scrubbed_NN")
      .withColumnRenamed("household_key", "household_key_NN")
      .withColumnRenamed("member_acct_open_date", "member_acct_open_date_NN")
      .withColumnRenamed("last_web_purch_date", "last_web_purch_date_NN")
      .withColumnRenamed("last_store_purch_date", "last_store_purch_date_NN")
      .withColumnRenamed("customer_introduction_date", "customer_introduction_date_NN")
      .withColumnRenamed("is_express_plcc", "is_express_plcc_NN")
      .withColumnRenamed("first_trxn_date", "first_trxn_date_NN")
      .withColumnRenamed("first_trxn_date_key", "first_trxn_date_key_NN")
      .withColumnRenamed("overlay_date", "overlay_date_NN")
      .withColumnRenamed("member_key", "member_key_NN")
      .filter(not(isnull(col("last_name_scrubbed_NN"))) and (ascii(col("last_name_scrubbed_NN")) !== 0)
        and not(isnull(col("address1_scrubbed_NN"))) and (ascii(col("address1_scrubbed_NN")) !== 0)
        //and not(isnull(col("address2_scrubbed_NN"))) and (ascii(col("address2_scrubbed_NN")) !== 0)
        and not(isnull(col("city_scrubbed_NN"))) and (ascii(col("city_scrubbed_NN")) !== 0)
        and not(isnull(col("state_scrubbed_NN"))) and (ascii(col("state_scrubbed_NN")) !== 0)
        and not(isnull(col("zip_code_scrubbed_NN"))) and (ascii(col("zip_code_scrubbed_NN")) !== 0)
        and col("dwelling_type").isin("MULTI_NO_APARTMENT", "MULTI_WITH_APARTMENT", "UNKNOWN", "SINGLE"))


    logger.info("Grouping members on last_name and address to assign proper household_key")

    val dimGrpAddress2NotNull = dimNotNullDFRenamed
      .groupBy(col("last_name_scrubbed_NN"), col("address1_scrubbed_NN"), col("address2_scrubbed_NN"), col("city_scrubbed_NN"), col("state_scrubbed_NN"), col("zip_code_scrubbed_NN"))
      .agg(
        HouseholdIdentifier(col("household_key_NN"), col("overlay_date_NN")).alias("household_key"),
        count("*").alias("member_count"),
        //    concat_ws(",", sort_array(collect_set(col("member_key_NN"))).cast("String")).alias("associated_member_key"),
        sort_array(collect_set(col("member_key_NN"))).cast("String").alias("associated_member_key"),
        min(col("first_trxn_date_NN")).alias("first_trxn_date"),
        concat_ws(",", collect_set(col("is_express_plcc_NN"))).alias("is_express_plcc_set"),
        min(col("customer_introduction_date_NN")).alias("introduction_date"),
        max(col("last_store_purch_date_NN")).alias("last_store_purch_date"),
        max(col("last_web_purch_date_NN")).alias("last_web_purch_date"),
        min(col("overlay_date_NN")).alias("overlay_date"),
        min(col("member_acct_open_date_NN")).alias("account_open_date")
      )
      .withColumn("is_express_plcc", when(col("is_express_plcc_set").contains("YES"), lit("YES")).otherwise(lit("NO")))
      .withColumn("add_date", current_date())
      .withColumnRenamed("zip_code_scrubbed_NN", "zip_code")
      .withColumn("valid_address", concat_ws("", col("address1_scrubbed_NN"), col("address2_scrubbed_NN")))
      .drop(col("address2_scrubbed_NN"))
      .withColumn("scoring_model_key", lit(null).cast("Long"))
      .withColumn("scoring_model_segment_key", lit(null).cast("Long"))
      .withColumn("record_type", lit(null).cast("String"))
      .withColumn("associated_member_key", when(!isnull(col("associated_member_key")),regexp_replace(col("associated_member_key"), "\\[|\\]", "")).otherwise(col("associated_member_key")))//.unionAll(dimNotNullDF.)

    logger.info("Joining with dim_date to populate proper date_keys")

    val dimTrnsfDF = dimGrpAddress2NotNull
      .withColumn("household_key", when(col("household_key").isNotNull, col("household_key")).otherwise(lit(0l).cast("Long")))


    /*
      Transforming all Date Keys for Dim Household table
     */

    val firstTrxnDate = dimTrnsfDF.join(broadcast(dimDateDF), col("first_trxn_date") === col("sdate"), "left")
      .withColumn("first_trxn_date_key", col("date_key"))
      .select(dimTrnsfDF.getColumns :+ col("first_trxn_date_key"): _*)

    val addDateKey = firstTrxnDate.join(broadcast(dimDateDF), col("add_date") === col("sdate"), "left").
      withColumn("add_date_key", col("date_key")).
      select(firstTrxnDate.getColumns :+ col("add_date_key"): _*)

    val introDateKey = addDateKey.join(broadcast(dimDateDF), col("introduction_date") === col("sdate"), "left").
      withColumn("introduction_date_key", col("date_key")).
      select(addDateKey.getColumns :+ col("introduction_date_key"): _*)

    val lastStorePurDate = introDateKey.join(broadcast(dimDateDF), col("last_store_purch_date") === col("sdate"), "left").
      withColumn("last_store_purch_date_key", col("date_key")).
      select(introDateKey.getColumns :+ col("last_store_purch_date_key"): _*)

    val lastWebPurDate = lastStorePurDate.join(broadcast(dimDateDF), col("last_web_purch_date") === col("sdate"), "left").
      withColumn("last_web_purch_date_key", col("date_key")).
      select(lastStorePurDate.getColumns :+ col("last_web_purch_date_key"): _*)

    val accountOpenDt = lastWebPurDate.join(broadcast(dimDateDF), col("account_open_date") === col("sdate"), "left").
      withColumn("account_open_date_key", col("date_key")).
      select(lastWebPurDate.getColumns :+ col("account_open_date_key"): _*)

    val HouseholdLoadDF = accountOpenDt
      .na.fill(-1, Seq("first_trxn_date_key", "introduction_date_key",
      "last_store_purch_date_key", "last_web_purch_date_key", "account_open_date_key", "add_date_key"))


    HouseholdLoadDF.registerTempTable("HouseholdLoadDF")
    val existingHKCols = HouseholdLoadDF.columns.mkString(",")
    val existingHKAliasCols = HouseholdLoadDF.columns.map("temp." + _).mkString(",")

    val identifiedHK = " select " + existingHKCols +
      ",row_number() over (partition by household_key order by overlay_date asc) as rank_on_criteria_and_HK  from HouseholdLoadDF"

    val identifiedHKDF = hiveContext.sql(identifiedHK)
      .withColumn("household_key", when((col("rank_on_criteria_and_HK") > 1) or (col("household_key") === -1), lit(0l)).otherwise(col("household_key")))
      .select(targetColumnList.head, targetColumnList.tail: _*)
      .persist()

    val existingHKLoadDF = identifiedHKDF.filter("household_key != 0").persist()
    val newHKLoadDF = identifiedHKDF.filter("household_key = 0").sort("associated_member_key","zip_code").persist()

    /*
      Loading Final Data and Natural Key generation for Dim Household table
    */
    logger.info("Generating new household_key for new households")

    val dimhousehold = hiveContext.sql(s"select * from $goldDB.dim_household")

    // fetch maximum value for household_key from existing data
    val maxhouseholdkey = dimhousehold
      .select(max(col("household_key")))
      .collect()
      .headOption match {
      case None => 0
      case Some(row) => row.get(0).asInstanceOf[Long]
    }
    logger.info("Printing Max household Key value :: " + maxhouseholdkey)

    val rankedRows = newHKLoadDF
      .rdd
      .zipWithIndex()
      .map { case (row, id) => Row.fromSeq((id + maxhouseholdkey + 1) +: row.toSeq.tail) }

    val NewHouseHoldDF = hiveContext.createDataFrame(rankedRows, newHKLoadDF.schema).persist()

    val finalHouseHoldDF = existingHKLoadDF.unionAll(NewHouseHoldDF)

    val finalHouseholdResultDataset = finalHouseHoldDF.withColumn("household_id", lit(null).cast("Long")).select("household_id", targetColumnList: _*)
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    finalHouseholdResultDataset.registerTempTable("temp_dim_household_table")

    val sourceFileData = hiveContext.table("temp_dim_household_table").withColumnRenamed(householdkeyCol, householdkeyColTransform).withColumnRenamed("household_id", "household_id_transf")

    val dimMember = hiveContext.table(s"$goldDB.dim_member")
      .filter("status='current' and best_household_member='YES'")
      .select("household_key", "valid_loose", "valid_strict", "last_updated_date")
      .withColumn("rank", row_number() over Window.partitionBy("household_key").orderBy(desc("last_updated_date")))
      .filter("rank = 1")
      .withColumnRenamed("household_key", "member_household_key")

    val sourceTable = sourceFileData
      .withColumn("member_count_transf", col("member_count"))
      .withColumn("first_trxn_date_key_transf", col("first_trxn_date_key")).withColumn("first_trxn_date_transf", col("first_trxn_date"))
      .withColumn("is_express_plcc_transf", col("is_express_plcc")).withColumn("add_date_key_transf", col("add_date_key"))
      .withColumn("add_date_transf", col("add_date")).withColumn("introduction_date_key_transf", col("introduction_date_key"))
      .withColumn("introduction_date_transf", col("introduction_date")).withColumn("last_store_purch_date_key_transf", col("last_store_purch_date_key"))
      .withColumn("last_store_purch_date_transf", col("last_store_purch_date")).withColumn("last_web_purch_date_key_transf", col("last_web_purch_date_key"))
      .withColumn("last_web_purch_date_transf", col("last_web_purch_date")).withColumn("account_open_date_key_transf", col("account_open_date_key"))
      .withColumn("account_open_date_transf", col("account_open_date")).withColumn("zip_code_transf", col("zip_code"))
      .withColumn("scoring_model_key_transf", col("scoring_model_key")).withColumn("scoring_model_segment_key_transf", col("scoring_model_segment_key"))
      .join(dimMember, col("member_household_key") === col("household_key_transf"), "left")
      .withColumn("valid_address_transf", when(col("valid_strict") === "YES", lit("STRICT"))
        .when(col("valid_loose") === "YES" and col("valid_strict") === "NO", lit("LOOSE"))
        .when(col("valid_loose") === "NO" and col("valid_strict") === "NO", lit("INVALID"))
        .when(col("valid_loose").isNull or col("valid_strict").isNull, lit("INVALID")))
      .withColumn("record_type_transf", col("record_type")).withColumn("associated_member_key_transf", col("associated_member_key"))
      .select("household_id_transf", sourceColumnList: _*)

    logger.info("inserting data into work.dim_household")

    // Insert into target table
    sourceTable
      .withColumn("action_flag", lit(null))
      .withColumn("last_updated_date", current_timestamp())
      .withColumn("batch_id", lit(batch_id))
      .withColumn("process", lit(processing_file))
      .insertIntoHive(SaveMode.Overwrite, TraansformHHResultTable, Some("process"), batch_id)

    logger.info("Loaded work.dim_household table")
  }
}