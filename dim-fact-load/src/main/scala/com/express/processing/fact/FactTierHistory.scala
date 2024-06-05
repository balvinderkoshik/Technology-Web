package com.express.processing.fact

/**
  * Created by aditi.chauhan on 7/10/2017.
  */


import com.express.cdw.{ETLUniqueIDColumn, fileDateFunc}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.functions._


object FactTierHistory extends FactLoad {

  override def factTableName = "fact_tier_history"

  override def surrogateKeyColumn = "tier_hist_id"

  override def transform: DataFrame = {

    import hiveContext.implicits._
    val lwMemberTiersDataset = hiveContext.sql(s"select * from $workDB.work_bp_member_tier_dataquality")

    /*Fetching dim_member dataset */
    val dimMemberDF = hiveContext.sql(s"select ip_code,member_key from $goldDB.dim_member where status='current' and ip_code is not null")
      .withColumn("rank",row_number().over(Window.partitionBy(col("ip_code")) orderBy(desc("member_key"))))
      .filter("rank=1")
      .drop("rank")

    /* dim_date dataset- Fetching sdate,date_key from dim_date.Lookup column:-sdate */
    val dimDateDataset = hiveContext.sql(s"select sdate, nvl(date_key,-1) as date_key from " +
      s"$goldDB.dim_date where status='current'")

    /* dim_tier dataset- Fetching tier_id,tier_key from dim_tier.Lookup column:-tier_id */
    val dimTierDataset = hiveContext.sql(s"select tier_id, nvl(tier_key,-1) as tier_key from " +
      s"$goldDB.dim_tier where status='current'")

    /* Dataframe frame formed after joining lwMemberTiersDataset and dim_date on from_date */
    val joinedTransformationFromDate = lwMemberTiersDataset
      .join(dimDateDataset, to_date(lwMemberTiersDataset.col("from_date")) === dimDateDataset.col("sdate"), "left")
      .withColumnRenamed("date_key", "tier_begin_date_key")
      .drop("sdate")

    /* Dataframe frame formed after joining JoinedTransformationFromDate and dim_date on to_date */
    val joinedTransformationDate = joinedTransformationFromDate
      .join(dimDateDataset, to_date(joinedTransformationFromDate.col("to_date")) === dimDateDataset.col("sdate"), "left")
      .withColumnRenamed("date_key", "tier_end_date_key")

    /* Dataframe frame formed after joining JoinedTransformationDate and dim_tier on tier_id and dim_member on member_id */
    val joinedTransformation = joinedTransformationDate
      .join(dimTierDataset, Seq("tier_id"), "left")
      .withColumn("tier_key", $"tier_key")
      .join(dimMemberDF,col("member_id") === col("ip_code"),"left")
      .drop(col("ip_code"))


    /*Applying relevant transformations based on the business logic */
    joinedTransformation
      .withColumn("ipcode", $"member_id")
      .withColumn("tier_begin_date", $"from_date")
      .withColumn("tier_end_date", $"to_date")
      .withColumn("last_updated_date", current_timestamp)
      .withColumn("batch_id", lit(batchId))
      .withColumn("member_key",when(col("member_key").isNull,lit(-1)).otherwise(col("member_key")))
      .withColumn("file_date", udf(fileDateFunc _).apply(col(ETLUniqueIDColumn)))
      .orderBy("file_date","id")   // Added this line as per DM-1752 change, dm-1752 is a sub task of DM-1714
  }

  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }

}
