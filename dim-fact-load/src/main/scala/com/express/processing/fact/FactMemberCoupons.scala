package com.express.processing.fact
import com.express.cdw.spark.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by Mahendranadh.Dasari on 21-08-2017.
  */
object FactMemberCoupons extends FactLoad {


  override def factTableName = "fact_member_coupons"

  override def surrogateKeyColumn = "fact_member_coupons_id"

  override def transform: DataFrame = {
    val dimMemberDF = hiveContext.sql(s"select ip_code,member_key as member_key_mbr from $goldDB.dim_member where status='current' and ip_code is not null")
      .withColumn("rank",row_number().over(Window.partitionBy(col("ip_code")) orderBy(desc("member_key_mbr"))))
      .filter("rank=1")
      .drop("rank")

    val factMemberCouponsTransformed = hiveContext.table(s"$workDB.work_bp_membercoupons_dataquality")
      //.withColumnRenamed("member_id", "member_key")
      .withColumnRenamed("times_used", "time_used")
      .withColumnRenamed("status", "member_coupon_status")
      .withColumn("last_updated_date", current_timestamp)
    logger.debug("Input record count for fact member coupons Fact Load:{}---------------------------->", factMemberCouponsTransformed.count.toString)

    factMemberCouponsTransformed.join(dimMemberDF,
      col("member_id") === col("ip_code"),"left")
      .withColumn("member_key",col("member_key_mbr"))
      .withColumn("member_key",when(col("member_key").isNull,lit(-1)).otherwise(col("member_key")))
  }

  def main(args: Array[String]): Unit = {
    load()
  }
}
