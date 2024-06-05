package com.express.processing.dim

import javax.jdo.annotations.Column

import akka.remote.transport.netty.UdpServerHandler
import com.express.processing.dim.DimRingCode.{hiveContext, load, logger, workDB}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import org.apache.spark.sql.functions.{col, lit, udf}
import com.express.cdw.spark.udfs._
/**
  * Created by manasee.kamble on 7/18/2017.
  */
object DimDiscountType extends DimensionLoad with LazyLogging{
  override def dimensionTableName: String = "dim_discount_type"

  override def surrogateKeyColumn: String = "discount_type_key"

  override def naturalKeys: Seq[String] = Seq("discount_type_code","discount_deal_code")

  override def derivedColumns: Seq[String] = Nil

  override def transform: DataFrame = {
    val tlogDiscountDedupWorkTable = s"$workDB.work_tlog_discount_dedup"
    logger.info("Reading work_tlog_discount_dedup Work Table: {}", tlogDiscountDedupWorkTable)
    hiveContext.table(tlogDiscountDedupWorkTable).
      withColumn("discount_type_code",checkEmptyUDF(prependZeroForSingleDigit(col("discount_type")))).
      withColumn("discount_deal_code",checkEmptyUDF(prependZeroForSingleDigit(col("discount_type_1_discount_deal_code")))).
      withColumn("discount_type_description",defineDiscountTypeDescription(col("discount_type"))).
      withColumn("discount_deal_description",defineDiscountDealDescription(col("discount_type"),col("discount_type_1_discount_deal_code"))).
      withColumn("effective_date",lit(null)).
      withColumn("expiration_date",lit(null)).
      withColumn("data_source",lit("Express T-log")).
      dropDuplicates(Seq("discount_type_code","discount_deal_code"))
  }


  val checkEmptyUDF  = udf ((inputStr : String)=>{
    inputStr match {
      case "" => null
      case "null" => null
      case _ => inputStr
    }
  })
  val prependZeroForSingleDigit =  udf((columnValue : Int) =>  {
    if(columnValue<=9)
      "0"+columnValue
    else
      columnValue.toString
  })
  val defineDiscountTypeDescription = udf((discountType : Int) => {
    discountType match {
      case 0 => "Group Coupon"
      case 1 => "Line Coupon"
      case 2 => "Group Damage"
      case 3 => "Line Damage"
      case 4 => "Group Promo"
      case 5 => "Line Promo"
      case 6 => "Associate"
      case _ => "Unknown"
    }
  })

  val defineDiscountDealDescription = udf((discountTypeCode : String,discountDealCode : String) => {
    val discountDealDesc = discountTypeCode.concat(discountDealCode)
    discountDealDesc match {
      case "00"|"40" => "Group Coupon"
      case "11"|"61" => "Item Discount % Off"
      case "12"|"62" => "Item Discount $ Off"
      case "63" | "03" => "Group Discount % Off"
      case "04"|"14"|"64" => "Group Discount $ Off"
      case "65"|"15" => "Free Item (Always % off)"
      case "66"|"06" => "Promo Discount % Off"
      case "07"|"67" => "Promo Discount $ Off"
      case "610" => "Associate Discount % Off"
      case "013"|"613" => "Deal Pricing Qty Break"
      case "614"|"014" => "Deal Pricing % Off"
      case "41"|"51" => "Line Coupon"
      case "42"|"52" => "Group Damage"
      case "45" => "Line Promo"
      case "10"|"60" => "Default"
      case "19"|"114"|"413"|"441" => "UNKNOWN"
    }
  })
  def main(args: Array[String]): Unit = {
    val batchId = args(0)
    load(batchId)
  }
}
