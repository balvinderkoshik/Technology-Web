package com.express.edw.transform

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.express.cdw.spark.DataFrameUtils._

object EDWTransactionSummary extends SmithLoad with LazyLogging{

  override def tableName: String = "edw_transaction_summary"

  override def sourcetable: String = s"$goldDB.fact_transaction_summary"

  override def sourceColumnList: Seq[String] = Seq("store_key", "trxn_id", "member_key", "trxn_date",  "w_purchase_qty", "m_purchase_qty", "gc_purchase_qty", "other_purchase_qty", "ttl_purchase_qty", "w_units", "m_units", "gc_units", "other_units", "ttl_purchase_units", "w_amount_after_discount", "m_amount_after_discount",  "gc_amount_after_discount", "other_amount_after_discount", "ttl_amount_after_discount", "discount_amount_8888", "discount_amount_no_8888", "discount_amount","shipping_amount","express_plcc_amount", "credit_card_amount", "cash_amount", "check_amount", "gift_card_amount", "other_amount", "ttl_amount", "has_tender", "has_detail","gift_card_sales_only_ind", "out_w_purchase_qty", "out_m_purchase_qty", "ttl_out_purchase_qty", "out_w_units", "out_m_units", "ttl_out_units", "out_w_amount_after_discount","out_m_amount_after_discount", "ttl_out_amount_after_discount", "distance_traveled", "reward_certs_qty", "captured_loyalty_id", "implied_loyalty_id", "enrollment_trxn_flag","cogs", "margin", "last_updated_date")

  def main(args: Array[String]): Unit = {
    val options = parse(args)
    val businessdate = options("businessdate")
    val rundate = options("rundate")

    val dimStoreMasterDF = hiveContext.table("gold.dim_store_master")
      .filter("status='current'")
      .select("store_id", "store_key")

    val ecomOrder = hiveContext.table("smith.ecomm_order_translation")
      .withColumn("rnk", row_number() over Window.partitionBy("trxn_id","exp_order_id"))
      .filter("rnk = 1")
      .select("trxn_id", "exp_order_id","demandloc")
      .withColumnRenamed("trxn_id","trxn_id_ecomm")

    val trnNbr = hiveContext.table("gold.fact_transaction_detail")
      .filter(to_date(col("last_updated_date"))>s"$businessdate")
      .withColumn("rank", row_number() over Window.partitionBy("trxn_id"))
      .filter("rank = 1")
      .select("trxn_id", "trxn_nbr", "register_nbr")

    val invoiceDF = hiveContext.table("gold.exp_ecomm_order_data")
      .withColumn("rank", row_number().over(Window.partitionBy(col("id_trn")).orderBy(col("ingest_date").desc)))
      .filter("rank=1")
      .withColumn("ecomm_trxn_date", concat_ws("-",substring(col("id_trn"),9,4),substring(col("id_trn"),13,2),substring(col("id_trn"),15,2)))
      .withColumn("ecomm_store_id", regexp_replace(substring(col("id_trn"),1,8),"^0*",""))
      .withColumn("ecomm_register_nbr", when(length(trim(regexp_replace(substring(col("id_trn"),17,10),"^0*","")))===0 , lit(0))
        .otherwise(trim(regexp_replace(substring(col("id_trn"),17,10),"^0*",""))))
      .withColumn("ecomm_trxn_nbr", when(length(trim(regexp_replace(substring(col("id_trn"),28,6),"^0*","")))===0 , lit(0))
        .otherwise(trim(regexp_replace(substring(col("id_trn"),28,6),"^0*",""))))
      .select("invoicenumber", "ecomm_trxn_date", "ecomm_store_id", "ecomm_register_nbr", "ecomm_trxn_nbr")

    val sourceDF = sourceData.filter(to_date(col("last_updated_date"))>s"$businessdate")
      .select(sourceColumnList.head, sourceColumnList.tail: _*)
      .withColumnRenamed("store_key","trxn_store_key")
      .withColumn("gift_card_sales_only_ind", when(col("gift_card_sales_only_ind") === "Y" ,lit("YES"))
        .when(col("gift_card_sales_only_ind") === "N" ,lit("NO"))
        .otherwise(null))
      .withColumn("ingest_date", lit(to_date(col("last_updated_date"))))

    val getStoreid_final = sourceDF.updateKeys(Map("trxn_store_key" -> "store_id"), dimStoreMasterDF, "store_key", "store_id")
      .select("store_id", sourceDF.columns: _*)

    val expOrderid = getStoreid_final.join(ecomOrder, col("trxn_id")===col("trxn_id_ecomm"), "left")
      //.select("exp_order_id", getStoreid_final.columns: _*)
      .drop("trxn_id_ecomm")


    val finalDF = expOrderid.join(trnNbr ,Seq("trxn_id"))

    val joinInvoiceDF = finalDF.join(invoiceDF, col("trxn_date") === col("ecomm_trxn_date") && col("store_id") === col("ecomm_store_id")
      && col("register_nbr") === col("ecomm_register_nbr") && col("trxn_nbr") === col("ecomm_trxn_nbr"), "left")
      .select("invoicenumber", finalDF.columns: _*)
      .withColumn("store_id",when(upper(col("demandloc")) === lit("NULL") or length(col("demandloc")) === 0 or isnull(col("demandloc")) ,col("store_id")).otherwise(col("demandloc")))
      .drop("demandloc")

    val transformedDF = transformData(A_C_check=Some(col("ingest_date")), sourceDF=joinInvoiceDF, run_date=rundate, business_date = businessdate)

    load(transformedDF)
  }

}
