package com.express.edw.transform

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.express.cdw.spark.DataFrameUtils._

object EDWTransactionDetail extends SmithLoad with LazyLogging{

  override def tableName: String = "edw_transaction_detail"

  override def sourcetable: String = s"$goldDB.fact_transaction_detail"

  override def sourceColumnList: Seq[String] = Seq("trxn_detail_id", "member_key", "trxn_date", "trxn_id", "trxn_nbr", "register_nbr", "trxn_seq_nbr", "store_key", "sku", "transaction_type_key", "trxn_time_key", "purchase_qty", "shipped_qty", "units", "is_shipping_cost", "original_price", "retail_amount","orig_trxn_nbr", "total_line_amnt_after_discount", "unit_price_after_discount", "discount_pct", "discount_amount", "discount_type_key", "ring_code_used",
    "markdown_type", "gift_card_sales_ind", "division_id", "cashier_id", "salesperson", "orig_trxn_nbr", "post_void_ind", "implied_loyalty_id",
    "captured_loyalty_id", "cogs", "margin", "last_updated_date")

  def main(args: Array[String]): Unit = {
    val options = parse(args)
    val businessdate = options("businessdate")
    val rundate = options("rundate")

    val factTrxnDetail = sourceData.filter(to_date(col("last_updated_date"))>s"$businessdate")
      .select(sourceColumnList.head, sourceColumnList.tail: _*)
      .withColumn("gift_card_sales_ind", when(col("gift_card_sales_ind")==="Y", lit("YES"))
        .when(col("gift_card_sales_ind")==="N", lit("NO"))
        .otherwise(null))
      .withColumn("ingest_date", lit(to_date(col("last_updated_date"))))
      .withColumnRenamed("store_key", "trxn_store_key")
      .withColumnRenamed("trxn_time_key", "ftd_trxn_time_key")
      .withColumnRenamed("trxn_detail_id", "transaction_detail_id")

    val dimStoreMasterDF = hiveContext.table("gold.dim_store_master")
      .filter("status='current'")
      .select("store_id", "store_key")

    val expOrderIdDF = hiveContext.table("smith.ecomm_order_translation")
      .withColumn("rnk", row_number().over(Window.partitionBy(col("trxn_id"))))
      .filter("rnk=1")
      .withColumnRenamed("trxn_id", "trxn_id_order")
      .select("trxn_id_order", "exp_order_id","demandloc")

    val trxnTimeDF = hiveContext.table("gold.dim_time")
      .filter("status='current'")
      .select("time_key", "time_in_24hr_day")

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

    val joinStoreIdDF = factTrxnDetail.updateKeys(Map("trxn_store_key" -> "store_id"), dimStoreMasterDF, "store_key", "store_id")
      .select("store_id", factTrxnDetail.columns: _*)

    val joinExpOrderIdDF = joinStoreIdDF.join(expOrderIdDF, col("trxn_id") === col("trxn_id_order"), "left")
      //.select("exp_order_id", joinStoreIdDF.columns: _*)
      .drop("trxn_id_order")

    val joinInvoiceDF = joinExpOrderIdDF.join(invoiceDF, col("trxn_date") === col("ecomm_trxn_date") && col("store_id") === col("ecomm_store_id")
      && col("register_nbr") === col("ecomm_register_nbr") && col("trxn_nbr") === col("ecomm_trxn_nbr"), "left")
      .select("invoicenumber", joinExpOrderIdDF.columns: _*)
      .withColumn("store_id",when(upper(col("demandloc")) === lit("NULL") or length(col("demandloc")) === 0 or isnull(col("demandloc")) ,col("store_id")).otherwise(col("demandloc")))

    val finalDF = joinInvoiceDF.join(trxnTimeDF, col("time_key") === col("ftd_trxn_time_key"), "left")
      .withColumnRenamed("time_in_24hr_day", "trxn_time_key")
      .select("trxn_time_key", joinInvoiceDF.columns: _*)

    val transformedDF = transformData(A_C_check=Some(col("ingest_date")), sourceDF = finalDF, run_date=rundate, business_date = businessdate)


    load(transformedDF)
  }

}
