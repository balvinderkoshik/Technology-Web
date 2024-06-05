package com.express.edw.transform

import org.apache.spark.sql.functions._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.expressions.Window

object EDWCustomerDedupTxnMtx extends SmithLoad with LazyLogging {

  override def tableName: String = "edw_customer_dedup_txn_mtx"

  override def sourcetable: String = s"$goldDB.fact_transaction_detail"

  override def filterCondition: String = "status='current'"

  override def sourceColumnList: Seq[String] = Seq("member_key","trxn_date","store_key","trxn_nbr","trxn_id","register_nbr")

  def main(args: Array[String]): Unit = {
    val options=parse(args)
    val businessdate=options("businessdate")
    val rundate = options("rundate")

    import com.express.cdw.spark.DataFrameUtils._



//    val outTable=hiveContext.table(s"$smithDB.edw_customer_dedup_txn_mtx")

    val facttds=hiveContext.table(s"$sourcetable")
      .select(sourceColumnList.head,sourceColumnList.tail: _*)
      .withColumnRenamed("trxn_id","trxn_id_ftd")
      .groupByAsList(Seq("trxn_date","store_key","register_nbr","trxn_nbr","member_key","trxn_id_ftd")).drop("grouped_data").drop("grouped_count")

    val factdmh=hiveContext.table(s"$goldDB.fact_dedupe_member_history")
      .filter(s"is_invalid = false and to_date(last_updated_date) > '$businessdate'")
      .select("new_member_key","last_updated_date").distinct()
      .persist

    val dimStoreMasterDF = hiveContext.table(s"$goldDB.dim_store_master")
      .filter("status = 'current'")
      .select("store_id","store_key")
      .persist

    val ecomOrder = hiveContext.table(s"$smithDB.ecomm_order_translation")
      .select("trxn_id", "exp_order_id","ingest_date")
      .withColumn("rank", row_number() over Window.partitionBy("trxn_id").orderBy(desc("ingest_date")))
        .filter("rank = 1")

    logger.info("--------Joining the ftd and fdmh----------")

    val ftds_fdmh= facttds
      .join(broadcast(factdmh),facttds("member_key") === factdmh("new_member_key"))
      .drop("new_member_key")
      .withColumnRenamed("store_key","trxn_store_key")

    logger.info("---------Finding store_id after join----------")

    val storejoin= getStoreId(ftds_fdmh,dimStoreMasterDF,"trxn_store_key","store_id")
      .withColumnRenamed("trxn_store_key","store_key")

    val expOrderid = storejoin
      .join(ecomOrder,col("trxn_id") === col("trxn_id_ftd"),"left")
      //.withColumn("dedupe_run_date",to_date(col("last_updated_date")))
      .dropColumns(Seq("trxn_id","trxn_id_ftd","rank","ingest_date"))
      //.withColumn("cc_flag",lit("A"))
//      .withColumn("run_date",current_date())
//    .select("store_id","member_key","trxn_date","trxn_nbr","register_nbr","exp_order_id","dedupe_run_date","cc_flag","run_date")
      .select("member_key","trxn_date","store_id","trxn_nbr","exp_order_id","register_nbr","last_updated_date")
      .distinct()

    val transformedDF=transformData(A_C_check=Some(col("ingest_date")),sourceDF = expOrderid,run_date = rundate,business_date = businessdate)

    logger.info("=========Calling load=========")

    load(transformedDF)
  }
}