package com.express

import java.text.SimpleDateFormat
import java.util.Calendar

import com.express.cdw.CDWContext
import com.express.cdw.spark.DataFrameUtils._
import com.express.util.Settings
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object kpi_transaction extends CDWContext with LazyLogging {

  def main(args: Array[String]): Unit = {

    val batch_id = args(0)
    val business_date = args(2)
    val date = Calendar.getInstance.getTime
    val date_format = new SimpleDateFormat("yyyy-MM")
    val timestamp_format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val tmpFactKpiReportTable = s"$workDB.fact_kpi_report_temp"

    // tables required for base table  calculations

    hiveContext.sql("select purchase_qty,discount_amount,captured_loyalty_id," +
      "implied_loyalty_id,ring_code_used,trxn_date,trxn_id,store_key,member_key from gold.fact_transaction_detail " +
      s"where batch_id = $batch_id").persist().registerTempTable("temp_fact_transaction_detail")

    hiveContext.sql("select reward_certs_qty,trxn_date,ttl_amount,has_tender,ttl_amount_after_discount " +
      s"from gold.fact_transaction_summary where batch_id = $batch_id").persist().registerTempTable("temp_fact_transaction_summary")

    val kpi_id_df = hiveContext.sql("select kpi_id,kpi_name,metrics from gold.dim_kpi where status='current'").persist()

    def kpi_id_fn(kpi_name_cnd: String): String = {
      kpi_id_df.filter(s"kpi_name='$kpi_name_cnd'").select("kpi_id").collect().mkString(" ").replaceAll("[\\[\\]]", "")
    }

    logger.info("Required tables are persisted")

    // base table calculations for fact_transaction_details

    val product_sold_cnt = hiveContext.sql(s"select sum(purchase_qty) kpi_attribute_value,trxn_date business_date from temp_fact_transaction_detail group by trxn_date")
      .withColumn("kpi_id", lit(kpi_id_fn("PRODUCTS SOLD")))
      .withColumn("kpi_attribute_name", lit(null))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val discount_amount_cnt = hiveContext.sql(s"select sum(discount_amount) kpi_attribute_value,trxn_date business_date from temp_fact_transaction_detail group by trxn_date")
      .withColumn("kpi_id", lit(kpi_id_fn("DISCOUNTS")))
      .withColumn("kpi_attribute_name", lit(null))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val captured_loyalty_id_cnt = hiveContext.sql(s"select trxn_date business_date, count(distinct trxn_id) kpi_attribute_value from temp_fact_transaction_detail where captured_loyalty_id is not null group by trxn_date")
      .withColumn("kpi_id", lit(kpi_id_fn("CAPTURED LOYALTY TRANSACTIONS")))
      .withColumn("kpi_attribute_name", lit(null))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val implied_loyalty_id_cnt = hiveContext.sql(s"select trxn_date business_date, count(distinct trxn_id) kpi_attribute_value from temp_fact_transaction_detail where implied_loyalty_id is not null group by trxn_date")
      .withColumn("kpi_id", lit(kpi_id_fn("IMPLIED LOYALTY TRANSACTIONS")))
      .withColumn("kpi_attribute_name", lit(null))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val trxn_with_reward_applied_cnt = hiveContext.sql(s"select trxn_date business_date, count(distinct trxn_id) kpi_attribute_value from temp_fact_transaction_detail where ring_code_used=1417 group by trxn_date")
      .withColumn("kpi_id", lit(kpi_id_fn("TRANSACTIONS WITH REWARD APPLIED – DETAIL")))
      .withColumn("kpi_attribute_name", lit(null))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val trxn_with_ring_code_8888_cnt = hiveContext.sql(s"select trxn_date business_date, count(distinct trxn_id) kpi_attribute_value from temp_fact_transaction_detail where ring_code_used=8888 group by trxn_date")
      .withColumn("kpi_id", lit(kpi_id_fn("TRANSACTIONS WITH RING CODE 8888")))
      .withColumn("kpi_attribute_name", lit(null))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")


    // base table calculations for fact_transaction_summary

    val total_sale_after_discount_cnt = hiveContext.sql(s"select sum(ttl_amount_after_discount) kpi_attribute_value,trxn_date business_date from temp_fact_transaction_summary group by trxn_date")
      .withColumn("kpi_id", lit(kpi_id_fn("TOTAL SALE AFTER DISCOUNT")))
      .withColumn("kpi_attribute_name", lit(null))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val trxn_with_certificates_cnt = hiveContext.sql(s"select trxn_date business_date, count(1) kpi_attribute_value from temp_fact_transaction_summary where reward_certs_qty>0 group by trxn_date")
      .withColumn("kpi_id", lit(kpi_id_fn("TRANSACTIONS WITH REWARD APPLIED - SUMMARY")))
      .withColumn("kpi_attribute_name", lit(null))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val trxn_with_zero_dollar_no_tender_cnt = hiveContext.sql(s"select trxn_date business_date, count(1) kpi_attribute_value from temp_fact_transaction_summary where ttl_amount=0 and has_tender='NO' group by trxn_date")
      .withColumn("kpi_id", lit(kpi_id_fn("TRANSACTIONS WITH NO TENDER")))
      .withColumn("kpi_attribute_name", lit(null))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val dimensionMaxSurrogateKey = hiveContext.table(s"$goldDB.fact_kpi_report").maxKeyValue(col("kpi_report_id"))

    val union_df = Seq(product_sold_cnt, total_sale_after_discount_cnt, discount_amount_cnt, captured_loyalty_id_cnt, implied_loyalty_id_cnt, trxn_with_reward_applied_cnt, trxn_with_ring_code_8888_cnt, trxn_with_certificates_cnt
      , trxn_with_zero_dollar_no_tender_cnt)

    val final_df = union_df.reduce(_ unionAll _)
      .withColumn("year_month", lit(date_format.format(date)))
      .withColumn("last_updated_date", lit(timestamp_format.format(date)))
      .withColumn("batch_id", lit(batch_id.trim))
      .generateSequence(dimensionMaxSurrogateKey, Some("kpi_report_id"))
      .select("kpi_report_id", "kpi_id", "kpi_attribute_name", "kpi_attribute_value", "business_date", "last_updated_date", "batch_id", "year_month")

    // writing data to base table

    final_df.coalesce(5).write.mode(SaveMode.Append).partitionBy("year_month").insertInto(tmpFactKpiReportTable)

    logger.info("Base table calculations complete and Data written to base table")

    hiveContext.sql("select * from gold.fact_kpi_report where kpi_id in (2,3,4,8,9,10,11,12,14) union all select * from work.fact_kpi_report_temp").persist().registerTempTable("temp_fact_kpi_report")

    // aggregation table calculations


    // hiveContext.sql("select kpi_report_id,kpi_id,kpi_attribute_name,kpi_attribute_value,business_date,last_updated_date,batch_id,year_month from work.fact_kpi_report_temp").persist().registerTempTable("temp_fact_kpi_report")

    val business_date_df = hiveContext.sql(s"select kpi_id,kpi_attribute_name,sum(kpi_attribute_value) as business_date_cnt from temp_fact_kpi_report where business_date='$business_date' " +
      s"group by kpi_attribute_name,kpi_id").na.fill("null",Seq("kpi_attribute_name")) //.registerTempTable("business_date_cnt")

    val business_date_minus_one_df = hiveContext.sql(s"select kpi_id,kpi_attribute_name,sum(kpi_attribute_value) as business_date_minus_one from temp_fact_kpi_report where business_date=date_sub('$business_date',1) " +
      s"group by kpi_attribute_name,kpi_id").na.fill("null",Seq("kpi_attribute_name")) //.registerTempTable("business_date,minus_one_cnt")

    val sdlw_df = hiveContext.sql(s"select kpi_id,kpi_attribute_name,sum(kpi_attribute_value) as sdlw from temp_fact_kpi_report where business_date=date_sub('$business_date',7)" +
      s"group by kpi_attribute_name,kpi_id").na.fill("null",Seq("kpi_attribute_name")) //.registerTempTable("sdlw_cnt")


    val wtd_val_kpi_df = hiveContext.sql(s"select kpi_id,kpi_attribute_name,sum(kpi_attribute_value) as wtd_val from temp_fact_kpi_report where business_date between DATE_SUB('$business_date', CAST(DATE_FORMAT('$business_date','u')%7 AS INT)) and '$business_date'" +
      s"group by kpi_attribute_name,kpi_id").na.fill("null",Seq("kpi_attribute_name")) //.registerTempTable("wtd_val_kpi_cnt")


    val last_week_wtd_val_df = hiveContext.sql(s"select kpi_id,kpi_attribute_name,sum(kpi_attribute_value) as last_week_wtd_val from temp_fact_kpi_report where business_date between DATE_SUB(DATE_SUB('$business_date',7), CAST(DATE_FORMAT(DATE_SUB('$business_date',7),'u')%7 AS INT)) and date_sub('$business_date',7)" +
      s"group by kpi_attribute_name,kpi_id").na.fill("null",Seq("kpi_attribute_name")) //.registerTempTable("last_week_wtd_val_cnt")


    val mtd_df = hiveContext.sql(s"select kpi_id,kpi_attribute_name,sum(kpi_attribute_value) as mtd from temp_fact_kpi_report where business_date between date_format('$business_date','yyyy-MM-01') and '$business_date'" +
      s" group by kpi_attribute_name,kpi_id").na.fill("null",Seq("kpi_attribute_name")) //.registerTempTable("mtd_cnt")

    val ytd_df = hiveContext.sql(s"select kpi_id,kpi_attribute_name,sum(kpi_attribute_value) as ytd from temp_fact_kpi_report where business_date between date_format('$business_date','yyyy-01-01') and '$business_date'" +
      s" group by kpi_attribute_name,kpi_id").na.fill("null",Seq("kpi_attribute_name")) //.registerTempTable("ytd_cnt")

    logger.info("Aggregate table calculations complete")

    val final_kpi = business_date_df.join(business_date_minus_one_df, Seq("kpi_id", "kpi_attribute_name"), "fullouter")
      .join(sdlw_df, Seq("kpi_id", "kpi_attribute_name"), "fullouter")
      .join(wtd_val_kpi_df, Seq("kpi_id", "kpi_attribute_name"), "fullouter")
      .join(last_week_wtd_val_df, Seq("kpi_id", "kpi_attribute_name"), "fullouter")
      .join(mtd_df, Seq("kpi_id", "kpi_attribute_name"), "fullouter")
      .join(ytd_df, Seq("kpi_id", "kpi_attribute_name"), "fullouter")
      .select("kpi_id", "kpi_attribute_name", "business_date_cnt", "business_date_minus_one", "sdlw", "wtd_val", "last_week_wtd_val", "mtd", "ytd")


    val final_kpi_values = final_kpi.join(kpi_id_df, Seq("kpi_id"), "inner")
      .select("metrics", "kpi_name", "kpi_attribute_name", "business_date_cnt", "business_date_minus_one", "sdlw", "wtd_val", "last_week_wtd_val", "mtd", "ytd")
      .withColumn("change_perc_business_date", ((col("business_date_cnt") / col("business_date_minus_one")) - 1) * 100)
      .withColumn("change_per_sdlw", ((col("business_date_cnt") / col("sdlw")) - 1) * 100)
      .withColumn("change_per_wtd", ((col("wtd_val") / col("last_week_wtd_val")) - 1) * 100)
      .withColumn("business_date", to_date(lit(args(2))))
      .withColumn("run_date", current_date())
      .select("metrics","kpi_name", "kpi_attribute_name", "business_date_cnt", "business_date_minus_one", "change_perc_business_date", "sdlw", "change_per_sdlw", "wtd_val", "last_week_wtd_val", "change_per_wtd", "mtd", "ytd", "business_date", "run_date")

    logger.info("Writing aggregate data in oracle")
    // variable for jdbc connection

    val audit_table = "nifi_tracking.kpi_aggregate"
    val jdbcUrl = Settings.getJDBCUrl
    val connectionProperties = Settings.getConnectionProperties

    // writing aggregated data to oracle

    final_kpi_values.coalesce(5).write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, audit_table, connectionProperties)

    logger.info("Aggregate data to oracle written")

  }
}