package com.express

import java.text.SimpleDateFormat
import java.util.Calendar

import com.express.cdw.CDWContext
import com.express.cdw.spark.DataFrameUtils._
import com.express.util.Settings
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._



object kpi_base extends CDWContext with LazyLogging {

  def main(args: Array[String]): Unit = {

    val batch_id = args(0)
    val previous_batch_id = args(1)
    val business_date = args(2)
    val date = Calendar.getInstance.getTime
    val date_format = new SimpleDateFormat("yyyy-MM")
    val timestamp_format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val tmpFactKpiReportTable = s"$workDB.fact_kpi_report_temp"

    // tables required for base table  calculations

    hiveContext.sql("select purchase_qty,discount_amount,captured_loyalty_id," +
      "implied_loyalty_id,ring_code_used,trxn_date,trxn_id,store_key,member_key from gold.fact_transaction_detail " +
      s"where batch_id = $batch_id").persist().registerTempTable("temp_fact_transaction_detail")


    hiveContext.sql(s"select tender_type_key,trxn_date,trxn_id " +
      s"from gold.fact_tender_history where batch_id = $batch_id").persist().registerTempTable("temp_fact_tender_history")

    hiveContext.sql("select tender_type_description,tender_type_key from gold.dim_tender_type " +
      s"where status='current'").persist().registerTempTable("temp_dim_tender_type")

    hiveContext.sql("select reward_certs_qty,trxn_date,ttl_amount,has_tender,ttl_amount_after_discount " +
      s"from gold.fact_transaction_summary where batch_id = $batch_id").persist().registerTempTable("temp_fact_transaction_summary")

    hiveContext.sql("select consent_value_retail,consent_value_outlet,consent_value_next " +
      s"from gold.dim_member_multi_email where status='current'").persist().registerTempTable("temp_dim_member_multi_email")

    hiveContext.sql("select member_key,loyalty_id,tier_name,is_express_plcc,preferred_channel,ip_code " +
      s"from gold.dim_member where status='current'").persist().registerTempTable("temp_dim_member")

    hiveContext.sql("select employee_status,member_key from gold.dim_employee where status='current' and employee_status<>'T'").persist().registerTempTable("temp_dim_employee")

    hiveContext.sql("select retail_web_flag,store_key,zone_code from gold.dim_store_master where status='current' " +
      s"and open_closed_ind='OPEN'").persist().registerTempTable("temp_dim_store_master")

    hiveContext.sql("select member_key,tier_name from backup.bkp_dim_member where status='current' " +
      s" and  batch_id=$previous_batch_id").persist().registerTempTable("temp_bkp_dim_member")

    val kpi_id_df = hiveContext.sql("select kpi_id,kpi_name,metrics from gold.dim_kpi where status='current'").persist()

    def kpi_id_fn(kpi_name_cnd: String): String = {
      kpi_id_df.filter(s"kpi_name='$kpi_name_cnd'").select("kpi_id").collect().mkString(" ").replaceAll("[\\[\\]]", "")
    }


    logger.info("------------KPI base calculations starting--------------------")
    // base table calculations

    val consent_value_retail_cnt = hiveContext.sql(s"select consent_value_retail kpi_attribute_name, count(1) kpi_attribute_value from temp_dim_member_multi_email group by consent_value_retail")
      .withColumn("kpi_id", lit(kpi_id_fn("CONSENT VALUE RETAIL")))
      .withColumn("business_date", lit(args(2)))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val consent_value_outlet_cnt = hiveContext.sql(s"select consent_value_outlet kpi_attribute_name, count(1) kpi_attribute_value from temp_dim_member_multi_email  group by consent_value_outlet")
      .withColumn("kpi_id", lit(kpi_id_fn("CONSENT VALUE OUTLET")))
      .withColumn("business_date", lit(args(2)))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val consent_value_next_cnt = hiveContext.sql(s"select consent_value_next kpi_attribute_name, count(1) kpi_attribute_value from temp_dim_member_multi_email  group by consent_value_next")
      .withColumn("kpi_id", lit(kpi_id_fn("CONSENT VALUE NEXT")))
      .withColumn("business_date", lit(args(2)))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val is_express_plcc_cnt = hiveContext.sql(s"select is_express_plcc kpi_attribute_name, count(1) kpi_attribute_value from temp_dim_member  group by is_express_plcc")
      .withColumn("kpi_id", lit(kpi_id_fn("IS PLCC")))
      .withColumn("business_date", lit(args(2)))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val member_key_cnt = hiveContext.sql(s"select count(member_key) kpi_attribute_value from temp_dim_member ")
      .withColumn("kpi_id", lit(kpi_id_fn("MEMBER COUNT")))
      .withColumn("kpi_attribute_name", lit(null))
      .withColumn("business_date", lit(args(2)))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val loyalty_id_cnt = hiveContext.sql(s"select count(loyalty_id) kpi_attribute_value from temp_dim_member where  loyalty_id is not null and trim(loyalty_id)<>''")
      .withColumn("kpi_id", lit(kpi_id_fn("LOYALTY MEMBERS COUNT")))
      .withColumn("kpi_attribute_name", lit(null))
      .withColumn("business_date", lit(args(2)))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val loyalty_id_cnt_ipcode = hiveContext.sql(s"select count(loyalty_id) kpi_attribute_value from temp_dim_member where  length(loyalty_id)=13  and ip_code is not null")
      .withColumn("kpi_id", lit(kpi_id_fn("LOYALTY MEMBERS WITH LENGTH OF LOYALTY ID=13 AND IPCODE NOT NULL COUNT")))
      .withColumn("kpi_attribute_name", lit(null))
      .withColumn("business_date", lit(args(2)))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val aList_members_cnt = hiveContext.sql(s"select count(member_key) kpi_attribute_value from temp_dim_member where  tier_name='A-List'")
      .withColumn("kpi_id", lit(kpi_id_fn("A-LIST TIER")))
      .withColumn("kpi_attribute_name", lit(null))
      .withColumn("business_date", lit(args(2)))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val baseTier_member_cnt = hiveContext.sql(s"select count(member_key) kpi_attribute_value from temp_dim_member where  tier_name='BaseTier'")
      .withColumn("kpi_id", lit(kpi_id_fn("BASE TIER")))
      .withColumn("kpi_attribute_name", lit(null))
      .withColumn("business_date", lit(args(2)))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val base_to_alist_movement_cnt = hiveContext.sql(s"select count(a.member_key) kpi_attribute_value from " +
      s"(select member_key from temp_dim_member where tier_name='A-List')a " +
      s"inner join " +
      s"(select member_key from temp_bkp_dim_member where tier_name='BaseTier')b " +
      s"on a.member_key=b.member_key")
      .withColumn("kpi_id", lit(kpi_id_fn("MOVEMENT FROM BASE TIER TO A-LIST TIER")))
      .withColumn("kpi_attribute_name", lit(null))
      .withColumn("business_date", lit(args(2)))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val alist_to_base_movement_cnt = hiveContext.sql(s"select count(a.member_key) kpi_attribute_value from " +
      s"(select member_key from temp_dim_member where tier_name='BaseTier')a " +
      s"inner join " +
      s"(select member_key from temp_bkp_dim_member where tier_name='A-List')b " +
      s"on a.member_key=b.member_key")
      .withColumn("kpi_id", lit(kpi_id_fn("MOVEMENT FROM A-LIST TIER TO BASE TIER")))
      .withColumn("kpi_attribute_name", lit(null))
      .withColumn("business_date", lit(args(2)))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val tender_type_cnt = hiveContext.sql(s"select trxn_date business_date,tender_type_description kpi_attribute_name, count(distinct trxn_id) kpi_attribute_value from " +
      s"(select tender_type_key,trxn_id,trxn_date from  temp_fact_tender_history  ) fth " +
      s"left join " +
      s"(select tender_type_key,tender_type_description from temp_dim_tender_type ) dtt " +
      s"on fth.tender_type_key=dtt.tender_type_key " +
      s"group by trxn_date,tender_type_description")
      .withColumn("kpi_id", lit(kpi_id_fn("TENDER TYPE")))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val emp_trxn_cnt = hiveContext.sql(s"select trxn_date business_date,count(distinct trxn_id) kpi_attribute_value from temp_dim_employee a " +
      s"inner join temp_fact_transaction_detail b " +
      s"on a.member_key=b.member_key " +
      s"group by trxn_date")
      .withColumn("kpi_id", lit(kpi_id_fn("EMPLOYEE TRANSACTIONS")))
      .withColumn("kpi_attribute_name", lit(null))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")

    val trxn_by_channel_cnt = hiveContext.sql(s"select ftd.trxn_date business_date,case when zone_code =8 Then 'EFO' when zone_code in (1,2) then 'RETAIL' when zone_code =6 then 'EComm' end as zone_code,count(1) kpi_attribute_value from " +
      s"(select store_key,trxn_date from temp_fact_transaction_detail ) ftd " +
      s"left join " +
      s"(select store_key,zone_code from temp_dim_store_master ) dsm " +
      s"on ftd.store_key=dsm.store_key group by ftd.trxn_date,zone_code")
      .withColumn("kpi_id", lit(kpi_id_fn("TRANSACTION CHANNEL")))
      .withColumn("kpi_attribute_name", lit(col("zone_code")))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")


    val loyalty_trxn_by_tier_cnt = hiveContext.sql(s"select trxn_date business_date,tier_name kpi_attribute_name,count(distinct trxn_id) kpi_attribute_value from " +
      s"( select member_key,tier_name from temp_dim_member where loyalty_id is not null and loyalty_id<>'')a " +
      s"inner join " +
      s"(select member_key,trxn_date,trxn_id from temp_fact_transaction_detail where (captured_loyalty_id is not null or implied_loyalty_id is not null)) b " +
      s"on a.member_key=b.member_key group by tier_name,trxn_date")
      .withColumn("kpi_id", lit(kpi_id_fn("LOYALTY TRANSACTIONS BY TIER")))
      .select("kpi_attribute_value", "kpi_attribute_name", "kpi_id", "business_date")


    val dimensionMaxSurrogateKey = hiveContext.table(s"$goldDB.fact_kpi_report").maxKeyValue(col("kpi_report_id"))


    val union_df = Seq(consent_value_retail_cnt, consent_value_outlet_cnt, consent_value_next_cnt, is_express_plcc_cnt, member_key_cnt, loyalty_id_cnt, loyalty_id_cnt_ipcode
      , aList_members_cnt, baseTier_member_cnt, base_to_alist_movement_cnt, alist_to_base_movement_cnt, tender_type_cnt, emp_trxn_cnt, trxn_by_channel_cnt,loyalty_trxn_by_tier_cnt)


    val final_df = union_df.reduce(_ unionAll _)
      .withColumn("year_month", lit(date_format.format(date)))
      .withColumn("last_updated_date", lit(timestamp_format.format(date)))
      .withColumn("batch_id", lit(batch_id.trim))
      .generateSequence(dimensionMaxSurrogateKey, Some("kpi_report_id"))
      .select("kpi_report_id", "kpi_id", "kpi_attribute_name", "kpi_attribute_value", "business_date", "last_updated_date", "batch_id", "year_month")


    logger.info("------------Inserting into KPI base table--------------------")

    //writing data to base table

    final_df.coalesce(5).write.mode(SaveMode.Append).partitionBy("year_month").insertInto(tmpFactKpiReportTable)

    logger.info("------------Successfully Inserted into KPI base table--------------------")

    //aggregation table calculations

    hiveContext.sql("select * from gold.fact_kpi_report where kpi_id not in (2,3,4,8,9,10,11,12,14) union all select * from work.fact_kpi_report_temp").persist().registerTempTable("temp_fact_kpi_report")

    logger.info("------------KPI aggregate calculations started--------------------")

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

    // variable for jdbc connection

    val audit_table = "nifi_tracking.kpi_aggregate"
    val jdbcUrl = Settings.getJDBCUrl
    val connectionProperties = Settings.getConnectionProperties

    logger.info("------------Inserting into KPI aggregate table started--------------------")

    // writing aggregated data to oracle
    final_kpi_values.coalesce(5).write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, audit_table, connectionProperties)

    logger.info("------------Successfully Inserted into KPI aggregate table--------------------")

  }
}