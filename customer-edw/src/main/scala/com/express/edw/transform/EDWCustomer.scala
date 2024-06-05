package com.express.edw.transform

import org.apache.spark.sql.functions.{max, _}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.expressions.Window
import com.express.cdw.spark.udfs.{CheckNotEmptyUDF, DistanceTravelled}
import com.express.cdw.spark.DataFrameUtils._



/**
  * Created by Mahendranadh.Dasari on 25-03-2019.
  */

object EDWCustomer extends SmithLoad with LazyLogging{

override def tableName: String = "edw_dim_member_history"

  private val NotNull = CheckNotEmptyUDF

  override def sourcetable: String = s"$goldDB.dim_member"

  override def filterCondition: String = "status='current' and (record_type != 'PROSPECT' or record_type is null)"

  override def sourceColumnList: Seq[String] = Seq("member_key","household_key","first_name","last_name","address1","address2","city","state","zip_code","zip4","first_name_scrubbed","last_name_scrubbed","address1_scrubbed","address2_scrubbed","city_scrubbed","state_scrubbed","zip_code_scrubbed","zip4_scrubbed","zip_full_scrubbed","non_us_postal_code","country_code","country_code_scrubbed","valid_strict","valid_loose","valid_email","valid_phone_for_sms","phone_nbr","phone_type","email_address","loyalty_id","gender","direct_mail_consent","email_consent","sms_consent","birth_date","deceased","address_is_prison","customer_add_date","customer_introduction_date","latitude","longitude","closest_store_key","second_closest_store_key","distance_to_closest_store","distance_to_sec_closest_store","preferred_store_key","distance_to_preferred_store","is_express_plcc","first_trxn_date","ads_do_not_statement_insert","ads_do_not_sell_name","ads_spam_indicator","ads_email_change_date","ads_return_mail_ind","cm_gender_merch_descr","is_dm_marketable","is_em_marketable","is_sms_marketable","record_type","email_consent_date","sms_consent_date","direct_mail_consent_date","preferred_channel","is_loyalty_member","tier_name","member_status","member_acct_open_date","member_acct_close_date","member_acct_enroll_date","ncoa_last_change_date","last_store_purch_date","last_web_purch_date","lw_enrollment_store_key","lw_enrollment_source_key","last_updated_date","first_trxn_store_key")

  def main(args: Array[String]): Unit = {
    val options = parse(args)
    val businessdate = options("businessdate")
    val rundate = options("rundate")

    import hiveContext.implicits._
    val dimStoreMasterDF = hiveContext.sql(s"select store_id,store_key,retail_web_flag,open_closed_ind,zone_code from $goldDB.dim_store_master where status='current' and (store_key != 0 OR store_key != -1)").persist()

    val outputTable=hiveContext.table(s"$smithDB.edw_dim_member_history")


    val factTrxnSummDF = hiveContext.sql(s"select trxn_id,trxn_date,store_key,member_key,w_units,m_units,w_amount_after_discount,m_amount_after_discount from $goldDB.fact_transaction_summary where member_key is not null and member_key > 0")
      .withColumnRenamed("trxn_id", "summ_trxn_id")
      .withColumnRenamed("trxn_date", "summ_trxn_date")
      .withColumnRenamed("member_key", "summ_member_key")
      .withColumnRenamed("store_key", "summ_store_key")

    val wt = Window.partitionBy("trxn_id")
    val ecommOrdTrxnDF = hiveContext.sql(s"select trxn_id,trxn_date,demandloc from $smithDB.ecomm_order_translation where member_key is not null and member_key > 0")
      .withColumn("rank", row_number().over(wt))
      .filter("rank = 1")
      .withColumnRenamed("trxn_id", "ecomm_trxn_id")
      .withColumnRenamed("trxn_date", "ecomm_trxn_date")
      .withColumn("demandloc", when(col("demandloc") === lit("NULL") || col("demandloc").isNull, lit(null)).otherwise(col("demandloc")))


    val FchDF = hiveContext.sql(s"select member_key,account_open_date,closed_date from $goldDB.fact_card_history where card_type_key=1 and source_key in (8,9)").withColumnRenamed("member_key", "fch_member_key")
      .withColumn("account_open_date", when(col("account_open_date") >= current_date() || col("account_open_date").isNull, lit(null)).otherwise(col("account_open_date")))
      .withColumn("closed_date", when(col("closed_date") >= current_date() || col("closed_date").isNull, lit(null)).otherwise(col("closed_date")))
      .groupBy(col("fch_member_key"))
      .agg(
        max(col("account_open_date")).alias("plcc_open_date"),
        max(col("closed_date")).alias("plcc_closed_date")
      )


    val AdobeMMmeIdMappingDF = hiveContext.sql(s"select mme_id as adobe_mme_id,member_key as adobe_member_key from cmhpcdw.adobe_mmeid_mapping where length(trim(mme_id))>0")
      .withColumn("rnk",row_number().over(Window.partitionBy("adobe_mme_id","adobe_member_key")))
      .filter("rnk=1").drop("rnk")

    val OmnitureWebDF = hiveContext.table(s"$goldDB.omniture_web")
      .withColumn("post_email_recepient", when($"post_email_recepient" === "" || $"post_email_recepient" === lit("N.A.") || length(trim($"post_email_recepient"))=== lit(0) , lit(null)).otherwise(col("post_email_recepient")))
      .withColumn("post_loyalty_id", when(col("post_loyalty_id") === "" || col("post_loyalty_id") === lit("N.A."), lit(null)).otherwise(col("post_loyalty_id")))
    .selectExpr("coalesce(post_email_recepient,email_recepient_id) as post_email_recepient","coalesce(post_loyalty_id,loyalty_id) as post_loyalty_id","ingest_date as web_ingest_date")
      .filter("(length(trim(post_email_recepient))>0 or length(trim(post_loyalty_id))=13) and web_ingest_date=date_sub(current_date(),1)")


    val OmnitureMobileDF = hiveContext.table(s"$goldDB.omniture_mobile").selectExpr("mme_id as mobile_mme_id","loyalty_id as mobile_loyalty_id","ingest_date as mobile_ingest_date")
        .filter("(length(trim(mobile_mme_id))>0 or length(trim(mobile_loyalty_id))=13) and mobile_ingest_date=date_sub(current_date(),1)")


    val storeMasterRetailWebDF = dimStoreMasterDF.filter(s"retail_web_flag in( 'WEB','RETAIL') AND open_closed_ind = 'OPEN' ").persist()
    val storeMasterZonecodeDF = dimStoreMasterDF.filter(s"zone_code in (1,2,6,8)").persist()

    val dimMemberGoldDF = hiveContext.sql(s"select member_key,household_key,first_name,last_name,address1,address2,city,state,zip_code,zip4,first_name_scrubbed,last_name_scrubbed,address1_scrubbed,address2_scrubbed,city_scrubbed,state_scrubbed,zip_code_scrubbed,zip4_scrubbed,zip_full_scrubbed,non_us_postal_code,country_code,country_code_scrubbed,valid_strict,valid_loose,valid_email,valid_phone_for_sms,phone_nbr,phone_type,email_address,loyalty_id,gender,direct_mail_consent,email_consent,sms_consent,birth_date,deceased,address_is_prison,customer_add_date,customer_introduction_date,latitude,longitude,closest_store_key,second_closest_store_key,distance_to_closest_store,distance_to_sec_closest_store,preferred_store_key,distance_to_preferred_store,is_express_plcc,first_trxn_date,ads_do_not_statement_insert,ads_do_not_sell_name,ads_spam_indicator,ads_email_change_date,ads_return_mail_ind,cm_gender_merch_descr,is_dm_marketable,is_em_marketable,is_sms_marketable,record_type,email_consent_date,sms_consent_date,direct_mail_consent_date,preferred_channel,is_loyalty_member,tier_name,member_status,member_acct_open_date,member_acct_close_date,member_acct_enroll_date,ncoa_last_change_date,last_store_purch_date,last_web_purch_date,lw_enrollment_store_key,lw_enrollment_source_key,last_updated_date,date(last_updated_date) as ingest_date,batch_id,first_trxn_store_key from $goldDB.dim_member where status='current' and (record_type != 'PROSPECT' OR  record_type is null) ")

    val joinedMemberForStoreid = dimMemberGoldDF.updateKeys(Map("lw_enrollment_store_key"->"lw_enrollment_store_id","closest_store_key"->"closest_store_id","second_closest_store_key"->"second_closest_store_id","first_trxn_store_key"->"first_trxn_store_id"), dimStoreMasterDF, "store_key", "store_id")

    val age = round(months_between(current_date(), col("birth_date")) / 12)

    val FindAgeAndAgeQualifier =joinedMemberForStoreid
      .withColumn("customer_age", when((year(col("birth_date")) <= 1900) || col("birth_date") > current_date() || col("birth_date").isNull, lit(null)).otherwise(age))
      .withColumn("birth_date", when((year(col("birth_date")) <= 1900) || col("birth_date") > current_date() || col("birth_date").isNull, lit(null)).otherwise(col("birth_date")))
      .withColumn("customer_age_qualifier", when(col("customer_age") > 0, lit("X")).otherwise(lit("U")))

    //val JoinedMemberWithFCH = dimMemberGoldDF.join(FchDF,col("member_key") === col("fch_member_key"),"left")

    val getPlccOpenClosed = leftJoin(FindAgeAndAgeQualifier, FchDF, "member_key", "fch_member_key")

    logger.info(s"<-------------Find Age completed-------------->")


    val joinedTrxnSummWithEcommOrdTrxn = leftJoin(factTrxnSummDF, ecommOrdTrxnDF, "summ_trxn_id", "ecomm_trxn_id")
    val joinedTrxnSummWithRetailStoreLast12Months = joinedTrxnSummWithEcommOrdTrxn.filter(s"summ_trxn_date >= add_months(current_date(), -12) ")

    val joinedTrxnSummWithRetailStore = leftJoin(joinedTrxnSummWithEcommOrdTrxn, storeMasterRetailWebDF, "summ_store_key", "store_key")

    val getLastStoreTrxnDate = joinedTrxnSummWithRetailStore
      .withColumn("ecomm_trxn_date",when($"retail_web_flag" === "RETAIL",$"ecomm_trxn_date").otherwise(lit(null)))
      .withColumn("summ_trxn_date",when($"retail_web_flag" === "RETAIL",$"summ_trxn_date").otherwise(lit(null)))
      .withColumn("last_store_trxn_date",coalesce($"ecomm_trxn_date",$"summ_trxn_date"))
      .withColumn("ecomm_trxn_date",when($"retail_web_flag" === "WEB",$"ecomm_trxn_date").otherwise(lit(null)))
      .withColumn("summ_trxn_date",when($"retail_web_flag" === "WEB",$"summ_trxn_date").otherwise(lit(null)))
      .withColumn("last_web_trxn_date",coalesce($"ecomm_trxn_date",$"summ_trxn_date"))
      .groupBy(col("summ_member_key"))
      .agg(
        max("last_store_trxn_date").alias("last_store_trxn_date"),
          max("last_web_trxn_date").alias("last_web_trxn_date")
      ).dropColumns(Seq("ecomm_trxn_date","summ_trxn_date"))


    val JoinedMemberWithSummEcommRetailStore = leftJoin(getPlccOpenClosed, getLastStoreTrxnDate, "member_key", "summ_member_key")


    logger.info(s"<-------------last_web_trxn_date completed-------------->" )

    val getbrandFirstTrxnDate = joinedTrxnSummWithEcommOrdTrxn
      .filter(s"summ_trxn_date >= add_months(current_date(), -24) ")
      .groupBy(col("summ_member_key"))
      .agg(
        min(col("summ_trxn_date")).alias("brand_first_trxn_date")
      )

    val joinMemberForBrandFirstTrxnDate = leftJoin(JoinedMemberWithSummEcommRetailStore, getbrandFirstTrxnDate, "member_key", "summ_member_key")

    val genderMerchUnitsSpends = factTrxnSummDF
      .groupBy(col("summ_member_key"))
      .agg(
        sum(col("w_units")).alias("sum_w_units"),
        sum(col("m_units")).alias("sum_m_units"),
        sum(col("w_amount_after_discount")).alias("sum_w_amount_after_discount"),
        sum(col("m_amount_after_discount")).alias("sum_m_amount_after_discount")
      )
      .withColumn("gender_merch_units", when(col("sum_m_units") === 0 and col("sum_w_units") > 0, lit("WO"))
        when(col("sum_w_units") === 0 and col("sum_m_units") > 0, lit("MO"))
        when(col("sum_w_units") === 0 and col("sum_m_units") === 0, lit(null))
        when(col("sum_w_units") > 0 and col("sum_m_units") > 0 and  col("sum_w_units") >= col("sum_m_units"), lit("DLW"))
        when(col("sum_w_units") > 0 and col("sum_m_units") > 0 and  col("sum_w_units") < col("sum_m_units"), lit("DLM")))
      .withColumn("gender_merch_spend", when(col("sum_m_amount_after_discount") === 0 and col("sum_w_amount_after_discount") > 0, lit("WO"))
        when(col("sum_w_amount_after_discount") === 0 and col("sum_m_amount_after_discount") > 0, lit("MO"))
        when(col("sum_w_amount_after_discount") === 0 and col("sum_m_amount_after_discount") === 0, lit(null))
        when(col("sum_w_amount_after_discount") > 0 and col("sum_m_amount_after_discount") > 0 and  col("sum_w_amount_after_discount") >= col("sum_m_amount_after_discount"), lit("DLW"))
        when(col("sum_w_amount_after_discount") > 0 and col("sum_m_amount_after_discount") > 0 and  col("sum_w_amount_after_discount") < col("sum_m_amount_after_discount"), lit("DLM")))
      .drop(col("sum_w_units"))
      .drop(col("sum_m_units"))
      .drop(col("sum_w_amount_after_discount"))
      .drop(col("sum_m_amount_after_discount"))

    val joinMemberForGenerMerchUnitsSpends = leftJoin(joinMemberForBrandFirstTrxnDate, genderMerchUnitsSpends, "member_key", "summ_member_key")

    logger.info(s"<-------------GenderMerchant completed-------------->" )

    val wms = Window.partitionBy("summ_member_key","store_id")
    val wmz = Window.partitionBy("summ_member_key","zone_code")
    val wm = Window.partitionBy("summ_member_key")

    val joinedTrxnSummForPreferredStore = joinedTrxnSummWithRetailStore
      .withColumn("demandloc",when($"retail_web_flag" === "RETAIL",$"demandloc").otherwise(lit(null)))
      .withColumn("store_id",when($"retail_web_flag" === "RETAIL",$"store_id").otherwise(lit(null)))
      .withColumn("store_id",coalesce($"demandloc",col("store_id")))
      .withColumn("ms_count", count("summ_trxn_id").over(wms))
      .withColumn("max_count_rank", row_number().over(wm.orderBy(desc("ms_count"), desc("summ_trxn_date"))))
      .filter("max_count_rank = 1")
      .select("summ_member_key","store_id")
      .withColumnRenamed("store_id", "preferred_store_id")

    val JoinMemberForPreferredStore = leftJoin(joinMemberForGenerMerchUnitsSpends, joinedTrxnSummForPreferredStore, "member_key", "summ_member_key")

    val getPrimaryStore = joinedTrxnSummWithRetailStore
      .filter(s"summ_trxn_date >= add_months(current_date(), -12) ")
      .withColumn("demandloc",when($"retail_web_flag" === "RETAIL",$"demandloc").otherwise(lit(null)))
      .withColumn("store_id",when($"retail_web_flag" === "RETAIL",$"store_id").otherwise(lit(null)))
      .withColumn("store_id",coalesce($"demandloc",col("store_id")))
      .withColumn("ms_count", count("summ_trxn_id").over(wms))
      .withColumn("max_count_rank", row_number().over(wm.orderBy(desc("ms_count"), desc("summ_trxn_date"))))
      .filter("max_count_rank = 1")
      .select("summ_member_key","store_id")
      .withColumnRenamed("store_id", "primary_store_id")

    val JoinMemberForPrimaryStore = leftJoin(JoinMemberForPreferredStore, getPrimaryStore, "member_key", "summ_member_key")

    logger.info(s"<-------------primary Store ID completed-------------->")

    val getSecondaryStore = joinedTrxnSummWithRetailStore
      .filter(s"summ_trxn_date >= add_months(current_date(), -12) ")
      .withColumn("demandloc",when($"retail_web_flag" === "RETAIL",$"demandloc").otherwise(lit(null)))
      .withColumn("store_id",when($"retail_web_flag" === "RETAIL",$"store_id").otherwise(lit(null)))
      .withColumn("store_id",coalesce($"demandloc",col("store_id")))
      .withColumn("ms_count", count("summ_trxn_id").over(wms))
      .withColumn("max_count_rank", row_number().over(wm.orderBy(desc("ms_count"), desc("summ_trxn_date"))))
      .filter("max_count_rank = 2")
      .select("summ_member_key","store_id")
      .withColumnRenamed("store_id", "secondary_store_id")

    val JoinMemberForSecondaryStore = leftJoin(JoinMemberForPrimaryStore, getSecondaryStore, "member_key", "summ_member_key")


    val joinedTrxnSummLast12monthsWithStoreZonecode = leftJoin(joinedTrxnSummWithRetailStoreLast12Months, storeMasterZonecodeDF, "summ_store_key", "store_key")

    logger.info(s"<-------------Zone Code completed-------------->")
    val getDominantShoppingChannel = joinedTrxnSummLast12monthsWithStoreZonecode
      .withColumn("summ_ecomm_store_id", when($"demandloc".isNull,col("store_id")).otherwise(col("demandloc")))
      .withColumn("ms_count", count("summ_trxn_id").over(wmz))
      .withColumn("max_count_rank", row_number().over(wm.orderBy(desc("ms_count"), desc("summ_trxn_date"))))
      .filter("max_count_rank = 1")
      .select("summ_member_key","zone_code")
      .withColumn("dominant_shopping_channel", when(col("zone_code") === 8 , lit("EFO"))
          .when(col("zone_code") === 6 , lit("Ecomm"))
          .when(col("zone_code") === 1 || col("zone_code") === 2, lit("Retail")).otherwise(lit(null)))
      .drop(col("zone_code"))

    val JoinMemberForDominantShoppingChannel = leftJoin(JoinMemberForSecondaryStore, getDominantShoppingChannel, "member_key", "summ_member_key")

    logger.info(s"<-------------Dominant Shopping -------------->")
    val storeDF = hiveContext.sql(s"select store_key,store_id,latitude,district_code,longitude,open_closed_ind" +
      s" from $goldDB.dim_store_master where status='current'")
      .withColumnRenamed("store_key", "store_key_dist")
      .withColumnRenamed("store_id", "store_id_dist")
      .withColumnRenamed("latitude", "latitude_store")
      .withColumnRenamed("longitude", "longitude_store")

    val getpreferedStoreDist = JoinMemberForDominantShoppingChannel.join(broadcast(storeDF), col("preferred_store_id") === col("store_id_dist"), "left").withColumn("distance_to_preferred_store",
      when(col("district_code") === lit(97),lit(-1))
        .when(not(isnull(col("latitude_store"))) and not(isnull(col("longitude_store"))) and not(isnull(col("latitude"))) and not(isnull(col("longitude"))) and col("open_closed_ind") === "OPEN",
          DistanceTravelled(col("latitude_store"),  col("latitude"), col("longitude_store"),col("longitude"))).otherwise(null))

    logger.info(s"<-------------WebUnion Started -------------->")
    val WebUnionMobile= OmnitureWebDF.unionAll(OmnitureMobileDF)
      .withColumn("rnk",row_number().over(Window.partitionBy("post_email_recepient","post_loyalty_id","web_ingest_date")))
      .filter("rnk=1").drop("rnk").repartition($"post_loyalty_id")

    val (matchOnLoyalty,unmatchedOnLoyalty)=WebUnionMobile.join(dimMemberGoldDF.filter("length(loyalty_id)=13"), trim($"post_loyalty_id") === trim($"loyalty_id"),"left")
      .partition(NotNull($"loyalty_id"))

    val JoinAdobeOnMmeId=AdobeMMmeIdMappingDF.join(unmatchedOnLoyalty,$"adobe_mme_id" === upper(trim($"post_email_recepient")))
      .select("adobe_member_key","web_ingest_date")

    val AdobeDataSet=JoinAdobeOnMmeId.unionAll(matchOnLoyalty.select("member_key","web_ingest_date")).distinct.repartition($"adobe_member_key")

    val LastBrowseCalculation=getpreferedStoreDist.join(AdobeDataSet,$"member_key" === $"adobe_member_key","left")
      .withColumn("last_browse_date",$"web_ingest_date")
    logger.info(s"<-------------Last_browse_date completed  -------------->")

    val lastDf =LastBrowseCalculation
      .withColumn("last_trxn_date",
        when(isnull(col("last_store_purch_date")) or col("last_store_purch_date") < col("last_web_purch_date"),
          col("last_web_purch_date")).otherwise(col("last_store_purch_date")))
      .withColumn("customer_next_conv_status", lit(""))
      .withColumn("gender",
        when(trim(upper(col("gender"))) === "M", lit("MALE"))
          .when(trim(upper(col("gender"))) === "F", lit("FEMALE"))
          .when(trim(upper(col("gender"))) === "U", lit("UNKNOWN"))
          .when(trim(upper(col("gender"))) === lit("FEMALE"), lit("FEMALE"))
          .when(trim(upper(col("gender"))) === lit("MALE"), lit("MALE"))
          .when(trim(upper(col("gender"))) === lit("UNKNOWN"), lit("UNKNOWN")).otherwise(lit(null)))
          .withColumn("cc_flag",lit(""))
          .withColumn("run_date",lit(""))
    //      .withColumn("run_date", current_date())


    logger.info(s"<-------------Last_browse_date is getting loaded  -------------->" )
    val snapShotDF=hiveContext.table(s"$smithDB.edw_dim_member")
        .selectExpr("member_key as snap_member_key","last_browse_date as snap_last_browse_date")

    val finalDF=lastDf.join(snapShotDF,$"member_key" === $"snap_member_key","left")
      .withColumn("last_browse_date",coalesce($"last_browse_date",$"snap_last_browse_date"))
      .drop("snap_member_key").drop("snap_last_browse_date").persist()

    val transformedDF=transformData(A_C_check=Some(col("customer_add_date")),sourceDF = finalDF,run_date = rundate,business_date = businessdate)
    load(s"$smithDB.edw_dim_member",transformedDF.select(outputTable.getColumns: _*))

    logger.info(s"<-------------Loaded to snapshot  -------------->",current_date())
    load(transformedDF.select(outputTable.getColumns: _*).filter(s"ingest_date > '$businessdate'"))
    logger.info(s"<-------------Finally Loaded  -------------->",current_date())
  }

}
