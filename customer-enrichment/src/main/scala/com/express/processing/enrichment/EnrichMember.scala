package com.express.processing.enrichment

import java.math.BigDecimal

import com.express.cdw._
import com.express.cdw.model.Store
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

/**
  * Created by Amruta.chalakh on 5/11/2017.
  */
object EnrichMember extends CDWContext with LazyLogging {

  val EnrichedAlias = "enrich"

  // persist fact transaction detail df, since it will be used by multiple enrich functions
  private val factTransactionDetailDF = hiveContext.table(s"$goldDB.fact_transaction_detail")
    .filter("member_key is not null and member_key > 0")
    .select("member_key", "store_key", "retail_amount", "trxn_date", "product_key", "trxn_id")
    .persist

  factTransactionDetailDF.registerTempTable("factTransactionDetailTemp")
  

  /*
    Enrich best and valid Email for member
   */
  private def bestAndValidMemberEmail(memberDF: DataFrame): DataFrame = {

    //Create Lookup dataframe for Best Email and Valid Email
    val memberMultiEmailDF = hiveContext.table(s"$goldDB.dim_member_multi_email")
      .filter("email_address is not null and best_email_flag='YES'").filter("status='current'")
      .select("member_key", "email_address", "valid_email", "email_consent_date", "email_consent")
      .withColumnRenamed("email_address", "email_address_multi_em")
      .withColumnRenamed("valid_email", "valid_email_multi_em")
      .withColumnRenamed("email_consent_date", "email_consent_date_multi_em")
      .withColumnRenamed("email_consent", "email_consent_multi_em")

    memberDF.join(memberMultiEmailDF, Seq("member_key"), "left")
      .withColumn("action_flag",
        when(trim(upper(col("valid_email"))) === trim(upper(col("valid_email_multi_em")))
          and trim(upper(col("email_address"))) === trim(upper(col("email_address_multi_em")))
          and trim(upper(col("email_consent"))) === trim(upper(col("email_consent_multi_em")))
          and col("email_consent_date") === col("email_consent_date_multi_em"), lit("NC")).otherwise(lit("U")))
      .withColumn("valid_email", when(upper(trim(col("valid_email_multi_em"))) === "YES", lit("YES")).otherwise(lit("NO")))
      .withColumn("email_address", when(not(isnull(col("email_address_multi_em"))), lit(col("email_address_multi_em"))).otherwise(col("email_address")))
      .withColumn("email_consent", when(upper(trim(col("email_consent_multi_em"))) === "YES", lit("YES")).otherwise(lit("NO")))
      .withColumn("email_consent_date", when(not(isnull(col("email_consent_date_multi_em"))), col("email_consent_date_multi_em")).otherwise(col("email_consent_date")))
      .select(memberDF.getColumns: _*)
  }

  /*
    Enrich marketability classification
   */
  private def marketabilityClassification(memberDF: DataFrame): DataFrame =
    memberDF
      .withColumn("is_dm_marketable_tmp",
        when((length(trim(col("address1"))) > 0
          or length(trim(col("address1_scrubbed"))) > 0)
          and (length(trim(col("last_name"))) > 0
          or length(trim(col("last_name_scrubbed"))) > 0)
          and (length(trim(col("zip_code"))) > 0
          or length(trim(col("zip_code_scrubbed"))) > 0)
          , lit("YES")).otherwise(lit("NO")))
      .withColumn("is_em_marketable_tmp", when(upper(trim(memberDF.col("valid_email"))) === "YES" and
        upper(trim(memberDF.col("email_consent"))) === "YES", lit("YES")).otherwise(lit("NO")))
      .withColumn("action_flag", when(col("is_dm_marketable_tmp") === col("is_dm_marketable") and
        col("is_em_marketable_tmp") === col("is_em_marketable"), col("action_flag")).otherwise(lit("U")))
      .drop("is_dm_marketable").drop("is_em_marketable")
      .withColumnRenamed("is_em_marketable_tmp", "is_em_marketable")
      .withColumnRenamed("is_dm_marketable_tmp", "is_dm_marketable")
      .select(memberDF.getColumns: _*)

  /*
    Enrich audience classification
   */
  private def audienceClassification(memberDF: DataFrame): DataFrame = {
    val transactionDetailFactLookup = factTransactionDetailDF.select("member_key").withColumnRenamed("member_key", "member_key_trxn").distinct()
    val dimSourceDF = hiveContext.table(s"$goldDB.dim_source").filter("status='current'").select("source_key", "source_code","is_prospect","is_cheetahmail","is_smartreply")

    val purchaserCustomerDF = memberDF.join(transactionDetailFactLookup,
      memberDF.col("member_key") === transactionDetailFactLookup.col("member_key_trxn"), "left")

    val customerPurchaserMember = purchaserCustomerDF.where(col("member_key_trxn").isNotNull)
      .withColumn("record_type_tmp", when(upper(trim(col("is_dm_marketable"))) === "YES"
        or upper(trim(col("is_em_marketable"))) === "YES"
        or upper(trim(col("is_sms_marketable"))) === "YES", lit("CUSTOMER"))
        .when (col("record_type")==="CUSTOMER",col("record_type")).otherwise(lit("PURCHASER")))
      .withColumn("action_flag", when(col("record_type_tmp") === col("record_type"), col("action_flag")).otherwise(lit("U")))
      .drop("record_type")
      .withColumnRenamed("record_type_tmp", "record_type")
      .select(memberDF.getColumns: _*)

    val prospectRequesterMember = purchaserCustomerDF.where(col("member_key_trxn").isNull)
      .join(broadcast(dimSourceDF), purchaserCustomerDF.col("current_source_key") === dimSourceDF.col("source_key"), "left")
      .withColumn("record_type_tmp", when((col("record_type") === "CUSTOMER" or col("record_type") === "PURCHASER"),col("record_type"))
        .otherwise(when (col("is_prospect")==="YES", lit("PROSPECT"))
          .otherwise(when((col("is_cheetahmail")==="YES") or (col("is_smartreply")==="YES") or (col("source_code")==="ADOBE") or (col("source_code")==="3C"), lit("REQUESTER"))
            .otherwise(when((col("current_source_key").isin( "2229","1236","1237","1064","1065") or col("record_type")==="REQUESTER"), col("record_type"))
              .otherwise(lit("RECORD"))))))
      .withColumn("action_flag", when(col("record_type_tmp") === col("record_type"), col("action_flag")).otherwise(lit("U")))
      .drop("source_code").drop("source_key").drop("record_type")
      .withColumnRenamed("record_type_tmp", "record_type")
      .select(memberDF.getColumns: _*)


    customerPurchaserMember.unionAll(prospectRequesterMember)
  }

  /*
   Enrich phone
   */
  private def enrichPhone(memberDF: DataFrame): DataFrame = {
    val multiPhoneDF = hiveContext.sql(s"select member_multi_phone_key,member_key,phone_number,phone_type,phone_consent,phone_consent_date" +
      s" from $goldDB.dim_member_multi_phone where status='current' and best_mobile_flag='YES'")
      .withColumnRenamed("member_key", "member_key_multi_ph")
      .withColumnRenamed("phone_type", "phone_type_multi_ph")
      .withColumnRenamed("phone_number", "phone_number_multi_ph")
      .withColumnRenamed("phone_consent", "phone_consent_multi_ph")
      .withColumnRenamed("phone_consent_date", "phone_consent_date_multi_ph")

    memberDF.join(multiPhoneDF, col("member_key") === col("member_key_multi_ph"), "left")
      .withColumn("phone_type_tmp", coalesce(col("phone_type_multi_ph")))
      .withColumn("phone_number_tmp", coalesce(col("phone_number_multi_ph")))
      .withColumn("sms_consent_tmp", coalesce(col("phone_consent_multi_ph")))
      .withColumn("sms_consent_date_tmp", coalesce(col("phone_consent_date_multi_ph")))
      .withColumn("action_flag", when(col("phone_type_tmp") === col("phone_type") and
        col("phone_number_tmp") === col("phone_nbr") and
        col("sms_consent_tmp") === col("sms_consent") and
        col("sms_consent_date_tmp") === col("sms_consent_date"), col("action_flag")).otherwise(lit("U")))
      .drop("phone_type").drop("phone_nbr").drop("sms_consent").drop("sms_consent_date")
      .withColumnRenamed("phone_type_tmp", "phone_type").withColumnRenamed("phone_number_tmp", "phone_nbr")
      .withColumnRenamed("sms_consent_tmp", "sms_consent").withColumnRenamed("sms_consent_date_tmp", "sms_consent_date")
      .select(memberDF.getColumns: _*)
  }


  /*
    Enrich Direct Mail Consent
   */
  private def enrichDMConsent(memberDF: DataFrame): DataFrame = {
    val dimMemberConsent = hiveContext.table(s"$goldDB.dim_member_consent").where("status = 'current' and consent_type = 'DM'")
      .withColumn("rank", row_number() over Window.partitionBy("member_key").orderBy(desc("consent_date"),asc("consent_value")))
      .filter("rank = 1")
      .select("member_key", "consent_date", "consent_value")

    memberDF.join(dimMemberConsent, Seq("member_key"), "left")
      .withColumn("action_flag", when(col("action_flag") === "U", lit("U"))
        .when(trim(upper(col("direct_mail_consent"))) === trim(upper(col("consent_value"))), lit("NC"))
        .otherwise(lit("U")))
      .withColumn("direct_mail_consent", when(trim(upper(col("direct_mail_consent"))) === trim(upper(col("consent_value"))),
        upper(col("direct_mail_consent"))).otherwise(col("consent_value")))
      .withColumn("direct_mail_consent_date", when(trim(upper(col("direct_mail_consent"))) === trim(upper(col("consent_value"))),
        col("direct_mail_consent_date")).otherwise(col("consent_date")))
      .select(memberDF.getColumns: _*)
  }

  /*
    Enrich Member First and Last Transaction Related Flags
   */

  private def enrichMemberTxnflags(memberDF: DataFrame): DataFrame = {

    val renamedMemberDF = memberDF
      .withColumnRenamed("first_trxn_date", "first_trxn_date_temp")
      .withColumnRenamed("first_trxn_store_key", "first_trxn_store_key_temp")
      .withColumnRenamed("last_store_purch_date", "last_store_purch_date_temp")
      .withColumnRenamed("last_web_purch_date", "last_web_purch_date_temp")

    val factTrxnSummaryDF = hiveContext.sql(s"select member_key,store_key,trxn_date from $goldDB.fact_transaction_summary where member_key > 0 and member_key is not null")
    val dimStoreDF = hiveContext.sql(s"select store_id,store_key,district_code from $goldDB.dim_store_master where status='current'")
    val joinStoreFactTrxnDF = factTrxnSummaryDF.join(broadcast(dimStoreDF), Seq("store_key"), "left")


    val transformedColumns = joinStoreFactTrxnDF
      .withColumn("flag", when(col("district_code") === 97, lit("WEB")).otherwise(lit("STORE")))
      .withColumn("first_trxn_date_enrich", first("trxn_date") over Window.partitionBy("member_key").orderBy(asc("trxn_date")))
      //order by batch_id,last_updated_date,trxn_id
      .withColumn("first_trxn_store_key_enrich", first("store_key") over Window.partitionBy("member_key").orderBy(asc("trxn_date"),asc("store_key")))
      .withColumn("last_store_purch_date_enrich", when(col("flag") === "STORE",
        first("trxn_date") over Window.partitionBy("member_key", "flag").orderBy(desc("trxn_date"))).otherwise(lit(null)))
      .withColumn("last_web_purch_date_enrich", when(col("flag") === "WEB",
        first("trxn_date") over Window.partitionBy("member_key", "flag").orderBy(desc("trxn_date"))).otherwise(lit(null)))
      .select("member_key", "first_trxn_date_enrich", "first_trxn_store_key_enrich", "last_store_purch_date_enrich", "last_web_purch_date_enrich")
      .groupBy("member_key", "first_trxn_date_enrich", "first_trxn_store_key_enrich")
      .agg(max("last_store_purch_date_enrich").as("last_store_purch_date_enrich"), max("last_web_purch_date_enrich").as("last_web_purch_date_enrich"))



    val enriched_memberDF = renamedMemberDF.join(transformedColumns, Seq("member_key"), "left")
      .withColumn("first_trxn_date", when(not(isnull(col("first_trxn_date_enrich"))),
        col("first_trxn_date_enrich")).otherwise(col("first_trxn_date_temp")))
      .withColumn("first_trxn_store_key", when(not(isnull(col("first_trxn_store_key_enrich"))),
        col("first_trxn_store_key_enrich")).otherwise(col("first_trxn_store_key_temp")))
      .withColumn("last_store_purch_date", when(not(isnull(col("last_store_purch_date_enrich"))),
        col("last_store_purch_date_enrich")).otherwise(col("last_store_purch_date_temp")))
      .withColumn("last_web_purch_date", when(not(isnull(col("last_web_purch_date_enrich"))),
        col("last_web_purch_date_enrich")).otherwise(col("last_web_purch_date_temp")))
      .withColumn("action_flag", when(col("action_flag") === "U", lit("U"))
        .when(trim(col("first_trxn_date")) === trim(col("first_trxn_date_temp")) and
          trim(col("first_trxn_store_key")) === trim(col("first_trxn_store_key_temp")) and
          trim(col("last_store_purch_date")) === trim(col("last_store_purch_date_temp")) and
          trim(col("last_web_purch_date")) === trim(col("last_web_purch_date_temp")), lit("NC"))
        .otherwise(lit("U"))).select(memberDF.getColumns: _*)

    enriched_memberDF
  }


  /*
    Enrich Gender Merchandise Description
   */
  private def enrichGenderMerchandiseDescription(memberDF: DataFrame): DataFrame = {

    val resolveGenderMerchandiseDesc = (rows: Seq[Row]) => {
      val genderMap = rows.map(row => row.getAs[String]("gender") -> row.getAs[Long]("count")).toMap
      genderMap.size match {
        case 1 => if (genderMap.head._1 == "M") "MO" else "WO"
        case _ => if (genderMap("M") > genderMap("W")) "DLM" else "DLW"
      }
    }

    val product = hiveContext.table(s"$goldDB.dim_product").where("status = 'current' and gender is not null")
      .select("product_key", "gender")

    // last 36 months transactions with gender Merchandise description populated
    val genderMerchandiseDescDF = factTransactionDetailDF
      .dropColumns(Seq("store_key", "retail_amount")).distinct
      .filter("product_key != 0 OR product_key != -1")
      //.filter(datediff(col("trxn_date"), add_months(current_date(), -36)) >= 0)
      .join(broadcast(product), Seq("product_key"))
      .groupBy("member_key", "gender").agg(count("gender").as("count"))
      .groupByAsList(Seq("member_key"))
      .withColumn("cm_gender_merch_descr", udf[String, Seq[Row]](resolveGenderMerchandiseDesc).apply(col("grouped_data")))
      .select("member_key", "cm_gender_merch_descr")

    memberDF.join(genderMerchandiseDescDF, Seq("member_key"), "left")
  }


  /*
    Enrich Member Preffered store and Channel
   */

  private def enrichpreferredStore(memberDF: DataFrame): DataFrame = {
    val dimStoreDF = hiveContext.table(s"$goldDB.dim_store_master").where("status = 'current' and open_closed_ind='OPEN'")
      .select("retail_web_flag", "store_id", "store_key")
      .withColumnRenamed("store_id", "prstoreid")
      .withColumnRenamed("store_key", "prstorekey")

    val factTransactionDetailLookup = hiveContext
      .sql(s"select member_key,store_key as storeid, count(distinct trxn_id) as cnt, " +
      s" sum(retail_amount) as maxpur, max(trxn_date) as maxdt from factTransactionDetailTemp where member_key != -1 " +
      s"and store_key != -1 group by member_key,store_key")
//      .withColumn("rank", row_number() over Window.partitionBy("member_key").orderBy(desc("cnt"), desc("maxpur"), desc("maxdt")))
//      .filter()
      .select("member_key", "storeid", "cnt", "maxpur", "maxdt")

    val preferredStoreChannel = factTransactionDetailLookup.join(broadcast(dimStoreDF), col("storeid") === col("prstorekey"))
      .withColumn("rank", row_number() over Window.partitionBy("member_key").orderBy(desc("cnt"), desc("maxpur"), desc("maxdt"),asc("storeid")))
      .filter("rank=1")
      .withColumn("prstorekey", when(col("prstorekey").isNull, lit("-1")).otherwise(col("prstorekey")))
      .select("member_key", "prstoreid", "prstorekey", "retail_web_flag")

    memberDF.join(preferredStoreChannel, Seq("member_key"), "left")
      .withColumn("action_flag", when(col("action_flag") === "U", lit("U"))
        .when(trim(col("preferred_store_key")) === trim(col("prstorekey")) and trim(upper(col("preferred_Channel"))) === trim(upper(col("retail_web_flag"))), lit("NC")).otherwise(lit("U")))
      .withColumn("preferred_store_key", when(trim(col("preferred_store_key")) === trim(col("prstorekey")), col("preferred_store_key")).otherwise(col("prstorekey")))
      .withColumn("preferred_Channel", when(trim(upper(col("preferred_Channel"))) === trim(upper(col("retail_web_flag"))), col("preferred_Channel")).otherwise(col("retail_web_flag")))
      .select(memberDF.getColumns: _*)
  }

  /*
    Enrich Member preferred store State
   */

  private def enrichprefstoreState(memberDF: DataFrame): DataFrame = {

    memberDF.withColumn("preferred_store_state_tmp",
      when(col("preferred_store_key").isin(lit(195),lit(31928),lit(30903)) and col("distance_to_preferred_store") === lit(-1), lit("E-COMMERCE"))
        .when(((col("preferred_store_key") > lit(-1) && !col("preferred_store_key").isin(lit(195),lit(31928),lit(30903)))
          and (col("distance_to_preferred_store").isNotNull && col("distance_to_preferred_store") > lit(-1))),
          lit("CALCULATED"))
        .when(col("preferred_store_key") === lit(-1) and col("distance_to_preferred_store").isNull and (col("latitude").isNull and col("longitude").isNull), lit("NO PREFERRED STORE"))
        .when(col("preferred_store_key") > lit(-1) and (col("distance_to_preferred_store").isNull and (col("latitude").isNull or col("longitude").isNull)), lit("CANNOT MEASURE"))
        .otherwise(lit("(RE)CALCULATE")))
      .withColumn("action_flag", when(col("action_flag") === "U", lit("U")).when(col("preferred_store_state") === col("preferred_store_state_tmp"), lit("NC")).otherwise(lit("U")))
      .drop("preferred_store_state")
      .withColumnRenamed("preferred_store_state_tmp", "preferred_store_state")
      .select(memberDF.getColumns: _*)
  }

  /*
    Enrich Member closest store State
   */

  private def enrichclosestStoreState(memberDF: DataFrame): DataFrame = {

    memberDF.withColumn("closest_store_state_tmp",
      when(col("closest_store_key") > lit(-1) and col("distance_to_closest_store").isNotNull, lit("CALCULATED"))
        .when(col("closest_store_key") === lit(-1) and col("distance_to_closest_store") === lit(-1), lit("OUTSIDE RANGE"))
        .when(col("closest_store_key") === lit(-1) and (col("distance_to_closest_store").isNull and (col("latitude").isNull or col("longitude").isNull)), lit("CANNOT MEASURE"))
        .otherwise(lit("(RE)CALCULATE")))
      .withColumn("action_flag", when(col("action_flag") === "U", lit("U")).when(col("closest_store_state") === col("closest_store_state_tmp"), lit("NC")).otherwise(lit("U")))
      .drop("closest_store_state")
      .withColumnRenamed("closest_store_state_tmp", "closest_store_state")
      .select(memberDF.getColumns: _*)
  }

  /*
    Enrich second closest store state
   */

  private def enrichSecclosestStoreState(memberDF: DataFrame): DataFrame = {

    memberDF.withColumn("second_closest_store_state_tmp",
      when(col("second_closest_store_key") > lit(-1) and col("distance_to_sec_closest_store").isNotNull, lit("CALCULATED"))
        .when(col("second_closest_store_key") === lit(-1) and col("distance_to_sec_closest_store") === lit(-1), lit("OUTSIDE RANGE"))
        .when(col("second_closest_store_key") === lit(-1) and (col("distance_to_sec_closest_store").isNull and (col("latitude").isNull or col("longitude").isNull)), lit("CANNOT MEASURE"))
        .otherwise(lit("(RE)CALCULATE")))
      .withColumn("action_flag", when(col("action_flag") === "U", lit("U")).when(col("second_closest_store_state") === col("second_closest_store_state_tmp"), lit("NC")).otherwise(lit("U")))
      .drop("second_closest_store_state")
      .withColumnRenamed("second_closest_store_state_tmp", "second_closest_store_state")
      .select(memberDF.getColumns: _*)

  }

  /*
    Enrich Member Household
   */

  private def enrichHousehold(memberDF: DataFrame, householdDF: DataFrame): DataFrame = {
    memberDF.join(householdDF, col("member_key") === col("member_key_hh"), "left")
      .withColumn("action_flag", when(col("action_flag") === "U", lit("U"))
        .when(col("household_key") === col("household_key_hh"), lit("NC")).otherwise(lit("U")))
      .withColumn("household_key", when(col("member_key") === col("member_key_hh"), col("household_key_hh")).otherwise(lit(-1)))
      .select(memberDF.getColumns: _*)
  }

  /*
      Enrich Best Household Member
     */

  private def enrichBestHousehold(memberDF: DataFrame): DataFrame = {

    val trxnDetailDF = factTransactionDetailDF.groupBy("member_key").agg(max("trxn_date").as("recent_trxn_dt")).select("member_key", "recent_trxn_dt")
    /*val factScoringHistDF = hiveContext.sql(s"select household_key, member_key,min(segment_rank) as hrank from $goldDB.fact_scoring_history " +
       s"where segment_rank is not null and household_key != -1 and member_key != -1 and household_key is not null and member_key is not null group by household_key,member_key")*/

    val factScoringHistDF = hiveContext.table(s"$goldDB.fact_scoring_history")
                                       .filter("household_key != -1 and member_key != -1 and household_key is not null and member_key is not null")
                                       .withColumn("segment_rank",when(col("segment_rank") > 0,col("segment_rank")).otherwise(lit(99)))
                                       .withColumn("hrank", row_number() over Window.partitionBy("member_key").orderBy(desc("last_updated_date")))
                                       .filter("hrank=1")

    val factScoringHistandTrxnDF = factScoringHistDF.join(trxnDetailDF, Seq("member_key"), "left")
                                                    .select("member_key", "segment_rank", "recent_trxn_dt")
                                                    .withColumnRenamed("member_key","member_key_sc")

    val bestHouseholdMemberDF = memberDF.select("member_key","household_key").join(factScoringHistandTrxnDF, col("member_key") === col("member_key_sc"))
                                        .withColumn("rank",row_number() over Window.partitionBy("household_key").orderBy(asc("segment_rank"),desc("recent_trxn_dt"),asc("member_key")))
                                        .filter("rank=1")
                                        .drop(col("member_key"))
                                        .drop(col("household_key"))

        memberDF.join(bestHouseholdMemberDF, col("member_key") === col("member_key_sc"), "left")
                .withColumn("best_household_member_tmp", when(not(isnull(col("member_key_sc"))),lit("YES")).otherwise(lit("NO")))
                .withColumn("action_flag", when(col("action_flag") === "U", lit("U"))
                .when(col("best_household_member") === col("best_household_member_tmp"), lit("NC")).otherwise(lit("U")))
                .withColumn("best_household_member", col("best_household_member_tmp"))
                .select(memberDF.getColumns: _*)
  }

  private def populateIsLoyaltyMember(memberDF: DataFrame): DataFrame = {
    memberDF
      .withColumn("is_loyalty_member", when(col("loyalty_id").isNotNull and
        (col("loyalty_id") !== ""), lit("YES")).otherwise(lit("NO")))
      .select(memberDF.getColumns: _*)
  }


  /*Enrich Global Opt Out Flag*/
  private def enrichGlobalOptOutFlag(memberDF: DataFrame): DataFrame = {
    /*Filtering the records with household_key = -1,which will be union once the entire processing is done*/
    val hk_memberDF = memberDF.na.fill(null, Seq("global_opt_out")).filter("household_key is not null and household_key != -1")
    val not_hk_memberDF = memberDF.filter("household_key = -1 or household_key is null")

    /*Killist Dataset-When Household Key is present*/
   /* val hk_nn_dimkilllist = hiveContext.sql(
      s"""select household_key as household_key_kl,
                                                rtrim(ltrim(upper(last_name))) as last_name_kl,
                                                trim(CONCAT(COALESCE(trim(upper(address_line_one)),'')," ",CONCAT(COALESCE(trim(upper(address_line_two)),'')))) as address_kl,
                                               zipcode as zipcode_kl
                                                from $goldDB.dim_kill_list
                                                where status="current" and
                                                household_key is not null and
                                                last_name is not null and
                                                (address_line_one is not null or address_line_two is not null) and
                                                zipcode is not null
                                                group by
                                                household_key,
                                                last_name,
                                                zipcode,
                                                trim(CONCAT(COALESCE(trim(upper(address_line_one)),'')," ",CONCAT(COALESCE(trim(upper(address_line_two)),''))))""") */

    val hk_nn_dimkilllist = hiveContext.sql(
      s"""select distinct household_key as household_key_kl from $goldDB.dim_kill_list where status="current" and household_key is not null""")

    /*val hk_nn_glbl_optflag = hk_memberDF.join(broadcast(hk_nn_dimkilllist),
      col("household_key_kl") === col("household_key") and
        trim(upper(col("last_name_kl"))) === trim(upper(col("last_name"))) and
        trim(upper(col("address_kl"))) === trim(upper(concat_ws("", col("address1"), col("address2")))) and
        col("zipcode_kl") === col("zip_full_scrubbed")
      , "left")
      .drop(col("global_opt_out"))
      .withColumn("global_opt_out", when(not(isnull(col("household_key_kl"))), lit("YES")).otherwise(lit(null)))
      .select(memberDF.getColumns: _*) */
    val hk_nn_glbl_optflag = hk_memberDF.join(broadcast(hk_nn_dimkilllist),col("household_key_kl") === col("household_key"),"left")
      .drop(col("global_opt_out"))
      .withColumn("global_opt_out", when(not(isnull(col("household_key_kl"))), lit("YES")).otherwise(lit(null)))
      .select(memberDF.getColumns: _*)


    /*Combining the different dataframes for getting the complete enriched dataset along with global_opt_out flag*/
    val final_enriched_dataset = hk_nn_glbl_optflag.unionAll(not_hk_memberDF).withColumn("global_opt_out",
      when(lower(trim(col("global_opt_out"))) === "null", lit(null)).otherwise(col("global_opt_out")))
    final_enriched_dataset
  }

  /*Enrich Is Sms Markatable*/
  private def enrichSmsMarketable(memberDF: DataFrame): DataFrame = {
    val multiPhoneDF = hiveContext.sql(s"select member_key,phone_number,phone_consent" +
      s" from $goldDB.dim_member_multi_phone where status='current'").filter(trim(lower(col("phone_consent"))) === "yes")
      .withColumnRenamed("member_key", "member_key_multi_ph")
      .withColumnRenamed("phone_number", "phone_number_multi_ph")

    memberDF.join(multiPhoneDF, col("member_key") === col("member_key_multi_ph"), "left")
      .withColumn("is_sms_marketable", when(not(isnull(col("phone_number_multi_ph"))), lit("YES")).otherwise(lit("NO")))
      .select(memberDF.getColumns: _*)
  }

  /*Enrich */
  private def applyDistanceAlgo(memberDF: DataFrame): DataFrame = {
    import hiveContext.implicits._
    val storeDF = hiveContext.sql(s"select store_key,latitude,district_code,longitude,open_closed_ind" +
      s" from $goldDB.dim_store_master where status='current'")
      .withColumnRenamed("store_key", "store_key_dist")
      .withColumnRenamed("latitude", "latitude_store")
      .withColumnRenamed("longitude", "longitude_store")

    val preferedStoreDist = memberDF.join(broadcast(storeDF), col("preferred_store_key") === col("store_key_dist"), "left").withColumn("distance_to_preferred_store",
      when(col("district_code") === lit(97),lit(-1))
        .when(not(isnull(col("latitude_store"))) and not(isnull(col("longitude_store"))) and not(isnull(col("latitude"))) and not(isnull(col("longitude"))) and col("open_closed_ind") === "OPEN",
        DistanceTravelled(col("latitude_store"),  col("latitude"), col("longitude_store"),col("longitude"))).otherwise(null))

    val storeDistDF = hiveContext.sql(s"select store_key,latitude,longitude from $goldDB.dim_store_master " +
      s"where status='current' and latitude is not null and longitude is not null and open_closed_ind='OPEN'")

    val storeDistDS = storeDistDF.as[Store]
    val storeDistArray = storeDistDS.collect.toSeq

    def shortestStoreKeyValues(stores: Seq[Store]) = callUDF(
        closestStoreKey(_: BigDecimal, _: BigDecimal, stores),
      StructType(Seq(
        StructField("closest_store", LongType),
        StructField("second_closest_store", LongType),
        StructField("closest_distance", DoubleType),
        StructField("second_closest_distance", DoubleType)
      )),
      col("latitude"), col("longitude")
    )

    preferedStoreDist.withColumn("storeColumn", shortestStoreKeyValues(storeDistArray))
      .withColumn("closest_store_key", col("storeColumn.closest_store"))
      .withColumn("distance_to_closest_store", col("storeColumn.closest_distance"))
      .withColumn("second_closest_store_key", col("storeColumn.second_closest_store"))
      .withColumn("distance_to_sec_closest_store", col("storeColumn.second_closest_distance"))
      .select(memberDF.getColumns :_*)

  }



  /**
    * apply ipcode and tier_id to member
    *
    * @param membeSourceDf sourcedf
    * @return [[DataFrame]]
    */
  def applyTierID(membeSourceDf: DataFrame): DataFrame = {

    val memberWindowSpec = Window.partitionBy("member_key")
    val dimTierSourceDf = membeSourceDf.sqlContext.table(s"gold.dim_tier").filter("status='current'").select("tier_key", "tier_name")
      .withColumnRenamed("tier_key", "tier_key_dim").withColumnRenamed("tier_name", "tier_name_dim").persist()

    val factTierDF = membeSourceDf.sqlContext.table(s"gold.fact_tier_history").filter("Member_key is not null")
      .withColumn("rank_member", row_number().over(memberWindowSpec.orderBy(desc("id"),desc("tier_begin_date"))))  // Added this line as per Jira DM-1714
      .filter("rank_member = 1")
      .select("tier_key", "Member_key")
      .withColumnRenamed("Member_key", "Member_key_fact")

    val joinedFactDimTier = factTierDF.join(broadcast(dimTierSourceDf),col("tier_key") === col("tier_key_dim"),"left").select("Member_key_fact","tier_key_dim","tier_name_dim")

    membeSourceDf.join(joinedFactDimTier, membeSourceDf.col("member_key") === factTierDF.col("Member_key_fact"), "left")
      .withColumn("current_tier_key", col("tier_key_dim"))
      .withColumn("tier_name_temp", col("tier_name_dim"))
      .withColumn("tier_name",when(trim(col("tier_name_temp")) === "",lit(null)).when(trim(col("tier_name_temp")) === " ",lit(null)).when(trim(col("tier_name_temp")) === "N/A",lit(null))
        .otherwise(col("tier_name_temp")))
      .select(membeSourceDf.getColumns: _*)
  }


  /**
    * Set value for 'is_express_plcc' flag
    *
    * @param memberSourceDf Member source
    * @return [[DataFrame]] with 'is_express_plcc' flag calculated
    */
  def enrichExpressPLCC(memberSourceDf: DataFrame): DataFrame = {
    val sqlContext = memberSourceDf.sqlContext
    import sqlContext.implicits._
    val isExpressPLCCData = sqlContext.table(s"$goldDB.fact_card_history")
      .select("tokenized_cc_nbr", "card_type_key", "member_key", "closed_date", "source_key", "is_primary_account_holder", "last_updated_date")
      .filter($"source_key".isin(8, 9, 300) and $"card_type_key".isin(1, 25, 2))
      .withColumn("rank", row_number() over Window.partitionBy("member_key", "tokenized_cc_nbr").orderBy(desc("last_updated_date")))
      .filter("rank = 1").drop("rank") /* Get latest updated record for a plcc account */
      .withColumn("is_express_plcc_new", when(($"closed_date".isNull or $"closed_date">lit(current_date())) and $"is_primary_account_holder".isNotNull, "YES").otherwise("NO"))
      .withColumn("rank", row_number() over Window.partitionBy("member_key").orderBy(desc("is_express_plcc_new")))
      .filter("rank = 1")
      .select("member_key", "is_express_plcc_new")

    memberSourceDf.join(isExpressPLCCData, Seq("member_key"), "left")
      .withColumn("is_express_plcc",
        when($"is_plcc_request" === "YES", "YES") /* set YES if is_plcc_request */
          .when($"is_express_plcc_new".isNotNull, $"is_express_plcc_new") /* Set to Yes/NO if Card History has any open/all closed plcc cards */
          .otherwise("NO")) /* Otherwise set to NO */
      .select(memberSourceDf.getColumns: _*)
  }

  /*
   Enrich Member Entry Point
 */
  def main(args: Array[String]): Unit = {

    val process =  args(0)
    val batch_id = args(1)

    val outputTempTable = hiveContext.table(s"$workDB.dim_member")

    // Creating Required DataFrames for Member Enrichment
    val dimMemberDF = hiveContext.sql(s"select member_key,valid_email,email_address,email_consent,email_consent_date,last_name,address1_scrubbed," +
      s"address2_scrubbed,address1,address2,zip_code_scrubbed,direct_mail_consent,is_dm_marketable,is_sms_marketable,is_em_marketable,record_type,phone_type,phone_nbr," +
      s"sms_consent,sms_consent_date,zip_code,address_is_prison,last_name_scrubbed,zip4_scrubbed,zip_full_scrubbed,deceased,first_trxn_date,first_trxn_store_key," +
      s"last_web_purch_date,last_store_purch_date,preferred_channel,preferred_store_key,distance_to_preferred_store,preferred_store_state," +
      s"closest_store_key,distance_to_closest_store,closest_store_state,second_closest_store_key,distance_to_sec_closest_store,latitude,longitude," +
      s"second_closest_store_state,best_household_member,is_loyalty_member,member_status,loyalty_id,global_opt_out,current_source_key,ip_code,current_tier_key,tier_name," +
      s"household_key,direct_mail_consent_date,is_plcc_request,is_express_plcc " +
      s"from $goldDB.dim_member where status = 'current'")
      .withColumn("action_flag", lit("NC"))



    val householdDF = hiveContext.sql(s"select household_key as household_key_hh,associated_member_key from $goldDB.dim_household where status='current'")
      .withColumn("member_key_hh", explode(split(col("associated_member_key"), ",")))


    logger.info("<----------------------------------Best and Valid Email Member-------------------------------------->")
    val bestAndValidEmailMember = bestAndValidMemberEmail(dimMemberDF)

    //logger.info("<----------------------------------Marketability Classification------------------------------------->")
    //val marketableMember = marketabilityClassification(bestAndValidEmailMember).persist()

//    logger.info("<------------------------------------Audience Classification---------------------------------------->")
  //  val audienceClassifyMember = audienceClassification(bestAndValidEmailMember).persist()

    logger.info("<--------------------------------------Enrich member with respect to Phone ------------------------->")
    val phoneEnrich = enrichPhone(bestAndValidEmailMember)
   // marketableMember.unpersist()

    logger.info("<--------------------------------------Enrich DM Consent Member------------------------------------->")
    val dmConsent = enrichDMConsent(phoneEnrich)

    logger.info("<----------------------------------Marketability Classification------------------------------------->")
    val marketableMember = marketabilityClassification(dmConsent).persist()

    logger.info("<-------------------------Enrich First and Last Transaction Related Flags -------------------------->")
    val trxnFlags = enrichMemberTxnflags(marketableMember)
    marketableMember.unpersist()

    logger.info("<--------------------------------------Enrich Preferred Store and Channel--------------------------->")
    val preferredStore = enrichpreferredStore(trxnFlags)

    logger.info("<--------------------------------------latitude longitude----------------------------------->")
    val _applyDistanceAlgo = applyDistanceAlgo(preferredStore)

    logger.info("<--------------------------------------Enrich Member for Preffered store State---------------------->")
    val preferredStoreState = enrichprefstoreState(_applyDistanceAlgo)

    logger.info("<--------------------------------------Enrich Member for closest store State------------------------>")
    val closeststoreState = enrichclosestStoreState(preferredStoreState)

    logger.info("<--------------------------------------Enrich Member for second closest store State------------------------>")
    val secCloseststoreState = enrichSecclosestStoreState(closeststoreState)

    logger.info("<--------------------------------------Enrich Member Household-------------------------------------->")
    val memberHousehold = enrichHousehold(secCloseststoreState, householdDF)

    logger.info("<--------------------------------------Enrich Best Member for Household----------------------------->")
    val bestMemberHousehold = enrichBestHousehold(memberHousehold)

    logger.info("<--------------------------------------populate is_loyalty_member----------------------------------->")
    val isLoyaltyMember = populateIsLoyaltyMember(bestMemberHousehold)


    logger.info("<--------------------------------------Global opt out----------------------------------->")
    val globaloptoutFlag = enrichGlobalOptOutFlag(isLoyaltyMember).persist()


    // enrich gender merchant description
    val genderMerchantDescription = enrichGenderMerchandiseDescription(globaloptoutFlag)

    logger.info("<--------------------------------------Sms Markatable----------------------------------->")
    val smsMarkatable = enrichSmsMarketable(genderMerchantDescription)

    logger.info("<------------------------------------Audience Classification---------------------------------------->")
    val audienceClassifyMember = audienceClassification(smsMarkatable).persist()

    logger.info("<--------------------------------------Tier ID----------------------------------->")
    val setTierName = applyTierID(audienceClassifyMember)

    val isExpressPlcc = enrichExpressPLCC(setTierName).alias(EnrichedAlias)

    val DimMem = "DimMember"
    val MemberWithAllColumns = hiveContext.sql(s"select * from $goldDB.dim_member where status='current'").alias(DimMem)

    val remainingColumns = MemberWithAllColumns.getColumns.diff(isExpressPlcc.getColumns)

    logger.info("Non Enriched columns to be selected from dim_member: {}", remainingColumns.mkString(", "))
    import hiveContext.implicits._

    val finalDF = MemberWithAllColumns.join(isExpressPlcc, $"$DimMem.member_key" === $"$EnrichedAlias.member_key", "left")
      .select(isExpressPlcc.getColumns(EnrichedAlias) ++ remainingColumns: _*)

    val dimDateDF = hiveContext.sql(s"select * from $goldDB.dim_date where status = 'current'").persist
    val dateColumns = finalDF.columns.filter(_.contains("date_key")).map(_.replaceAll("_key", ""))
    logger.info("Update date keys for date columns:- {}", dateColumns.mkString(","))

    val updatedWithKeys = finalDF.updateKeys(dateColumns, dimDateDF, "sdate", "date_key")
      .select(MemberWithAllColumns.getColumns :+ col("action_flag"): _*)

    val enrichMemberResult = updatedWithKeys
      .withColumn("status", lit("current"))
      .withColumn("process", lit(process))
      .withColumn("last_updated_date", current_timestamp())
      .withColumn("tier_name",when(trim(col("tier_name")) === "",lit(null)).when(trim(col("tier_name")) === " ",lit(null)).when(trim(col("tier_name")) === "N/A",lit(null))
        .otherwise(col("tier_name")))
      .withColumn("member_status",
        when(col("member_status") === 1 or upper(trim(col("member_status"))) === "ACTIVE", lit("ACTIVE"))
          .when(col("member_status") === 2 or upper(trim(col("member_status"))) === "CANCELED", lit("CANCELED"))
          .when(col("member_status") === 3 or upper(trim(col("member_status"))) === "CLOSED", lit("CLOSED"))
          .when(col("member_status") === 4 or upper(trim(col("member_status"))) === "LOCKED", lit("LOCKED"))
          .when(col("member_status") === 5 or upper(trim(col("member_status"))) === "UNKNOWN", lit("UNKNOWN"))
          .when(col("member_status") === 6 or upper(trim(col("member_status"))) === "MERGED", lit("MERGED")).otherwise(lit(null)))
      .withColumn("action_flag",lit(null))
      .select(outputTempTable.getColumns: _*)


    enrichMemberResult.insertIntoHive(SaveMode.Overwrite, s"$workDB.dim_member", Some("process"), batch_id)
  }
}