package com.express.processing.transform

import com.express.cdw.ThirdPartyType._
import com.express.cdw.CDWContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import com.express.cdw.spark.DataFrameUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.express.processing.transform.TransformMember._


/**
  * Created by amruta.chalakh on 10/30/2017.
  */
object TransformMemberCH extends CDWContext with LazyLogging {

  val tmpDimMemberTable = s"$workDB.dim_member"
  val tmpMemberDF = hiveContext.table(s"$workDB.dim_member")

  def main(args: Array[String]): Unit = {

    val experianCH_DF = hiveContext.table(s"$workDB.fact_ch_experian_in_temp")
                                   .withColumnRenamed("member_key", "member_key_ch")
                                   .withColumn("member_key", explode(split(col("member_key_ch"), ",")))
                                   .withColumn("match_status",lit(true))
                                   .withColumn("is_prison",when(col("prison_suppress_flag") === "A",lit("YES")))
                                   .withColumn("is_deceased",when(col("deceased_suppress_flag") === "I",lit("YES")))
                                   .withColumn("ncoa_last_change_date", when(col("ncoa_indicator") isin ("F,P,I,B,U"), col("ncoa_move_date")))
                                   .withColumn("dwelling_type",when(col("acm_dwelling_code") === "M",lit("Multi_No_Apartment"))
                                                              .when(col("acm_dwelling_code") === "B",lit("BUSINESS"))
                                                              .when(col("acm_dwelling_code") === "S",lit("SINGLE"))
                                                              .when(col("acm_dwelling_code") === "A", lit("MULTI_WITH_APARTMENT"))
                                                              .when(col("acm_dwelling_code") === "U", lit("UNKNOWN")))
                                   .withColumn("country_code",when((col("primary_address") isNotNull) or(col("secondary_address") isNotNull)
                                                                    or(col("city") isNotNull) or(col("state") isNotNull), lit("US"))
                                                                    .otherwise(lit(null)))
                                  .withColumn("valid_strict",when((col("experian_deliverability_score") isin (1,2,3,4)) or
                                    (lower(trim(col("dsf2_verified"))) === lit("y")),lit("YES"))
                                    .when((col("experian_deliverability_score") isin (0,5,6,7,8)) or
                                      (lower(trim(col("deliverability_indicator"))) === lit("u")) or
                                    (lower(trim(col("dsf2_verified"))) === lit("n") or lower(trim(col("dsf2_verified"))) === lit("d")),lit("NO")).otherwise(lit(null)))
                                 .withColumn("valid_loose",when(col("experian_deliverability_score") isin(1,2,3,4,5,6) or lower(trim(col("dsf2_verified"))) === lit("s"),lit("YES"))
                                                          .when(col("experian_deliverability_score") isin(0,7,8) or
                                                           (lower(trim(col("deliverability_indicator"))) === lit("u"))  or
                                                           (lower(trim(col("dsf2_verified"))) === lit("n") or lower(trim(col("dsf2_verified"))) === lit("d")),lit("NO")).otherwise(lit(null)))
                                .withColumn("etl_unique_id",lit(null))
                                .withColumn("current_source_key",lit(2229))


    /*Identifying the Dataframe for Nullyfying attributes in scenarios where pander_suppress_flag='F' and record_type='PROSPECT'*/

    val dimMemberDF = hiveContext.sql(s"""select member_key as dm_member_key,member_id,original_source_key,valid_email,email_consent,customer_add_date,
                                                 customer_introduction_date,record_info_key,original_record_info_key,match_type_key,is_express_plcc,record_type,
                                                 customer_add_date_key,customer_introduction_date_key,is_dm_marketable,is_em_marketable,is_sms_marketable,closest_store_state,
                                                 is_loyalty_member,preferred_store_state,second_closest_store_state,best_household_member from $goldDB.dim_member
                                          where status='current' and upper(trim(record_type))='PROSPECT'""")


    /*Identifying the matched and unmatched records,the matched records are processed further to nullify the attributes while the
      unmatched dataframe is passed through the TransformMember class
     */

    val (prospect_matched,prospect_unmatched) = experianCH_DF.join(dimMemberDF,col("dm_member_key") === col("member_key"),"left")
                                                             .partition(upper(trim(col("pander_suppress_flag"))) === "F"
                                                                        and not(isnull(col("dm_member_key"))))

    val populated_col_list = List("member_key","member_id","last_updated_date","batch_id","process","current_source_key","original_source_key","valid_email",
                                  "email_consent","customer_add_date","customer_introduction_date","record_info_key","original_record_info_key",
                                  "match_type_key","is_express_plcc","record_type","customer_add_date_key","customer_introduction_date_key","is_dm_marketable",
                                  "is_em_marketable","is_sms_marketable","closest_store_state","is_loyalty_member","preferred_store_state",
                                  "second_closest_store_state","best_household_member")

    val default_valuesMap = Map("cdh_member_key" -> null,"overlay_rank_key" -> 0,"direct_mail_consent" -> "UNKNOWN","gender" -> "UNKNOWN","gender_scrubbed" -> "UNKNOWN",
                                "deceased" -> "UNKNOWN","address_is_prison" -> "UNKNOWN")

    val default_colList =   default_valuesMap.keys.toList

    val null_colList =     tmpMemberDF.columns.toList diff (populated_col_list ++ default_colList)

    val prospect_nullDF = null_colList.foldLeft(prospect_matched) {
                                     case (df,colName) => colName match
                                                                {case s if s.matches(""".*_key""") => df.withColumn(s"$colName",lit(-1))
                                                                 case _ => df.withColumn(s"$colName",lit(null))
                                                                }

                                                                  }

    val prospect_DF = default_colList.foldLeft(prospect_nullDF) {
                                     case (df,colName) => df.withColumn(s"$colName",lit(default_valuesMap(s"$colName")))
                                                                }
                                    .withColumn("current_source_key",lit(776))
                                    .withColumn("last_updated_date", current_timestamp)
                                    .withColumn("action_flag", lit("U"))
                                    .withColumn("batch_id", col("batch_id"))
                                    .withColumn("process", lit(ComprehensiveHygiene))
                                    .select(tmpMemberDF.getColumns: _*)

    /*Passing the prospect_unmatched dataframe to the Transform Member object for further transformations*/

    val transformMemberCHobj = new TransformMember(prospect_unmatched,ComprehensiveHygiene)

    // Get insert and update member records
    val transformedMemberData = transformMemberCHobj.getInsertUpdateTransformedRecords
      .select(tmpMemberDF.getColumns: _*).unionAll(prospect_DF)

    // Write output to temp dim_member table
    transformedMemberData
      .withColumn("last_updated_date", current_timestamp)
      .insertIntoHive(SaveMode.Overwrite, tmpDimMemberTable, Some("process"), null)

  }
}