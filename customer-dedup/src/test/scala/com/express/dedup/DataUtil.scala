package com.express.dedup

import com.express.cdw.test.utils.DataUtil._
import com.express.cdw.test.utils.SparkUtil
import org.apache.spark.sql.{DataFrame, UserDefinedFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}


/**
  *
  * @author mbadgujar
  */
object DataUtil {


  val MemberLoyaltyIDColumn = "loyalty_id"


  val DimCardEmailPhoneStruct = StructType(
    Seq(
      StructField("member_key", LongType),
      StructField("source_overlay_rank", IntegerType),
      StructField("bank_card", StringType),
      StructField("open_close_ind", StringType),
      StructField("last_updated_date", DateType),
      StructField("dedup_email_address", StringType),
      StructField("dedup_phone_number", StringType),
      StructField("txn_date", StringType)
    )
  )

  val memberCardFinalDF: DataFrame = {

    val memberFile = this.getClass.getClassLoader.getResource("dim_member_multi_email_phone.csv").getPath
    val cardFile = this.getClass.getClassLoader.getResource("sample_card_data_sterling.csv").getPath
    val sqlContext = SparkUtil.getHiveContext
    sqlContext.udf.register[Boolean, String]("not_empty", isNotEmpty)
    sqlContext.udf.register[Boolean, String]("empty", !isNotEmpty(_: String))


    val memberDF = sqlContext.getDfFromCSV(memberFile, "dim_member", Seq("member_id", "member_key", "household_key", "rid", "name_prefix", "name_suffix", "first_name", "last_name", "company_name", "address1_scrubbed", "address2_scrubbed", "open_close_ind", "last_updated_date", "overlay_rank_key", "direct_mail_consent", "email_consent", "email_consent_date", "customer_introduction_date", "is_loyalty_member", "zip_code_scrubbed", "email_address", "phone_nbr", "state_scrubbed", "city_scrubbed"), Map("dateFormat" -> "yyyy/MM/dd", "nullValue" -> "NULL", "header" -> "true")).distinct().repartition(6)
    //memberDF.show(false)
    val cardDF = sqlContext.getDfFromCSV(cardFile, DimCardEmailPhoneStruct, Map("dateFormat" -> "yyyy/MM/dd", "nullValue" -> "NULL", "header" -> "true"))
      .distinct()
      .repartition(6)
    val CardEmailPhoneDF = cardDF
      .distinct()
      .repartition(6)
      .groupBy("member_key")
      .agg(max(col("source_overlay_rank")).alias("source_overlay_rank"),
        collect_set(trim(col("bank_card"))).alias("bank_card"),
        collect_set(trim(col("open_close_ind"))).alias("open_close_ind"),
        collect_set(trim(col("dedup_email_address"))).alias("dedup_email_address"),
        collect_set(trim(col("dedup_phone_number"))).alias("dedup_phone_number"),
        max(col("txn_date")).alias("txn_date")
      )

    //CardEmailPhoneDF.show(false)
    memberDF
      .join(CardEmailPhoneDF, Seq("member_key"), "left").persist

  }

  def isNotEmpty(value: Any): Boolean = {
    Option(value) match {
      case None => false
      case Some(str: String) => str.trim.nonEmpty
      case Some(_) => true
    }
  }

  val CheckEmptyUDF: UserDefinedFunction = udf(!isNotEmpty(_: Any))
}
