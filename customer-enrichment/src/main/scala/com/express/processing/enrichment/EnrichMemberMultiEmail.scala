package com.express.processing.enrichment

import com.express.cdw.{Settings, _}
import com.express.cdw.spark.DataFrameUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._





/**
  * Created by poonam.mishra on 5/15/2017.
  */
object EnrichMemberMultiEmail extends LazyLogging {

  val MemberMultiEmail = "dimMemberMultiEmail"
  val deliveryDataAlias = "deliveryData"
  val clickDataAlias = "clickData"
  val quarantineAlias = "quarantine"
  val transactionDetailAlias = "transactionDetail"
  val timeAlias = "time"

  val YES = "YES"
  val NO = "NO"
  val NCD = "NCD"
  val UPDATE = "U"

  /*
    Case class to store Email related information
   */
  case class EmailFlags(var email_address: String, var email_consent: String, var valid_email: String, var no_hard_bounce: String, var no_soft_bounce: String, var best_email_flag: String,
                        var action_flag: String, var recently_opened_flag: String, var recent_email_consent_flag: String, var most_opened_12_months: String) {
    def this(data: Seq[String]) = this(data.head, data(1), data(2), data(3), data(4), data(5), data(6), data(7), data(8), data(9))
  }

  /**
    * Resolves the best email given multiple Email information
    *
    * @param flagList [[Seq]] of EmailFlags for a member
    * @return
    */
  def bestEmailResolution(flagList: Seq[Row]): Seq[EmailFlags] = {

    val flagObjList = flagList
      .map(row => row.toSeq.map(value => if (value == null) "" else value.toString))
      .map(seq => new EmailFlags(seq))


    val emailConsentCount = flagObjList.map(_.email_consent).count(_ == YES)
    val validEmailCount = flagObjList.map(_.valid_email).count(_ == YES)
    val NHBCount = flagObjList.map(_.no_hard_bounce).count(_ == YES)
    val NSBCount = flagObjList.map(_.no_soft_bounce).count(_ == YES)
    val ROCount = flagObjList.map(_.recently_opened_flag).count(_ == YES)
    val REConsentCount = flagObjList.map(_.recent_email_consent_flag).count(_ == YES)
    val MOCount = flagObjList.map(_.most_opened_12_months).count(_ == YES)


    def updateFlagList(bestFlags: Seq[EmailFlags], others: Seq[EmailFlags]) = {
      val bestFlag = bestFlags.head
      if (bestFlag.best_email_flag == YES)
        bestFlag.action_flag = NCD
      else {
        bestFlag.best_email_flag = YES
        bestFlag.action_flag = UPDATE
      }
      others.foreach(nbef => {
        if (nbef.best_email_flag == NO)
          nbef.action_flag = NCD
        else {
          nbef.best_email_flag = NO
          nbef.action_flag = UPDATE
        }
      })
    }

    if (emailConsentCount == 1) {
      val (bestFlags, others) = flagObjList.partition(_.email_consent == YES)
      updateFlagList(bestFlags, others)
      return flagObjList
    }
    if (validEmailCount == 1) {
      val (bestFlags, others) = flagObjList.partition(_.valid_email == YES)
      updateFlagList(bestFlags, others)
      return flagObjList
    }
    if (NHBCount == 1) {
      val (bestFlags, others) = flagObjList.partition(_.no_hard_bounce == YES)
      updateFlagList(bestFlags, others)
      return flagObjList
    }
    if (NSBCount == 1) {
      val (bestFlags, others) = flagObjList.partition(_.no_soft_bounce == YES)
      updateFlagList(bestFlags, others)
      return flagObjList
    }
    if (ROCount == 1) {
      val (bestFlags, others) = flagObjList.partition(_.recently_opened_flag == YES)
      updateFlagList(bestFlags, others)
      return flagObjList
    }
    if (REConsentCount == 1) {
      val (bestFlags, others) = flagObjList.partition(_.recent_email_consent_flag == YES)
      updateFlagList(bestFlags, others)
      return flagObjList
    }
    if (MOCount == 1) {
      val (bestFlags, others) = flagObjList.partition(_.most_opened_12_months == YES)
      updateFlagList(bestFlags, others)
      return flagObjList
    }
    updateFlagList(Seq(flagObjList.head), flagObjList.tail)
    flagObjList
  }

  def bestMemberForEmail(emailData: DataFrame): DataFrame = {

    import emailData.sqlContext.implicits._
    val emailCountWindowSpec = Window.partitionBy("email_address")

    emailData
      .withColumn("best_member_rank", row_number().over(emailCountWindowSpec.orderBy(asc("segment_rank"), desc("trxn_time"), desc("unit_price_after_discount"),desc("best_member"))))
      .withColumn("best_member_flag", when($"best_member_rank" === 1, lit("YES")).otherwise("NO"))

  }

  // Best Email UDF
  private val bestEmailUDF: UserDefinedFunction = udf(bestEmailResolution(_: Seq[Row]))

  //Invalid Email Check UDF
  private val CheckValidEmailUDF: UserDefinedFunction = udf[Boolean, String](validEmailFlag)

  /**
    * Set Email as Valid/Invalid based on Quarantine data(This enrichment is no longer used)
    *
    * @param dimMemberMultiEmailDF Multi Member Email Dataframe
    * @param quarantineDF          Quarantine Dataframe
    * @return
    */
  private def setValidEmail(dimMemberMultiEmailDF: DataFrame, quarantineDF: DataFrame): DataFrame = {
    val sqlContext = quarantineDF.sqlContext

    val DMMCols = dimMemberMultiEmailDF.columns

    //Need to check logic for valid email
    val enrichedValidEmail = dimMemberMultiEmailDF.withColumn("action_flag",lit("NCD"))
                             .select(DMMCols.head, DMMCols.tail: _*)

    //logger.info("Classification of Members having single email and multiple email")

    // Finding each member having how many no of emails
    val otherCols = (enrichedValidEmail.columns.toList diff List("member_key", "email_address")).map("temp." + _).mkString(",")
    enrichedValidEmail.registerTempTable("enrichedValidEmail")
    val emailCountInitialFilter = " select temp.member_key," +
      "temp.email_address," +
      otherCols +
      ",count(temp.email_address) over(partition by temp.member_key) as count " +
      "from enrichedValidEmail as temp"
    sqlContext.sql(emailCountInitialFilter).sort("member_key")
  }


  /**
    * Entry Point
    *
    * @param args dd
    */
  def main(args: Array[String]): Unit = {
    System.setProperty("hive.exec.dynamic.partition.mode", "nonstrict")
    //Configurations
    val conf = Settings.sparkConf
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val workDB = Settings.getWorkDB
    val goldDB = Settings.getGoldDB

    //Load required Source tables

    // Unskew dataset//
    val dimMemberMultiEmail = hiveContext.sql(s"select lower(email_address) as lower_email_address,* from $goldDB.dim_member_multi_email where status ='current' and member_key != 1134577523 and member_key != 116628031 and email_address != '@'").persist()
      .drop("email_address")
      .withColumnRenamed("lower_email_address","email_address")
      .withColumn("action_flag": String, lit("NCD"))
      .alias(MemberMultiEmail)

    val workBpMemberCm = hiveContext.sql(s"select member_key,trim(lower(primaryemailaddress)) as email_address from $workDB.work_bp_member_cm where trim(primaryemailaddress) <>'' or primaryemailaddress is not null")
      .withColumn("is_latest_loyalty_record",lit("YES"))


    // deduping fact delivery data -- need to check order by in window func//
    val deliveryData_raw = hiveContext.sql(s"select mme_id, delivery_contact_date, fact_delivery_data_id, event_date, " +
      s"lower(email_address) as delivered_email_address, member_key as delivered_member_key, status as delivery_status, " +
      s"failure_reason from $goldDB.fact_delivery_data")
        .persist()
    deliveryData_raw.registerTempTable("deliveryData_raw")
    val deliveryDataTemp = "select *, row_number() over (partition by delivered_member_key,delivered_email_address order by fact_delivery_data_id,event_date desc) as rnk_delivered_email from deliveryData_raw"
    val deliveryData = hiveContext.sql(deliveryDataTemp).filter("rnk_delivered_email = 1").alias(deliveryDataAlias)

    // dedup  fact quarintine data  -- need to check order by in window func//
    val quarantine_raw = hiveContext.sql(s"select lower(email_address) as lower_email_address,* from $goldDB.fact_quarantine_data")
      .drop("email_address")
      .withColumnRenamed("member_key", "quarantined_members")
      .withColumnRenamed("lower_email_address", "quarantined_email")
    quarantine_raw.registerTempTable("quarantine_raw")
    val quarantineTemp = "select *,row_number() over (partition by quarantined_email order by fact_quarantine_data_id desc) as rank_quarantined_email from quarantine_raw"
    val quarantine = hiveContext.sql(quarantineTemp).filter("rank_quarantined_email = 1").alias(quarantineAlias)

    // dedup  fact click data  -- need to check order by in window func//
    val clickData_raw = hiveContext.sql(s"select lower(email_address) as lower_email_address,* from $goldDB.fact_click_data")
      .withColumnRenamed("member_key", "clicked_member_key")
      .drop("email_address")
      .withColumnRenamed("lower_email_address", "clicked_email_address")
    clickData_raw.registerTempTable("clickData_raw")
    val clickDataTemp = "select *,row_number() over (partition by clicked_member_key,clicked_email_address order by fact_click_data_id desc) as rank_clicked_email from clickData_raw"
    val clickData = hiveContext.sql(clickDataTemp).filter("rank_clicked_email = 1").alias(clickDataAlias).persist()

    // load dim_time  -- need to check necessity//
    val time = hiveContext.sql(s"select time_key as trxn_time_key, time_in_24hr_day from $goldDB.dim_time where status ='current'")

    // Convert date into time stamp of 24 hours//
    val transactionDetailFact = hiveContext.table(s"$goldDB.fact_transaction_detail")
      .where("member_key != -1")
      .select("member_key", "trxn_time_key", "trxn_date", "unit_price_after_discount")
      .join(broadcast(time), Seq("trxn_time_key"), "left")
      .withColumn("trxn_time", unix_timestamp(concat_ws(" ", $"trxn_date", $"time_in_24hr_day")))


    val transactionDetailRecenTrxnInfo2 = transactionDetailFact
      .withColumn("count1",dense_rank().over(Window.partitionBy("member_key")
      .orderBy(desc("trxn_time"))))
    val transactionDetailRecenTrxnInfo3 = transactionDetailRecenTrxnInfo2.filter("count1=1").drop("count1")
    val transactionDetailRecenTrxnInfo = transactionDetailRecenTrxnInfo3
        .groupBy("member_key","trxn_time")
      .agg(sum("unit_price_after_discount").alias("unit_price_after_discount"))
      .withColumn("unit_price_after_discount",$"unit_price_after_discount".cast("decimal(10,3)"))
      //.withColumn("unit_price_after_discount", sum("unit_price_after_discount").over(Window.partitionBy("member_key","trxn_time")))
      .select("member_key", "trxn_time","unit_price_after_discount")


    logger.info("Enrichment: VALID EMAIL FLAG using Fact Quarantine Table")
    val DMMCols = dimMemberMultiEmail.columns
    val enrichedValidEmail = setValidEmail(dimMemberMultiEmail, quarantine)
    enrichedValidEmail.persist
    val (multipleEmails, singleEmail, zeroEmail) = (enrichedValidEmail.filter("count>1"), enrichedValidEmail.filter("count=1"), enrichedValidEmail.filter("count=0"))


    logger.info("Enrichment: BEST EMAIL FLAG for Members with single email address")
    val enrichedSingleEmail = singleEmail
      .withColumn("action_flag", when(col("best_email_flag") !== "YES", lit("U")).otherwise(col("action_flag")))
      .withColumn("best_email_flag", when((col("best_email_flag") !== "YES") or (col("best_email_flag").isNull), lit("YES")).otherwise(col("best_email_flag")))
      .select(DMMCols.head, DMMCols.tail: _*)


    val MultipleEmails = "multipleEmails"
    val checkBounceEmails = multipleEmails.alias(MultipleEmails)
      .join(deliveryData.drop("mme_id"), $"$MultipleEmails.email_address" === $"$deliveryDataAlias.delivered_email_address" and $"$MultipleEmails.member_key" === $"$deliveryDataAlias.delivered_member_key", "left")
      .drop($"$deliveryDataAlias.last_updated_date").drop($"$deliveryDataAlias.batch_id")
    val MultipleEmailsWithBounceFlags = "multipleEmailsWithBounceFlags"
    val multipleEmailsWithBounceFlags = checkBounceEmails
      .withColumn("no_hard_bounce", when(not(isnull(col("delivered_email_address"))) and
        (col("delivery_status") === "2" and col("failure_reason") === "1")
        , lit("NO")).otherwise(lit("YES")))
      .withColumn("no_soft_bounce", when(not(isnull(col("delivered_email_address"))) and
      (col("delivery_status") === "2" and (col("failure_reason") === "2" or col("failure_reason") === "3" or
        col("failure_reason") === "4" or col("failure_reason") === "5" or col("failure_reason") === "20")), lit("NO")).otherwise(lit("YES")))
      .select("no_hard_bounce", "no_soft_bounce" +: DMMCols: _*).alias(MultipleEmailsWithBounceFlags)

    //Generate flag for most recently opened email address
    val joinFlagCheckClick = multipleEmailsWithBounceFlags.join(clickData.drop("mme_id"),
      $"$MultipleEmailsWithBounceFlags.email_address" === $"$clickDataAlias.clicked_email_address" and $"$MultipleEmailsWithBounceFlags.member_key" === $"$clickDataAlias.clicked_member_key", "left")
      .drop($"$clickDataAlias.last_updated_date").drop($"$clickDataAlias.batch_id")
      .sort("member_key")

    joinFlagCheckClick.registerTempTable("joinFlagCheckClick")
    val otherColsRecentlyOpened = joinFlagCheckClick.columns.toList.mkString(",")
    val otherColsRecentlyOpenedAlias = (joinFlagCheckClick.columns.toList diff List("member_key", "email_address")).map("temp." + _).mkString(",")
    val mostRecentlyOpenedEmail = " select temp.member_key,temp.email_address,cast(temp.rank_log_date AS bigint)," + otherColsRecentlyOpenedAlias +
      ",count(*) over(partition by temp.member_key,temp.rank_log_date) as log_date_email_count " +
      "from (select " + otherColsRecentlyOpened +
      ",rank() over (partition by member_key order by log_date desc) as rank_log_date " +
      "from  joinFlagCheckClick ) as temp"
    val MostRecentlyOpenedEmail = hiveContext.sql(mostRecentlyOpenedEmail)
    val withRecentlyOpenedFlag = MostRecentlyOpenedEmail
      .withColumn("recently_opened_flag", when(col("rank_log_date") === 1 and col("log_date_email_count") > 1 and col("url_type") === "2", lit("YES"))
      .otherwise(lit("NO")))

    //Generate flag for most recent Email_Consent
    withRecentlyOpenedFlag.registerTempTable("withRecentlyOpenedFlag")
    val withRecentlyOpenedFlagCols = withRecentlyOpenedFlag.columns.toList.mkString(",")
    val withRecentlyOpenedFlagColsAlias = (withRecentlyOpenedFlag.columns.toList diff List("member_key", "email_address")).map("temp." + _).mkString(",")
    val mostRecentEmailConsent = " select temp.member_key,temp.email_address,cast(temp.rank_email_consent AS bigint)," + withRecentlyOpenedFlagColsAlias +
      ",count(*) over(partition by temp.member_key,temp.rank_email_consent) as email_consent_date_count " +
      "from (select " + withRecentlyOpenedFlagCols +
      ",rank() over (partition by member_key order by email_consent_date desc,email_address desc) as rank_email_consent " +
      "from  withRecentlyOpenedFlag) as temp"
    val MostRecentlyEmailConsent = hiveContext.sql(mostRecentEmailConsent)
    val withRecentEmailConsentFlag = MostRecentlyEmailConsent
      .withColumn("recent_email_consent_flag", when(col("rank_email_consent") === 1 and col("email_consent_date_count") > 1, lit("YES"))
      .otherwise(lit("NO")))


    //Generate flag for most opened email address
    val WithMostOpenedEmailFlag = "withMostOpenedEmailFlag"
    val withMostOpenedEmailFlag = withRecentEmailConsentFlag
      .withColumn("most_opened_12_months", when((datediff(current_date(), to_date(col("log_date"))) <= 365) and
        col("url_type") === "2", lit("YES")).otherwise(lit("NO"))).alias(WithMostOpenedEmailFlag)

    withMostOpenedEmailFlag.persist()


    val flagStruct = Seq("email_address", "email_consent", "valid_email",
      "no_hard_bounce", "no_soft_bounce", "best_email_flag",
      "action_flag", "recently_opened_flag", "recent_email_consent_flag", "most_opened_12_months")

    val aggregatedFlags = withMostOpenedEmailFlag
      .groupByAsList(Seq("member_key"), toBeGroupedColumns = flagStruct)
      .withColumn("aggregated_checked_flags", bestEmailUDF($"grouped_data"))
      .withColumn("explode_cols", explode($"aggregated_checked_flags"))
      .withColumn("email_address", $"explode_cols.email_address")
      .withColumn("best_email_flag", $"explode_cols.best_email_flag")
      .withColumn("action_flag", $"explode_cols.action_flag")
      .drop("aggregated_flag").drop("aggregated_checked_flags").drop("explode_cols")


    val EnrichedMultipleEmails = "enrichedMultipleEmails"
    val enrichedMultipleEmails = withMostOpenedEmailFlag
      .join(aggregatedFlags, Seq("member_key", "email_address"), "left")
      .drop($"$WithMostOpenedEmailFlag.best_email_flag")
      .drop($"$WithMostOpenedEmailFlag.action_flag")
      .select(DMMCols.head, DMMCols.tail: _*).alias(EnrichedMultipleEmails)
    enrichedMultipleEmails.registerTempTable("enrichedMultipleEmails")
/*    val enrichedMultipleEmailsFinal = "select *,row_number() over (partition by member_key,best_email_flag order by member_key,email_address asc) as rank_on_MK_and_BEF  from enrichedMultipleEmails"
    val enrichedMultipleEmailsFinalDF = hiveContext.sql(enrichedMultipleEmailsFinal)
      .withColumn("best_email_flag",when((col("best_email_flag") === "YES" and col("rank_on_MK_and_BEF") > 1),lit("NO")).otherwise(col("best_email_flag")))
      .select(DMMCols.head, DMMCols.tail: _*)*/

    logger.info("Enrichment: BEST EMAIL FLAG")
    val EnrichedBestEmailFlag = "enrichedBestEmailFlag"
    val enrichedZeroEmail = zeroEmail.drop("count")
    val enrichedBestEmailFlag = enrichedSingleEmail.unionAll(enrichedMultipleEmails).unionAll(enrichedZeroEmail).alias(EnrichedBestEmailFlag)


    logger.info("Enrichment : EMAIL ACTIVITY INACTIVITY FLAG")

     val deliveryData_DF = hiveContext.sql(s"""select mme_id as delivered_mme_id, delivered_email_address,
                                               row_number() over(partition by mme_id,delivered_email_address order by event_date desc) as rank
                                               from deliveryData_raw where to_date(delivery_contact_date) >= date_sub(current_date(),90)""")
                                      .filter("rank = 1").drop(col("rank"))

     val joinMMEDelivery = enrichedBestEmailFlag.join(deliveryData_DF,
                           enrichedBestEmailFlag.col("mme_id") === deliveryData_DF.col("delivered_mme_id")
                           and enrichedBestEmailFlag.col("email_address") === deliveryData_DF.col("delivered_email_address")
                           , "left").withColumn("delivery_date_check_flag", when((not(isnull(col("delivered_email_address")))) and (not(isnull(deliveryData_DF.col("delivered_mme_id"))))
                           , lit("YES")).otherwise(lit("NO")))

    val clickData_DF = hiveContext.sql(s"select distinct mme_id from $goldDB.fact_click_data where to_date(log_date) >= date_sub(current_date(), 90) and url_type in (1,2)")

    val withActivityFlag = joinMMEDelivery.join(clickData_DF,
      joinMMEDelivery.col("mme_id") === clickData_DF.col("mme_id"), "left")
      .withColumn("activity_flag", when((col("delivery_date_check_flag") === "YES") and (not(isnull(clickData_DF.col("mme_id")))), lit("YES")).otherwise(lit("NO")))
      .drop(clickData_DF.col("mme_id"))
      .persist()

    val enrichedEmailActivityInactivity = withActivityFlag.withColumn("e_action_flag", when((col("active_inactive_flag") === "ACTIVE" and
      col("activity_flag") === "YES") or (col("active_inactive_flag") === "INACTIVE" and
      col("activity_flag") === "NO"), lit("U")).otherwise(lit("NCD")))
      .withColumn("active_inactive_flag", when(col("activity_flag") === "YES"
        , lit("ACTIVE")).otherwise(lit("INACTIVE"))).withColumn("process", lit("enrich")).select("process", "e_action_flag" +: DMMCols: _*)


    /* best member for an email  */
    logger.info("Enrichment : BEST MEMBER FOR A EMAIL")

    //val scoringHistoryDF = hiveContext.sql(s"select member_key,segment_rank from $goldDB.fact_scoring_history")
    val scoringHistoryDF = hiveContext.sql(s"select member_key,segment_rank,scoring_date,scoring_history_id from $goldDB.fact_scoring_history ")
      .withColumn("segment_rank",when($"segment_rank".isNull or length(trim($"segment_rank")) === 0,lit(999)).otherwise($"segment_rank"))
      .filter("member_key is not null and member_key !=-1")
      .withColumn("rank", row_number().over(Window.partitionBy("member_key").orderBy(desc("scoring_date"),desc("scoring_history_id"))))
      .filter("rank=1")
      .drop("rank")
     val joinMultiEmailWithTrxnTime = enrichedEmailActivityInactivity.join(transactionDetailRecenTrxnInfo,
      Seq("member_key"), "left")

    val joinedWithScoringHistDF = joinMultiEmailWithTrxnTime.join(scoringHistoryDF,
      Seq("member_key"),"left")
      .sort("email_address")

    val bestMemberForEmailData=bestMemberForEmail(joinedWithScoringHistDF)

    val outputTempTable = hiveContext.table(s"$workDB.dim_member_multi_email")

      val finalBestMember = bestMemberForEmailData
      .withColumn("action_flag",when(not($"best_member_flag" === $"best_member") , lit("U")).otherwise(col("e_action_flag")))
      .withColumn("best_member",when($"best_member_flag" === "YES", lit("YES")).otherwise(lit("NO")))



/*    finalBestMember.registerTempTable("finalBestMember")
    val enrichedBestMultipleMembers = hiveContext.sql("select *,row_number() over (partition by email_address " +
      "order by best_member_rank) as rank_on_MK_and_BM  from finalBestMember")
      .withColumn("best_member", when(col("best_member") === "YES" and col("rank_on_MK_and_BM") > 1, lit("NO")).otherwise(col("best_member")))*/

    val enrichedLoyaltyEmail = finalBestMember.join(workBpMemberCm,Seq("member_key","email_address"),"left")
      .withColumn("loyalty_rank",  row_number().over(Window.partitionBy("member_key", "is_loyalty_email").orderBy(desc("is_latest_loyalty_record"))))
      .withColumn("is_loyalty_email", when(col("is_loyalty_email").equalTo("YES") and col("loyalty_rank").notEqual(1), lit("NO")).otherwise(col("is_loyalty_email"))) /* update is_loyalty_email within member */
      .withColumn("email_rank",  row_number().over(Window.partitionBy("email_address", "is_loyalty_email").orderBy(desc("is_latest_loyalty_record"))))
      .withColumn("is_loyalty_email", when(col("is_loyalty_email").equalTo("YES") and col("email_rank").notEqual(1), lit("NO")).otherwise(col("is_loyalty_email"))) /* update is_loyalty_email across members having same email */

      val allEnrichedData = enrichedLoyaltyEmail
      .withColumn("best_email_flag", when(col("email_address") === "@", "NO").otherwise(col("best_email_flag")))
      .withColumn("valid_email", when(col("email_address") === "@", "NO").otherwise(col("valid_email")))
      .select(outputTempTable.getColumns: _*)

    allEnrichedData.write.mode(SaveMode.Overwrite).partitionBy("process").insertInto(s"$workDB.dim_member_multi_email")
  }
}