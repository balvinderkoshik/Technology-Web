package com.express.cm.criteria

import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cm.lookup.LookUpTable._
import com.express.cm.lookup.LookUpTableUtil.getLookupTable
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by aman.jain on 5/22/2017.
  */
object TieBreaker extends LazyLogging {

  private val SourceAlias="source"
  private val lookUpDF_Member = getLookupTable(MemberDimensionLookup).select("member_key","is_dm_marketable",
    "is_em_marketable","is_sms_marketable","customer_introduction_date")

  /**
    * This function is use to identify which records from a dataframe qualifies for tie breaking
    * @param tieBreakerColumns
    * @param tieBreakerDF
    * @return returns a dataframe along with the count at record level
    * Example :
    * Source-
    * +-------------+----------+
    * |tender_number|member_key|
    * +-------------+----------+
    * |        PLCC1|    101   |
    * |        PLCC1|    102   |
    * |        PLCC2|    103   |
    * |        PLCC2|    104   |
    * |        PLCC3|    105   |
    * |        PLCC4|    106   |
    * +-------------+----------+
    * Expected Output:
    * TiedDF:
    * +-------------+----------+-----+
    * |tender_number|member_key|count|
    * +-------------+----------+-----+
    * |        PLCC1|    101   |  2  |
    * |        PLCC1|    102   |  2  |
    * |        PLCC2|    103   |  2  |
    * |        PLCC2|    104   |  2  |
    * +-------------+----------+-----+
    *
    * ResolvedDF:
    * +-------------+----------+-----+
    * |tender_number|member_key|count|
    * +-------------+----------+-----+
    * |        PLCC3|    105   |  1  |
    * |        PLCC4|    106   |  1  |
    * +-------------+---------+------+
    */

  def identifyTieBreaker(tieBreakerColumns: List[String], tieBreakerDF: DataFrame): (DataFrame,DataFrame) = {
    val sqlContext = tieBreakerDF.sqlContext
    val tiebreakercols = tieBreakerColumns.mkString(",")
    val tempAliasColStr = tieBreakerColumns.map("temp." + _).mkString(",")
    tieBreakerDF.registerTempTable("tieBreakerDF")
    val tiebreakerQuery = " select " + tempAliasColStr +
      ",temp.member_key" +
      ",count(*) over(partition by " + tempAliasColStr + ") as count " +
      "from (select distinct " + tiebreakercols +
      ",member_key " +
      "from tieBreakerDF) as temp"
    val tieBreakerGrp = sqlContext.sql(tiebreakerQuery)
    (tieBreakerGrp.filter("count<>1").persist, tieBreakerGrp.filter("count=1").persist)
  }


  /** TieBreaker Check 1
    Find the Primary account holder from the list of member keys
    * Example :
    * Source-
    * +-------------+----------+
    * |tender_number|member_key|
    * +-------------+----------+
    * |        PLCC1|    101   |
    * |        PLCC1|    102   |
    * |        PLCC2|    103   |
    * |        PLCC2|    104   |
    * |        PLCC3|    105   |
    * |        PLCC3|    106   |
    * |        PLCC3|    107   |
    * |        PLCC4|    108   |
    * |        PLCC4|    109   |
    * +-------------+----------+
    *
    * Result of Analytical Query :
    * +-------------+----------+----------+----------+
    * |tender_number|member_key|LkpMemKey |difference|
    * +-------------+----------+----------+----------+
    * |        PLCC1|    101   |  101     |  1       |
    * |        PLCC1|    102   |  Null    |  1       |
    * |        PLCC2|    103   |  103     |  2       |
    * |        PLCC2|    104   |  104     |  2       |
    * |        PLCC3|    105   |  105     |  2       |
    * |        PLCC3|    106   |  Null    |  2       |
    * |        PLCC3|    107   |  107     |  2       |
    * |        PLCC4|    108   |  Null    |  0       |
    * |        PLCC4|    109   |  Null    |  0       |
    * +-------------+----------+----------+----------+

    difference column : count of distinct members(member_key) - count of null members(LkpMemKey)
      group by PLCC

      when difference is 1 : no tie breaker required , filter null LkpMemKey and save the result as resolved
      when difference is 0 : pass the result to further tie breaker
      when difference is > 1 : pass the result to further tie breaker , by filtering null LkpMemKey
    * Expected Output:
    * TiedDF:
    * +-------------+----------+
    * |tender_number|member_key|
    * +-------------+----------+
    * |        PLCC2|    103   |
    * |        PLCC2|    104   |
    * |        PLCC3|    105   |
    * |        PLCC3|    107   |
    * |        PLCC4|    108   |
    * |        PLCC4|    109   |
    * +-------------+----------+
    *
    * ResolvedDF:
    * +-------------+----------+
    * |tender_number|member_key|
    * +-------------+----------+
    * |        PLCC1|    101   |
    * +-------------+----------+
    */

  def primaryAccountHolderCheck(tiedDF: DataFrame ,source: DataFrame,tieBreakerColumns: List[String]): (DataFrame,DataFrame) = {

    val sqlContext = tiedDF.sqlContext
    import sqlContext.implicits._

    val tiebreakercols = tieBreakerColumns.mkString(",")
    val tempAliasColStr = tieBreakerColumns.map("temp." + _).mkString(",")
    val temp2AliasColStr = tieBreakerColumns.map("temp2." + _).mkString(",")
    val cardHistory="lookUpDF_CardHistory"
    val cardType="lookUpDF_CardType"

    val lookUpDF_CardHistory = getLookupTable(CardHistoryFactLookup).filter("is_primary_account_holder='YES'").withColumnRenamed("member_key","LkpMemKey").alias(cardHistory)
    val lookUpDF_CardType = getLookupTable(CardTypeDimensionLookup).filter("is_express_plcc='YES'").alias(cardType)
    val joinCardHistoryCardType = lookUpDF_CardHistory.join(lookUpDF_CardType, lookUpDF_CardHistory("card_type_key")===lookUpDF_CardType("card_type_key"))//.alias(LookUpAlias)
    val PrimaryAccountHolder = tiedDF.alias(SourceAlias).join(joinCardHistoryCardType, tiedDF("member_key") === $"$cardHistory.LkpMemKey","left")
      .select(source.getColumns(SourceAlias):+ $"$cardHistory.LkpMemKey":_*)

    PrimaryAccountHolder.registerTempTable("PrimaryAccountHolder")

    val PrimaryAccountHolderWithCount=sqlContext.sql("select "+temp2AliasColStr+
      ",temp2.member_key" +
      ",temp2.LkpMemKey" +
      ",temp2.count_member-temp2.count_null as difference " +
      "from(" +
      "select " +tempAliasColStr+
      ",temp.member_key" +
      ",temp.LkpMemKey" +
      ",count(*) over(partition by " +tempAliasColStr+") count_member" +
      ",(count(*) over(partition by  " +tempAliasColStr+")-count(temp.LkpMemKey) over(partition by  " +tempAliasColStr+")) count_null " +
      "from (" +
      "select distinct "+tiebreakercols+
      ",member_key" +
      ",LkpMemKey " +
      "from PrimaryAccountHolder) temp ) temp2")

    val PrimaryAccountHolderWithZeroCount=PrimaryAccountHolderWithCount.filter("difference=0").select(tiebreakercols,"member_key")

    val PrimaryAccountHolderWithOtherThanOneCount=PrimaryAccountHolderWithCount.filter("difference>1 and LkpMemKey is not null").select(tiebreakercols,"member_key")

    (PrimaryAccountHolderWithCount.filter("difference=1 and LkpMemKey is not null").select(tiebreakercols,"member_key").withColumn("count",lit("1")).persist,
      PrimaryAccountHolderWithZeroCount.unionAll(PrimaryAccountHolderWithOtherThanOneCount).withColumn("count",lit("1")).persist)
  }


  /**
    * TieBreaker Check 2
    * Check tied members with is_dm_marketable,is_em_marketable,is_sms_marketable flag priority on Dim_Member
    * Member with highest priority is considered as resolved
    * Source for Marketability Check:
    * +-------------+----------+
    * |tender_number|member_key|
    * +-------------+----------+
    * |        PLCC2|    103   |
    * |        PLCC2|    104   |
    * |        PLCC3|    105   |
    * |        PLCC3|    107   |
    * |        PLCC4|    108   |
    * |        PLCC4|    109   |
    * +-------------+----------+
    * Priority is based on is_dm_marketable,is_em_marketable,is_sms_marketable
    * base on priority rank will be provided by analytical function and member with highest rank in each group will be considered as resolved
    * if two members tie for the highest rank it will be further sent to next Tie Breaker check
    ** +-------------+----------+----------+----------+------+
    * |tender_number |member_key|Flag      |Priority  |Rank  |
    * +--------------+----------+----------+----------+------+
    * |        PLCC2 |    103   | NYN      |  5       |  1   |
    * |        PLCC2 |    104   | NNY      |  6       |  2   |
    * |        PLCC3 |    105   | YYN      |  2       |  1   |
    * |        PLCC3 |    107   | YYN      |  2       |  1   |
    * |        PLCC4 |    108   | YYY      |  1       |  1   |
    * |        PLCC4 |    109   | YYY      |  1       |  1   |
    * +--------------+----------+----------+----------+------+
    */
  def marketabilityCheck(tiedPrimaryAccountHolder: DataFrame,tieBreakerColumns: List[String]): (DataFrame,DataFrame) = {

    val sqlContext = tiedPrimaryAccountHolder.sqlContext

    val tiebreakercols = tieBreakerColumns.mkString(",")

    val joinTiedPrimaryAccountHolderDimMember = tiedPrimaryAccountHolder.join(lookUpDF_Member, Seq("member_key"), "left")
      .withColumn("flag", concat_ws("~",col("is_dm_marketable"), col("is_em_marketable"), col("is_sms_marketable")))
      .withColumn("priority", expr("case when flag = 'YES~YES~YES' then 1 " +
        "when flag = 'YES~YES~NO' then 2 " +
        "when flag = 'YES~NO~NO' then 3 " +
        "when flag = 'NO~YES~YES' then 4 " +
        "when flag = 'NO~YES~NO' then 5 " +
        "when flag = 'NO~NO~YES' then 6 " +
        "else 99 end"))

    val memberStaticColumns = Seq( "member_key", "is_dm_marketable", "is_em_marketable", "is_sms_marketable","customer_introduction_date", "flag", "priority")
    val DynamicColumns = tieBreakerColumns ++ memberStaticColumns
    val MembersWithFlag = joinTiedPrimaryAccountHolderDimMember.select(DynamicColumns.head, DynamicColumns.tail : _*)
    MembersWithFlag.registerTempTable("MembersWithFlag")
    //Assign Ranks to members based on their priority
    val RankMembersWithFlag = sqlContext.sql("select " + tiebreakercols +
      ",member_key," +
      "rank() over (partition by " + tiebreakercols + " order by priority asc) as rank " +
      "from MembersWithFlag")


    //Filter records with highest priority i.e rank =1
    val MembersWithHighPriority = RankMembersWithFlag.filter("rank=1")

    //Create DataFrame for next TieBreaker Check
    val (tiedMembersWithHighPriority, resolvedMembersWithHighPriority) = identifyTieBreaker(tieBreakerColumns, MembersWithHighPriority)

    logger.debug("Tied Result Count After TieBreaker Marketability Check : {}",tiedMembersWithHighPriority.count().toString)
    logger.debug("Resolved Result Count After TieBreaker Marketability Check : {}",resolvedMembersWithHighPriority.count().toString)

    (tiedMembersWithHighPriority, resolvedMembersWithHighPriority)
  }


  /**
    * TieBreaker Check 3
    * Check tied members with Recent Transaction on Fact_Transaction_Detail
    * Members with recent transaction will be considered as resolved
    * Source for Recency Check
    * +--------------+-----------+
    * |tender_number |member_key |
    * +--------------+-----------+
    * |        PLCC3 |    105    |
    * |        PLCC3 |    107    |
    * |        PLCC4 |    108    |
    * |        PLCC4 |    109    |
    * +--------------+-----------+
    *
    * Result of Recency check with Analytical Rank function on Trxn Date
    * Recent Trxn Date in each group will be given rank 1 and is considered as resolved and members with ties on Rank 1 will be sent
    * for the last Tie Breaker Check
    * Result:
    * +--------------+-----------+----------------------+-----------+
    * |tender_number |member_key |         Trxn_Date    | Rank      |
    * +--------------+-----------+----------------------+-----------+
    * |        PLCC3 |    105    | 2015-05-10 08:05:02  |   2       |
    * |        PLCC3 |    107    | 2016-05-10 03:10:01  |   1       |
    * |        PLCC4 |    108    | 2015-08-18 11:05:08  |   1       |
    * |        PLCC4 |    109    | 2015-08-18 11:05:08  |   1       |
    * +--------------+-----------+----------------------+-----------+
    */

  def recencyCheck(tiedMembersWithHighPriority: DataFrame,tieBreakerColumns: List[String]): (DataFrame,DataFrame) = {

    val sqlContext = tiedMembersWithHighPriority.sqlContext

    val tiebreakercols = tieBreakerColumns.mkString(",")

    val lookUpDF_FactTransactionDetail = getLookupTable(TransactionDetailFactLookup).filter("member_key <> -1")
    val lookUpDimTime = getLookupTable(TimeDimensionLookup)
    //val joinTiedMembersFactTrxnDetail = tiedMembersWithHighPriority.join(lookUpDF_FactTransactionDetail, Seq("member_key"), "left")
    //val joinTiedMembersFactTrxnDetailDimTime=joinTiedMembersFactTrxnDetail.join(lookUpDimTime,joinTiedMembersFactTrxnDetail("trxn_time_key")===lookUpDimTime("time_key"),"left")
    val joinTiedMembersFactTrxnDetailDimTime = tiedMembersWithHighPriority.join(lookUpDF_FactTransactionDetail, Seq("member_key"), "left")
    joinTiedMembersFactTrxnDetailDimTime.registerTempTable("joinTiedMembersFactTrxnDetailDimTime")

    //Assign Ranks based on Recent Transactions
    val RankRecentTrxn = sqlContext.sql("select "
      + tiebreakercols +
      ",member_key" +
      ",rank() over (partition by " + tiebreakercols + " order by trxn_date,time_in_24hr_day desc) as rank " +
      "from joinTiedMembersFactTrxnDetailDimTime ")
    val RecentTrxn = RankRecentTrxn.filter("rank=1")

    //Create DataFrame for next TieBreaker Check
    val (tiedRecentTrxn, resolvedRecentTrxn) = identifyTieBreaker(tieBreakerColumns, RecentTrxn)

    logger.debug("Tied Result Count After TieBreaker Recency Check  : {}",tiedRecentTrxn.count().toString)
    logger.debug("Resolved Result Count After TieBreaker Recency Check  : {}",resolvedRecentTrxn.count().toString)
    (tiedRecentTrxn, resolvedRecentTrxn)
  }


  /**
    * TieBreaker Check 4
    * This is the last check
    * Select the oldest member from the tied members using Dim_Member table
    * Member_key with oldest customer_introduction_date is considered as resolved
    * Source for Last Check:
    * +--------------+-----------+
    * |tender_number |member_key |
    * +--------------+-----------+
    * |        PLCC4 |    108    |
    * |        PLCC4 |    109    |
    * +--------------+-----------+
    *
    * Expected Result:
    * +--------------+-----------+---------------------------+-----------+
    * |tender_number |member_key | customer_introduction_date| Rank      |
    * +--------------+-----------+---------------------------+-----------+
    * |        PLCC4 |    108    | 2016-08-18 11:05:08       |   2       |
    * |        PLCC4 |    109    | 2015-08-18 11:05:08       |   1       |
    * +--------------+-----------+---------------------------+-----------+
    *
    * Member with Rank 1 is considered as resolved .
    */
  def oldestMemberCheck(tiedRecentTrxn: DataFrame,tieBreakerColumns: List[String]): (DataFrame) = {


    val sqlContext = tiedRecentTrxn.sqlContext

    val tiebreakercols = tieBreakerColumns.mkString(",")

    val joinTiedRecentTrxnDimMember = tiedRecentTrxn.join(lookUpDF_Member, Seq("member_key"), "left")
    joinTiedRecentTrxnDimMember.registerTempTable("joinTiedRecentTrxnDimMember")

    val OldestMember = sqlContext.sql("select " + tiebreakercols +
      ",member_key" +
      ",row_number() over (partition by " + tiebreakercols + " order by customer_introduction_date asc) as rank " +
      "from joinTiedRecentTrxnDimMember ").filter("rank=1").persist

    logger.debug("Resolved record counts after Oldest Member Check : {}",OldestMember.count().toString)
    logger.debug("Result after last tie breaker check i.e oldest member")


    OldestMember
    //OldestMember.unionAll(resolvedRecentTrxn.unionAll(resolvedMembersWithHighPriority
      //.unionAll(resolvedPrimaryAccountHolder))).unionAll(resolvedDF)

  }


  /**
    * This function applies the tie breaking algorithm on the dataframe
    * @param tieBreakerColumns
    * @param source
    * @return returns a dataframe with unique member keys for each tie breaker columns
    */
  def tieBreakerCheck(tieBreakerColumns: List[String] , source: DataFrame, functionName: String): DataFrame = {

    source.persist

    logger.debug("No. of  Input Records for Tie Breaker Columns {} :- ",  source.count().toString)

    if(source.filter(not(isnull(source(MemberKeyColumn)))).isEmpty) {
      logger.debug("There are no matched records for eligible for identify Tie Breaker ")
      return source.unpersist
    }

    logger.debug("Matched records found Calling Identify Tie Breaker......")
    logger.debug("No of Input Records after removing Null member records..... {}",source.filter(not(isnull(source(MemberKeyColumn)))).count().toString)

    val (tiedDF ,resolvedDF) = identifyTieBreaker(tieBreakerColumns, source.filter(not(isnull(source(MemberKeyColumn)))))
    logger.debug("Tied Records Count : {}",tiedDF.count().toString)
    logger.debug("Resolved Records Count : {}",resolvedDF.count().toString)

    if(!tiedDF.isNotEmpty) // put log here match found no tied record found
      return resolvedDF


    logger.debug("Initiating TieBreaker PLCC Check")
    val (resolvedPrimaryAccountHolder,tiedPrimaryAccountHolder) = if(functionName.matches("PLCCMatch")){
        primaryAccountHolderCheck(tiedDF,source,tieBreakerColumns)
    }
    else {
      logger.debug("TieBreaker PLCC Check not required")
      (resolvedDF.filter(lit(false)), tiedDF)
    }

    logger.debug("Tied Result Count After TieBreaker PLCC Check : {}",tiedPrimaryAccountHolder.count().toString)
    logger.debug("Resolved Result Count After TieBreaker PLCC Check : {}",resolvedPrimaryAccountHolder.count().toString)

    tiedDF.unpersist

    /**
      * Check if 2nd Tie Breaker Marketability Check is required
      */
    logger.debug("Checking if Tied Data Frame is empty ...........")

    if (tiedPrimaryAccountHolder.isNotEmpty) {

      logger.debug("Tied Data Frame not empty ,Initiating TieBreaker Marketability Check .........")

      val (tiedMembersWithHighPriority, resolvedMembersWithHighPriority)=  marketabilityCheck(tiedPrimaryAccountHolder,tieBreakerColumns)

      tiedPrimaryAccountHolder.unpersist

      /**
        * Check if 3rd Tie Breaker Recency Check is required
        *
        */

      logger.debug("Checking if Tied Data Frame is empty .........")

      if (tiedMembersWithHighPriority.isNotEmpty) {

        logger.debug("Tied Data Frame not empty ,Initiating TieBreaker Recency Check ............")

        val (tiedRecentTrxn, resolvedRecentTrxn) =   recencyCheck(tiedMembersWithHighPriority,tieBreakerColumns)
        tiedMembersWithHighPriority.unpersist


        /**
          * Check if 4th Tie Breaker Check is required
          *
          */

        logger.debug("Checking if Tied Data Frame is empty ......")
        if (tiedRecentTrxn.isNotEmpty) {

          logger.debug("Tied Data Frame is not empty ,Initiating last Tie Breaker to select the oldest member from dim_member")

          val OldestMember=oldestMemberCheck(tiedRecentTrxn,tieBreakerColumns)

          OldestMember.unionAll(resolvedRecentTrxn).unionAll(resolvedMembersWithHighPriority).
          unionAll(resolvedPrimaryAccountHolder).unionAll(resolvedDF)
        }

        else {
          logger.debug("Tied Data Frame is empty ,remaining checks not required")
          resolvedRecentTrxn.unionAll(resolvedMembersWithHighPriority).unionAll(resolvedPrimaryAccountHolder)
            .unionAll(resolvedDF)

        }
      }

      else {
        logger.debug("Tied Data Frame is empty ,remaining checks not required")
        resolvedMembersWithHighPriority.unionAll(resolvedPrimaryAccountHolder).unionAll(resolvedDF)

      }
    }

    else {
      logger.debug("Tied Data Frame is empty ,remaining checks not required")
      resolvedPrimaryAccountHolder.unionAll(resolvedDF)
    }

  }
}
