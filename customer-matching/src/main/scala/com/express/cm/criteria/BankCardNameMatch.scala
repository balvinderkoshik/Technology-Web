package com.express.cm.criteria

import com.express.cdw.MatchColumnAliases._
import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.express.cm._
import com.express.cm.lookup.LookUpTable.{MemberDimensionLookup, TenderHistoryFactLookup, TenderTypeDimensionLookup}
import com.express.cm.lookup.LookUpTableUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{isnull, lit, not}


/**
  * Bank card and Name check match Function it is performed on TLOG_CUSTOMER, ADS 400, ADS 600
  *
  * @author pmishra
  */
class BankCardNameMatch(nameFormatUDF: UDF) extends MatchTrait {

  val MemberAlias = "Member"
  val TenderHistoryAlias = "TenderHist"
  val TenderTypeAlias  = "TenderType"


  //here source = tlog_customer file in Customer Matching process
  override def matchFunction(source: DataFrame): DataFrame = {

    val sqlContext = source.sqlContext
    import sqlContext.implicits._

    val DistinctBankCardName = "DistinctUnmatchedBankCardName"
    val DistinctTempJoined="DistinctTempJoined"
    val tieBreakerCols = List(CardAlias,FormattedNameColumn)
    val functionName = "BankCardNameMatch"
    val UnionDistinctBankCardName = "UnionTiedDistinctBankCardName"

    //filter out records with space
    val unmatchedWithSpace=source
      .withColumn(FormattedNameColumn, nameFormatUDF.apply)
      .filter(CheckEmptyUDF($"$FormattedNameColumn.firstName") and CheckEmptyUDF($"$FormattedNameColumn.lastName"))
      .select(source.getColumns:_*)
    val unmatched = source.withColumn(FormattedNameColumn, nameFormatUDF.apply)
      .filter(not(CheckEmptyUDF($"$FormattedNameColumn.firstName") and CheckEmptyUDF($"$FormattedNameColumn.lastName")))
      .alias(SourceAlias)

    val DistinctUnmatchedBankCardName = unmatched
      .dropDuplicates(Seq(CardAlias,CardTypeAlias, s"$FormattedNameColumn.firstName", s"$FormattedNameColumn.lastName"))
      .select(CardAlias,CardTypeAlias,FormattedNameColumn)
      .alias(DistinctBankCardName)
    // load the look-up

    val lookUpDF_TenderHis = getLookupTable(TenderHistoryFactLookup).filter("tokenized_cc_nbr<>0").as(TenderHistoryAlias)
    val lookUpDF_TenderType = getLookupTable(TenderTypeDimensionLookup).filter("is_bank_card='YES'").alias(TenderTypeAlias)
    val lookUpDF_Member = getLookupTable(MemberDimensionLookup, this)
      .withColumn(FormattedNameColumn, MemberNameFormatUDF.apply).alias(MemberAlias)

    //The first join is performed on the tender type key from dim_tender_type,fact_tender_history and the second join is performed on member key from fact_tender_history,dim_member
    val lookUpDF = lookUpDF_TenderHis
      .join(lookUpDF_TenderType, $"$TenderHistoryAlias.tender_type_key"  === $"$TenderTypeAlias.tender_type_key")
      .join(lookUpDF_Member, $"$TenderHistoryAlias.member_key" === $"$MemberAlias.member_key")

    // left outer join on Lookup and Source(unmatched records)
    val tempJoinedDF = DistinctUnmatchedBankCardName.join(lookUpDF, (strToLongUDF($"$DistinctBankCardName.$CardAlias") === $"$TenderHistoryAlias.tokenized_cc_nbr")
      && $"$DistinctBankCardName.$FormattedNameColumn.firstName" === $"$MemberAlias.$FormattedNameColumn.firstName"
      && $"$DistinctBankCardName.$FormattedNameColumn.lastName" === $"$MemberAlias.$FormattedNameColumn.lastName", "left")
      .select(DistinctUnmatchedBankCardName.getColumns(DistinctBankCardName):+ $"$TenderHistoryAlias.$MemberKeyColumn": _*)
      .distinct()
      .alias(DistinctTempJoined)

    val UnionTiedDistinctBankCardName = TieBreaker.tieBreakerCheck(tieBreakerCols,tempJoinedDF,functionName).alias(UnionDistinctBankCardName)
    /*
       NOTE: Temporary resolution for the org.apache.spark.sql.AnalysisException: resolved attribute(s) missing Issue
       ==> https://issues.apache.org/jira/browse/SPARK-10925
       Due to Complex chaining of the unmatched dataframes, and aliases set at each CM functions, Spark is unable to resolve the CardAlias column
       Workaround: Renamed the column and used it for joining and renamed it back to original.
       TODO: Check if this is fixed with Latest Spark version
    */
    val tempCardTypeCol = "TempCardType"

    // The below join is performed on bank card (i.e. Tokenized credit card number and tender type)and Customer Name from lookUpDF and dim_member
    val joinedDF = unmatched.withColumnRenamed(CardTypeAlias, tempCardTypeCol).join(UnionTiedDistinctBankCardName,
       $"$SourceAlias.$CardAlias" === $"$UnionDistinctBankCardName.$CardAlias"
      && ($"$SourceAlias.$FormattedNameColumn.firstName" === $"$UnionDistinctBankCardName.$FormattedNameColumn.firstName")
      && ($"$SourceAlias.$FormattedNameColumn.lastName" === $"$UnionDistinctBankCardName.$FormattedNameColumn.lastName"), "left")

    // Un-matched result set
    val BankCardNameUnmatched = joinedDF.filter(isnull($"$UnionDistinctBankCardName.$MemberKeyColumn"))
      .select(source.getColumns(SourceAlias, List(CardTypeAlias)) :+ $"$tempCardTypeCol": _*)
      .withColumnRenamed(tempCardTypeCol, CardTypeAlias)
      .select(source.getColumns: _*)

    // Matched result set
    val BankCardNameMatched = joinedDF.filter(not(isnull($"$UnionDistinctBankCardName.$MemberKeyColumn")))
      .select(source.getColumns(SourceAlias, CMMetaColumns :+ CardTypeAlias) :+ $"$tempCardTypeCol" :+ $"$UnionDistinctBankCardName.$MemberKeyColumn": _*)
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.BankCardName))
      .withColumnRenamed(tempCardTypeCol, CardTypeAlias)
      .select(source.getColumns: _*)
    // create union and return the result
    BankCardNameMatched.unionAll(BankCardNameUnmatched).unionAll(unmatchedWithSpace)
  }
}
