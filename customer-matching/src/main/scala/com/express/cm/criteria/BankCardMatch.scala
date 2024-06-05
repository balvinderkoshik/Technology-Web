/**
  * Created by poonam.mishra on 4/5/2017.
  */
package com.express.cm.criteria

import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.express.cm.UDF
import com.express.cm.lookup.LookUpTable.{TenderHistoryFactLookup, TenderTypeDimensionLookup}
import com.express.cm.lookup.LookUpTableUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Bank card check match Function is a generic function used to find match on Bank Cardsand get the corresponding member key
  * it is applied in TLOG_CUSTOMER, ADS 400, ADS 600
  *
  * @author pmishra
  */

class BankCardMatch(nameFormatUDF: UDF) extends MatchTrait {


  //here source = resultant file after Bank card and name Check in Customer Matching process
  override def matchFunction(source: DataFrame): DataFrame = {

    import MatchColumnAliases._
    val DistinctBankCard = "UnionTiedDistinctBankCard"

    val sqlContext = source.sqlContext
    import sqlContext.implicits._

    val TenderHistoryAlias = "TenderHist"
    val TenderTypeAlias = "TenderType"
    val tieBreakerCols = List(CardAlias)
    val functionName = "BankCardMatch"
    val UnmatchedDistinctBankCard = "UnmatchedDistinctBankCard"

    val (invalidForMatch, validForMatch) = source
      .withColumn(FormattedNameColumn, nameFormatUDF.apply)
      .partition(CheckNotEmptyUDF($"$FormattedNameColumn.firstName") and CheckNotEmptyUDF($"$FormattedNameColumn.lastName"))

    val unmatched = validForMatch.alias(SourceAlias)

    //filtering out distinct Bank Card
    val distinctUnmatchedBankCard = unmatched.select(CardAlias, CardTypeAlias).distinct().alias(UnmatchedDistinctBankCard)

    // load the Tender Type and Tender History
    val lookUpDF_TenderHis = getLookupTable(TenderHistoryFactLookup).filter("tokenized_cc_nbr<>0").alias(TenderHistoryAlias)
    val lookUpDF_TenderType = getLookupTable(TenderTypeDimensionLookup).filter("is_bank_card='YES'").alias(TenderTypeAlias)
    val lookUpDF = lookUpDF_TenderHis.join(lookUpDF_TenderType, Seq("tender_type_key"))

    val tempJoinedDF = distinctUnmatchedBankCard.join(lookUpDF,
      strToLongUDF($"$UnmatchedDistinctBankCard.$CardAlias") === $"$TenderHistoryAlias.tokenized_cc_nbr", "left")

    val UnionTiedDistinctBankCard = TieBreaker.tieBreakerCheck(tieBreakerCols, tempJoinedDF, functionName)
      .alias(DistinctBankCard)

    //The below join is performed on bank card (i.e. Tokenized credit card number and tender type) from unmatched and lookUpDF
    val joinedDF = unmatched.join(UnionTiedDistinctBankCard, $"$SourceAlias.$CardAlias" === $"$DistinctBankCard.tender_number", "left")

    // Un-matched result set
    val bankcardUnmatched = joinedDF.filter(isnull($"$DistinctBankCard.$MemberKeyColumn"))
      .select(source.getColumns(SourceAlias): _*)

    //  Matched result set
    val bankcardMatched = joinedDF.filter(not(isnull($"$DistinctBankCard.$MemberKeyColumn")))
      .select(source.getColumns(SourceAlias, CMMetaColumns) :+ $"$DistinctBankCard.$MemberKeyColumn": _*)
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.BankCard))
      .select(source.getColumns: _*)

    // create union and return the result
    bankcardMatched.unionAll(bankcardUnmatched).unionAll(invalidForMatch.select(source.getColumns: _*))

  }
}
