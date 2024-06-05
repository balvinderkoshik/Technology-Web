package com.express.cm.criteria

import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.express.cm._
import com.express.cm.lookup.LookUpTable.{CardHistoryFactLookup, CardTypeDimensionLookup, MemberDimensionLookup}
import com.express.cm.lookup.LookUpTableUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{isnull, lit, not, broadcast}


/**
  * Created by aman.jain on 4/14/2017.
  * this function is used to find match on PLCC + Name  and get the corresponding member key.
  * this function is applicable on Tlog and ADS 400/600 files
  */
class PLCCNameImpliedLoyaltyMatch(nameFormatUDF: UDF) extends MatchTrait {
  val CardHistory = "fact_card_history"
  val CardType = "dim_card_type"
  val Member = "dim_member"
  val MemberAlias = "MemberAlias"

  override def matchFunction(source: DataFrame): DataFrame = {

    val sqlContext = source.sqlContext
    import MatchColumnAliases._
    import sqlContext.implicits._

    val DistinctPLCCName = "DistinctUnmatchedPLCCName"
    val DistinctTempJoined = "DistinctTempJoined"
    val Member = "lookUpDF_Member"
    val CardHistory = "lookUpDF_CardHistory"
    val CardType = "lookUpDF_CardType"
    val tieBreakerCols = List(CardAlias, FormattedNameColumn)
    val functionName = "PLCCNameMatch"
    val UnionDistinctDistinctPLCCName = "UnionTiedDistinctPLCC"

    // filter out unmatched records and records with space
    val unmatchedWithSpace = source
      .withColumn(FormattedNameColumn, nameFormatUDF.apply)
      .filter(CheckEmptyUDF($"$FormattedNameColumn.firstName") and CheckEmptyUDF($"$FormattedNameColumn.lastName"))
      .select(source.getColumns: _*)
    val unmatched = source
      .withColumn(FormattedNameColumn, nameFormatUDF.apply)
      .filter(not(CheckEmptyUDF($"$FormattedNameColumn.firstName") and CheckEmptyUDF($"$FormattedNameColumn.lastName")))
      .alias(SourceAlias)

    val distinctUnmatchedPLCCName = unmatched
      .dropDuplicates(Seq(CardAlias, s"$FormattedNameColumn.firstName", s"$FormattedNameColumn.lastName"))
      .select(CardAlias, FormattedNameColumn)
      .alias(DistinctPLCCName)

    // load the CardHistory , CardType and Member
    val lookUpDF_Member = getLookupTable(MemberDimensionLookup, this).withColumn(FormattedNameColumn, MemberNameFormatUDF.apply)
      .alias(Member)

    val lookUpDF_CardHistory = getLookupTable(CardHistoryFactLookup).alias(CardHistory).filter("tokenized_cc_nbr<>'000000000000000000'")

    val lookUpDF_CardType = getLookupTable(CardTypeDimensionLookup).filter("is_express_plcc='YES'").alias(CardType)

    //creating LookUp to perform final join by performing inner join on CardHistory , CardType and Member
    val lookUpDF = lookUpDF_CardHistory.join(lookUpDF_CardType, Seq("card_type_key"))
      .join(lookUpDF_Member, Seq("member_key"))
      .alias(LookUpAlias)

    // left outer join on Lookup and Source(unmatched records)
    val tempJoinedDF = distinctUnmatchedPLCCName.join(lookUpDF, $"$DistinctPLCCName.$FormattedNameColumn.firstName" === $"$LookUpAlias.$FormattedNameColumn.firstName"
      && $"$DistinctPLCCName.$FormattedNameColumn.lastName" === $"$LookUpAlias.$FormattedNameColumn.lastName" &&
      $"$DistinctPLCCName.$CardAlias" === $"$LookUpAlias.tokenized_cc_nbr", "left")
      .select(distinctUnmatchedPLCCName.getColumns(DistinctPLCCName) :+ $"$LookUpAlias.$MemberKeyColumn": _*)
      .alias(DistinctTempJoined)

    val UnionTiedDistinctPLCC = TieBreaker.tieBreakerCheck(tieBreakerCols, tempJoinedDF, functionName)
      .alias(UnionDistinctDistinctPLCCName)

    val joinedDF = unmatched
      .join(UnionTiedDistinctPLCC, ($"$SourceAlias.$CardAlias" === $"$UnionDistinctDistinctPLCCName.$CardAlias") &&
        $"$SourceAlias.$FormattedNameColumn.firstName" === $"$UnionDistinctDistinctPLCCName.$FormattedNameColumn.firstName" &&
        $"$SourceAlias.$FormattedNameColumn.lastName" === $"$UnionDistinctDistinctPLCCName.$FormattedNameColumn.lastName", "left")

    // Filter the data that are candidate for Implied loyalty match
    val (plccNameImpliedMatched, plccNameImpliedUnMatched) = broadcast(joinedDF)
      .join(lookUpDF_Member.select(MemberKeyColumn, MemberLoyaltyIDColumn),
        $"$UnionDistinctDistinctPLCCName.$MemberKeyColumn" === $"$Member.$MemberKeyColumn")
      .filter(not(isnull($"$UnionDistinctDistinctPLCCName.$MemberKeyColumn")))
      .partition(CheckNotEmptyUDF($"$MemberLoyaltyIDColumn"))

    // filtering out  Matched PLCC+Name, set the member id from matched records
    val plccNameMatched = plccNameImpliedMatched
      .select(source.getColumns(SourceAlias, CMMetaColumns) :+ $"$UnionDistinctDistinctPLCCName.$MemberKeyColumn": _*)
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.PlccNameLoyalty))
      .select(source.getColumns: _*)

    // filtering out unmatched PLCC+Name
    val plccNameUnmatched = joinedDF.filter(isnull($"$UnionDistinctDistinctPLCCName.$MemberKeyColumn"))
      .select(source.getColumns(SourceAlias): _*)
      .unionAll(plccNameImpliedUnMatched.select(source.getColumns(SourceAlias): _*))

    // create union and return the result
    plccNameMatched.unionAll(plccNameUnmatched).unionAll(unmatchedWithSpace)

  }
}