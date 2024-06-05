package com.express.cm.criteria

import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.express.cm._
import com.express.cm.lookup.LookUpTable.{CardHistoryFactLookup, CardTypeDimensionLookup, MemberDimensionLookup}
import com.express.cm.lookup.LookUpTableUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{isnull, lit, not}


/**
  * Created by aman.jain on 4/14/2017.
  * this function is used to find match on PLCC + Name  and get the corresponding member key.
  * this function is applicable on Tlog and ADS 400/600 files
  */
class PLCCNameMatch(nameFormatUDF: UDF) extends MatchTrait {
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
    val tieBreakerCols = List(CardAlias,FormattedNameColumn)
    val functionName = "PLCCNameMatch"
    val UnionDistinctDistinctPLCCName="UnionTiedDistinctPLCC"

    // filter out unmatched records and records with space
    val unmatchedWithSpace=source
      .withColumn(FormattedNameColumn, nameFormatUDF.apply)
      .filter(CheckEmptyUDF($"$FormattedNameColumn.firstName") and CheckEmptyUDF($"$FormattedNameColumn.lastName"))
      .select(source.getColumns:_*)
    val unmatched = source
      .withColumn(FormattedNameColumn, nameFormatUDF.apply)
      .filter(not(CheckEmptyUDF($"$FormattedNameColumn.firstName") and CheckEmptyUDF($"$FormattedNameColumn.lastName")))
      .alias(SourceAlias)

    val distinctUnmatchedPLCCName = unmatched
      .dropDuplicates(Seq(CardAlias, s"$FormattedNameColumn.firstName", s"$FormattedNameColumn.lastName"))
      .select(CardAlias, FormattedNameColumn)
      .alias(DistinctPLCCName)

    // load the CardHistory , CardType and Member
    val lookUpDF_Member = getLookupTable(MemberDimensionLookup, this).withColumn(FormattedNameColumn, MemberNameFormatUDF.apply).alias(Member)

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

    /*
        NOTE: Temporary resolution for the org.apache.spark.sql.AnalysisException: resolved attribute(s) missing Issue
        ==> https://issues.apache.org/jira/browse/SPARK-10925
        Due to Complex chaining of the unmatched dataframes, and aliases set at each CM functions, Spark is unable to resolve the CardAlias column
        Workaround: Renamed the column and used it for joining and renamed it back to original.
        TODO: Check if this is fixed with Latest Spark version
     */

    val UnionTiedDistinctPLCC = TieBreaker.tieBreakerCheck(tieBreakerCols, tempJoinedDF,functionName)
      .alias(UnionDistinctDistinctPLCCName)

    val tempCardCol = "TempCard"

    val joinedDF = unmatched.withColumnRenamed(CardAlias, tempCardCol)
      .join(UnionTiedDistinctPLCC, ($"$tempCardCol" === $"$UnionDistinctDistinctPLCCName.$CardAlias") &&
        $"$SourceAlias.$FormattedNameColumn.firstName" === $"$UnionDistinctDistinctPLCCName.$FormattedNameColumn.firstName" &&
        $"$SourceAlias.$FormattedNameColumn.lastName" === $"$UnionDistinctDistinctPLCCName.$FormattedNameColumn.lastName", "left")

    // filtering out unmatched PLCC+Name
    val PLCCNameUnmatched = joinedDF.filter(isnull($"$UnionDistinctDistinctPLCCName.$MemberKeyColumn"))
      .select(source.getColumns(SourceAlias, List(CardAlias)) :+ $"$tempCardCol": _*)
      .withColumnRenamed(tempCardCol, CardAlias)
      .select(source.getColumns: _*)

    // filtering out  Matched PLCC+Name, set the member id from matched records
    val PLCCNameMatched = joinedDF.filter(not(isnull($"$UnionDistinctDistinctPLCCName.$MemberKeyColumn")))
      .select(source.getColumns(CMMetaColumns :+ CardAlias) :+ $"$tempCardCol" :+ $"$UnionDistinctDistinctPLCCName.$MemberKeyColumn": _*)
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.PlccName))
      .withColumnRenamed(tempCardCol, CardAlias)
      .select(source.getColumns: _*)

    // create union and return the result
    PLCCNameMatched.unionAll(PLCCNameUnmatched).unionAll(unmatchedWithSpace)

  }
}