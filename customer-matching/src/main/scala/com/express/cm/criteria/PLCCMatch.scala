package com.express.cm.criteria

import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs.CheckNotEmptyUDF
import com.express.cm.lookup.LookUpTable.{CardHistoryFactLookup, CardTypeDimensionLookup, MemberDimensionLookup}
import com.express.cm.lookup.LookUpTableUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by aman.jain on 4/7/2017.
  * this generic function is used to find match on PLCC  and get the corresponding member key
  * this function is applicable on Tlog and ADS 400/600 files
  */
object PLCCMatch extends MatchTrait {


  override def matchFunction(source: DataFrame): DataFrame = {

    import MatchColumnAliases._

    val DistinctPLCC = "UnionTiedDistinctPLCC"
    val UnmatcedPLCC = "distinctUnmatchedPLCC"
    val MemberAlias = "member"


    val sqlContext = source.sqlContext
    import sqlContext.implicits._

    val tieBreakerCols = List(CardAlias)
    val functionName = "PLCCMatch"
    // filter out unmatched records
    val unmatched = source.filter(s"$MatchStatusColumn = false").alias(SourceAlias)

    //filtering out distinct PLCC
    val distinctUnmatchedPLCC = unmatched.select(CardAlias).distinct().alias(UnmatcedPLCC)

    // load the CardHistory and CardType
    val lookUpDF_CardHistory = getLookupTable(CardHistoryFactLookup).filter("tokenized_cc_nbr<>'000000000000000000'")
    lookUpDF_CardHistory.registerTempTable("temp_CardHistory")

    val lookUpDF_CardType = getLookupTable(CardTypeDimensionLookup).filter("is_express_plcc='YES'")
    lookUpDF_CardType.registerTempTable("temp_CardType")

    val dimMemberLookupDF = getLookupTable(MemberDimensionLookup).select(MemberKeyColumn, MemberLoyaltyIDColumn)
      .alias(MemberAlias)

    //creating LookUp to perform final join by performing inner join on CardHistory and CardType
    val lookUpDF = sqlContext.sql("select * from temp_CardHistory , temp_CardType where temp_CardHistory.card_type_key = temp_CardType.card_type_key").alias(LookUpAlias)

    // left outer join on Lookup and distinct PLCC records
    val tempJoinedDF = distinctUnmatchedPLCC.join(lookUpDF, distinctUnmatchedPLCC(CardAlias) === $"$LookUpAlias.tokenized_cc_nbr", "left")
      .select(distinctUnmatchedPLCC.getColumns(UnmatcedPLCC) :+ $"$MemberKeyColumn": _*)
    val UnionTiedDistinctPLCC = TieBreaker.tieBreakerCheck(tieBreakerCols, tempJoinedDF, functionName)
      .alias(DistinctPLCC)

    val joinedDF = unmatched.join(UnionTiedDistinctPLCC, Seq(CardAlias), "left")
      .join(dimMemberLookupDF, $"$DistinctPLCC.$MemberKeyColumn" === $"$MemberAlias.$MemberKeyColumn", "left")

    // filtering out implied loyalty members
    val (impliedPLCCMatched, impliedPLCCUnMatched) = joinedDF.filter(not(isnull($"$DistinctPLCC.$MemberKeyColumn")))
      .partition(CheckNotEmptyUDF($"$MemberLoyaltyIDColumn"))

    // filtering out unmatched PLCC
    val PLCCUnmatched = joinedDF.filter(isnull($"$DistinctPLCC.$MemberKeyColumn"))
      .select(unmatched.getColumns(SourceAlias): _*)
      .unionAll(impliedPLCCUnMatched.select(unmatched.getColumns(SourceAlias): _*))
    // filtering out Matched PLCC, set the member id from matched records
    val PLCCMatched = impliedPLCCMatched
      .select(unmatched.getColumns(SourceAlias, CMMetaColumns) :+ $"$DistinctPLCC.$MemberKeyColumn": _*)
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.PlccLoyalty))
      .select(unmatched.getColumns: _*)

    // create union and return the result
    PLCCMatched.unionAll(PLCCUnmatched)
  }

}
