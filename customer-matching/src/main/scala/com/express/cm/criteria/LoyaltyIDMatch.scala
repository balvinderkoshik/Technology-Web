package com.express.cm.criteria

import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.express.cm.lookup.LookUpTable._
import com.express.cm.lookup.LookUpTableUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by bhautik.patel on 12/04/17.
  */
object LoyaltyIDMatch extends MatchTrait {

  val MemberLoyaltyIDColumn = "loyalty_id"

  override def matchFunction(source: DataFrame): DataFrame = {

    val sqlContext = source.sqlContext
    import sqlContext.implicits._
    import MatchColumnAliases._

    val tieBreakerCols = List(ImpliedLoyaltyIdAlias)
    val distinctLoyaltyAlias = "loyalty_distinct"
    val functionName = "LoyaltyMatch"

    //  //filter out records with space
    val emptyLoyaltyIDRecords = source.filter(CheckEmptyUDF($"$ImpliedLoyaltyIdAlias"))

    // filter out  unmatched
    val unmatched = source.filter(not(CheckEmptyUDF($"$ImpliedLoyaltyIdAlias"))).alias(SourceAlias)

    val distinctLoyaltyIds = unmatched.dropDuplicates(Seq(ImpliedLoyaltyIdAlias)).select(ImpliedLoyaltyIdAlias)

    // load the look-up
    val lookUpDF = getLookupTable(MemberDimensionLookup, this).alias(LookUpAlias)

    //Join
    val tempJoinedDF = distinctLoyaltyIds
      .join(lookUpDF, $"$ImpliedLoyaltyIdAlias" === $"$LookUpAlias.$MemberLoyaltyIDColumn", "left")
      .select(distinctLoyaltyIds.getColumns :+ $"$LookUpAlias.$MemberKeyColumn": _*)
      .distinct()

    val unionTiedDistinctLoyalty = TieBreaker.tieBreakerCheck(tieBreakerCols, tempJoinedDF, functionName)
      .alias(distinctLoyaltyAlias)

    val joinedDF = unmatched.join(unionTiedDistinctLoyalty, Seq(ImpliedLoyaltyIdAlias), "left")

    // unmatched loyalty ids
    val loyaltyIdUnmatched = joinedDF.filter(isnull($"$distinctLoyaltyAlias.$MemberKeyColumn"))
      .select(source.getColumns(SourceAlias): _*)

    // Matched loyalty id, set the member id from matched records
    val loyaltyIdMatched = joinedDF.filter(not(isnull($"$distinctLoyaltyAlias.$MemberKeyColumn")))
      .select(source.getColumns(SourceAlias, CMMetaColumns) :+ $"$distinctLoyaltyAlias.$MemberKeyColumn": _*)
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.LoyaltyId))
      .select(source.getColumns: _*)

    // create union and return the result
    loyaltyIdMatched.unionAll(loyaltyIdUnmatched).unionAll(emptyLoyaltyIDRecords)
  }

}
