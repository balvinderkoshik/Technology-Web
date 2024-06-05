package com.express.cm.criteria

import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.express.cm.lookup.LookUpTable.MemberDimensionLookup
import com.express.cm.lookup.LookUpTableUtil.getLookupTable
import com.express.cm.{UDF, _}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Name Address Match Functiona
  *
  * @author mbadgujar
  */
class NameAddressMatch(nameFormatUDF: UDF, addressFormatUDF: UDF) extends MatchTrait {

  override def matchFunction(source: DataFrame): DataFrame = {

    import source.sqlContext.implicits._
    val Member = "member"
    val tieBreakerCols = List(FormattedNameColumn, FormattedAddressColumn)
    val functionName = "NameAddressMatch"
    val UnmatchedNameAddress = "distinctUnmatchedNameAddress"
    val DistinctNameAddress = "UnionTiedDistinctNameAddress"
    val Separator = ","

    //filter out records with space
    val (invalidForMatching, validForMatching) = source
      .withColumn(FormattedNameColumn, nameFormatUDF.apply)
      .withColumn(FormattedAddressColumn, addressFormatUDF.apply)
      .partition((CheckEmptyUDF($"$FormattedAddressColumn.address1") and CheckEmptyUDF($"$FormattedAddressColumn.address2"))
        or CheckEmptyUDF($"$FormattedAddressColumn.zipCode")
        or (CheckEmptyUDF($"$FormattedNameColumn.firstName") and CheckEmptyUDF($"$FormattedNameColumn.lastName")))

    val unmatchedWithSpace = invalidForMatching.select(source.getColumns: _*)
    val unmatched = validForMatching.alias(SourceAlias)

    // load the look-up
    val lookUpDFOnMember = getLookupTable(MemberDimensionLookup, this)
      .withColumn(FormattedNameColumn, MemberNameFormatUDF.apply)
      .withColumn(FormattedAddressColumn, MemberAddressFormatUDF.apply)
      .alias(Member)

    val distinctUnmatchedNameAddress = unmatched
      .dropDuplicates(Seq(s"$FormattedNameColumn.firstName", s"$FormattedNameColumn.lastName", s"$FormattedAddressColumn.address1",
        s"$FormattedAddressColumn.address2", s"$FormattedAddressColumn.zipCode"))
      .select(FormattedNameColumn, FormattedAddressColumn)
      .alias(UnmatchedNameAddress)

    val tempJoinedDF = distinctUnmatchedNameAddress.join(lookUpDFOnMember,
      $"$UnmatchedNameAddress.$FormattedNameColumn.firstName" === $"$Member.$FormattedNameColumn.firstName"
        and $"$UnmatchedNameAddress.$FormattedNameColumn.lastName" === $"$Member.$FormattedNameColumn.lastName"
        and concat_ws(Separator, $"$UnmatchedNameAddress.$FormattedAddressColumn.address1", $"$UnmatchedNameAddress.$FormattedAddressColumn.address2")
        === concat_ws(Separator, $"$Member.$FormattedAddressColumn.address1", $"$Member.$FormattedAddressColumn.address2")
        and $"$UnmatchedNameAddress.$FormattedAddressColumn.zipCode" === $"$Member.$FormattedAddressColumn.zipCode", "left")
      .select(distinctUnmatchedNameAddress.getColumns(UnmatchedNameAddress) :+ $"$Member.$MemberKeyColumn": _*)

    //calling Tie Breaker
    val UnionTiedDistinctNameAddress = TieBreaker.tieBreakerCheck(tieBreakerCols, tempJoinedDF, functionName)
      .alias(DistinctNameAddress)

    val joinedDF = unmatched.join(UnionTiedDistinctNameAddress,
      $"$SourceAlias.$FormattedNameColumn.firstName" === $"$DistinctNameAddress.$FormattedNameColumn.firstName"
        and $"$SourceAlias.$FormattedNameColumn.lastName" === $"$DistinctNameAddress.$FormattedNameColumn.lastName"
        and concat_ws(Separator, $"$SourceAlias.$FormattedAddressColumn.address1", $"$SourceAlias.$FormattedAddressColumn.address2")
        === concat_ws(Separator, $"$DistinctNameAddress.$FormattedAddressColumn.address1", $"$DistinctNameAddress.$FormattedAddressColumn.address2")
        and $"$SourceAlias.$FormattedAddressColumn.zipCode" === $"$DistinctNameAddress.$FormattedAddressColumn.zipCode", "left")

    // unmatched
    val addressNameUnmatched = joinedDF.filter(isnull($"$DistinctNameAddress.$MemberKeyColumn"))
      .select(source.getColumns(SourceAlias): _*)

    // Matched
    val addressNameMatched = joinedDF.filter(not(isnull($"$DistinctNameAddress.$MemberKeyColumn")))
      .select(source.getColumns(SourceAlias, CMMetaColumns) :+ $"$DistinctNameAddress.$MemberKeyColumn": _*)
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.NameAddress))
      .select(source.getColumns: _*)

    // create union and return the result
    addressNameMatched.unionAll(addressNameUnmatched).unionAll(unmatchedWithSpace)
  }
}
