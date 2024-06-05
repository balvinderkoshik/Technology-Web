package com.express.cm.criteria


import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.express.cm.lookup.LookUpTable._
import com.express.cm.lookup.LookUpTableUtil._
import com.express.cm.{UDF, _}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by bhautik.patel on 11/04/17.
  */
class PhoneNameMatch(nameFormatUDF: UDF) extends MatchTrait {


  override def matchFunction(source: DataFrame): DataFrame = {

    val DistinctPhoneName = "UnionTiedDistinctPhoneName"
    val UnmatchedPhoneName = "distinctUnmatchedPhoneName"
    val multiPhoneAlias = "multiphone"
    val MemberAlias = "dimMember"
    import MatchColumnAliases._
    val sqlContext = source.sqlContext
    import sqlContext.implicits._

    //filter out records with space
    val unmatchedWithSpace = source
      .withColumn(FormattedPhoneColumn, formatPhoneNumber(source(PhoneAlias)))
      .withColumn(FormattedNameColumn, nameFormatUDF.apply)
      .filter(CheckEmptyUDF($"$PhoneAlias")
        or (CheckEmptyUDF($"$FormattedNameColumn.firstName") and CheckEmptyUDF($"$FormattedNameColumn.lastName")))
      .select(source.getColumns:_*)
    val unmatched = source
      .withColumn(FormattedPhoneColumn, formatPhoneNumber(source(PhoneAlias)))
      .withColumn(FormattedNameColumn, nameFormatUDF.apply)
      .filter(not(CheckEmptyUDF($"$PhoneAlias")
        or (CheckEmptyUDF($"$FormattedNameColumn.firstName") and CheckEmptyUDF($"$FormattedNameColumn.lastName"))))
      .alias(SourceAlias)


    val tieBreakerCols = List(FormattedPhoneColumn , FormattedNameColumn)
    val functionName = "PhoneNameMatch"
    val distinctUnmatchedPhoneName = unmatched
      .dropDuplicates(Seq(FormattedPhoneColumn, s"$FormattedNameColumn.firstName", s"$FormattedNameColumn.lastName"))
      .select(FormattedPhoneColumn, FormattedNameColumn)
      .alias(UnmatchedPhoneName)

    val lookupMultiPhoneDF = getLookupTable(MemberMultiPhoneDimensionLookup, this)
      .alias(multiPhoneAlias)

    val lookUpMemberDF = getLookupTable(MemberDimensionLookup, this)
                  .withColumn(FormattedNameColumn, MemberNameFormatUDF.apply)
                  .alias(MemberAlias)





    //val intermediateDF = distinctUnmatchedPhoneName.join(lookupMultiPhoneDF,
     // $"$UnmatchedPhoneName.$FormattedPhoneColumn" === $"$multiPhoneAlias.phone_number","left")

    // load the look-up
    val lookUpDF = lookUpMemberDF.join(lookupMultiPhoneDF,$"$MemberAlias.$MemberKeyColumn"===$"$multiPhoneAlias.$MemberKeyColumn")
        .withColumn(FormattedPhoneColumn,formatPhoneNumber(lookupMultiPhoneDF("phone_number")))
        .drop($"$multiPhoneAlias.$MemberKeyColumn")
        .alias(LookUpAlias)


    val tempJoinedDF = distinctUnmatchedPhoneName.join(lookUpDF,
      $"$UnmatchedPhoneName.$FormattedNameColumn.firstName" === $"$LookUpAlias.$FormattedNameColumn.firstName"
        && $"$UnmatchedPhoneName.$FormattedNameColumn.lastName" === $"$LookUpAlias.$FormattedNameColumn.lastName"
        && $"$UnmatchedPhoneName.$FormattedPhoneColumn" === $"$LookUpAlias.$FormattedPhoneColumn", "left" )
      .select(distinctUnmatchedPhoneName.getColumns(UnmatchedPhoneName):+ $"$LookUpAlias.$MemberKeyColumn": _*)
   /* val tempJoinedDF = distinctUnmatchedPhoneName.join(lookUpDF,
      $"$UnmatchedPhoneName.$FormattedPhoneColumn" === $"$LookUpAlias.phone_nbr"
        && $"$UnmatchedPhoneName.$FormattedNameColumn.firstName" === $"$LookUpAlias.$FormattedNameColumn.firstName"
        && $"$UnmatchedPhoneName.$FormattedNameColumn.lastName" === $"$LookUpAlias.$FormattedNameColumn.lastName", "left")
      .select(distinctUnmatchedPhoneName.getColumns(UnmatchedPhoneName):+ $"$LookUpAlias.$MemberKeyColumn": _*)*/

    //Calling Tie Breaker
    val UnionTiedDistinctPhoneName = TieBreaker.tieBreakerCheck(tieBreakerCols, tempJoinedDF,functionName)
            .alias(DistinctPhoneName)

    val joinedDF = unmatched.join(UnionTiedDistinctPhoneName,
      $"$SourceAlias.$FormattedPhoneColumn" === $"$DistinctPhoneName.$FormattedPhoneColumn"
        && $"$SourceAlias.$FormattedNameColumn.firstName" === $"$DistinctPhoneName.$FormattedNameColumn.firstName"
        && $"$SourceAlias.$FormattedNameColumn.lastName" === $"$DistinctPhoneName.$FormattedNameColumn.lastName", "left")


    // filtering out unmatched Phone & Name
    val PhoneNameUnmatched = joinedDF.filter(isnull($"$DistinctPhoneName.$MemberKeyColumn"))
      .select(source.getColumns(SourceAlias): _*)

    // filtering out Matched Phone & Name, set the member id from matched records
    val PhoneNameMatched = joinedDF.filter(not(isnull($"$DistinctPhoneName.$MemberKeyColumn")))
      .select(unmatched.getColumns(SourceAlias, CMMetaColumns) :+ $"$DistinctPhoneName.$MemberKeyColumn": _*)
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.PhoneName))
      .select(source.getColumns: _*)

    PhoneNameMatched.unionAll(PhoneNameUnmatched).unionAll(unmatchedWithSpace)

  }
}
