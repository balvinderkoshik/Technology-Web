package com.express.cm.criteria

import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.express.cm._
import com.express.cm.lookup.LookUpTable.{MemberDimensionLookup, MemberMultiEmailDimensionLookup}
import com.express.cm.lookup.LookUpTableUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by Gaurav.Maheshwari on 4/20/2017.
  */
class EmailNameMatch(nameFormatUDF: UDF) extends MatchTrait {


  override def matchFunction(source: DataFrame): DataFrame = {

    import MatchColumnAliases._
    val MemberAlias = "member"
    val MemberMultiEmailAlias = "membermultiemail"
    val DistinctEmailName = "UnionTiedDistinctEmailName"
    val UnmatchedEmailName = "distinctUnmatchedEmailName"
    val sqlContext = source.sqlContext
    import sqlContext.implicits._

    val tieBreakerCols = List(EmailAddressAlias, FormattedNameColumn)
    val functionName = "EmailNameMatch"

    //filter out records with space
    val unmatchedWithSpace = source
      .withColumn(FormattedNameColumn, nameFormatUDF.apply)
      .withColumn(EmailAddressAlias, formatEmailUDF($"$EmailAddressAlias"))
      .filter(CheckEmptyUDF($"$EmailAddressAlias")
        or (CheckEmptyUDF($"$FormattedNameColumn.firstName") and CheckEmptyUDF($"$FormattedNameColumn.lastName")))
      .select(source.getColumns:_*)

    val unmatched = source
      .withColumn(FormattedNameColumn, nameFormatUDF.apply)
      .withColumn(EmailAddressAlias, formatEmailUDF($"$EmailAddressAlias"))
      .filter(not(CheckEmptyUDF($"$EmailAddressAlias")
        or (CheckEmptyUDF($"$FormattedNameColumn.firstName") and CheckEmptyUDF($"$FormattedNameColumn.lastName"))))
      .alias(SourceAlias)

    //filtering out distinct EMail and Name
    val distinctUnmatchedEmailName = unmatched
      .dropDuplicates(Seq(EmailAddressAlias, s"$FormattedNameColumn.firstName", s"$FormattedNameColumn.lastName"))
      .select(EmailAddressAlias, FormattedNameColumn)
      .alias(UnmatchedEmailName)


    // load the look-up for Member and MemberMuliEmail
    val lookUpDFOnMember = getLookupTable(MemberDimensionLookup, this).withColumn(FormattedNameColumn, MemberNameFormatUDF.apply).alias(MemberAlias)
    val lookUpDFOnMemberMultiEmail = getLookupTable(MemberMultiEmailDimensionLookup).alias(MemberMultiEmailAlias)

    // join member and multiMemberEmail table on member_key
    val joinedDFOfMemberAndMemberMultiEmail = lookUpDFOnMember.join(lookUpDFOnMemberMultiEmail,Seq("member_key"))

    // join on email_address and name fields :: *** Needs to be changed after Address Formatting Logic ***
    val tmpJoinedDF = distinctUnmatchedEmailName.join(joinedDFOfMemberAndMemberMultiEmail,
      formatEmailUDF($"$MemberMultiEmailAlias.email_address") === distinctUnmatchedEmailName(EmailAddressAlias)
        && $"$UnmatchedEmailName.$FormattedNameColumn.firstName" === $"$MemberAlias.$FormattedNameColumn.firstName"
        && $"$UnmatchedEmailName.$FormattedNameColumn.lastName" === $"$MemberAlias.$FormattedNameColumn.lastName", "left")
        .select(distinctUnmatchedEmailName.getColumns(UnmatchedEmailName):+ $"$MemberAlias.$MemberKeyColumn": _*)

    //calling Tie Breaker
    val UnionTiedDistinctEmailName = TieBreaker.tieBreakerCheck(tieBreakerCols, tmpJoinedDF, functionName)
      .alias(DistinctEmailName)

    val joinedDF = unmatched.join(UnionTiedDistinctEmailName,
      $"$SourceAlias.$EmailAddressAlias"===lower($"$DistinctEmailName.$EmailAddressAlias") &&
      $"$SourceAlias.$FormattedNameColumn.firstName" === $"$DistinctEmailName.$FormattedNameColumn.firstName" &&
      $"$SourceAlias.$FormattedNameColumn.lastName" === $"$DistinctEmailName.$FormattedNameColumn.lastName", "left")

    // unmatched email_address and name
    val emailAddressNameUnmatched = joinedDF.filter(isnull($"$DistinctEmailName.$MemberKeyColumn"))
      .select(source.getColumns(SourceAlias): _*)

    // Matched emailAddress and name  set the member id from matched records
    val emailAddressNameMatched = joinedDF.filter(not(isnull($"$DistinctEmailName.$MemberKeyColumn")))
      .select(unmatched.getColumns(SourceAlias, CMMetaColumns) :+ $"$DistinctEmailName.$MemberKeyColumn": _*)
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.EmailName))
      .select(source.getColumns: _*)

    // create union and return the result
    emailAddressNameMatched.unionAll(emailAddressNameUnmatched).unionAll(unmatchedWithSpace)
  }
}
