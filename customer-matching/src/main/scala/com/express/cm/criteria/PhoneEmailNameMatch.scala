package com.express.cm.criteria

import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.express.cm._
import com.express.cm.lookup.LookUpTable.{MemberDimensionLookup, MemberMultiEmailDimensionLookup, MemberMultiPhoneDimensionLookup}
import com.express.cm.lookup.LookUpTableUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Employee ID match Function
  * Match Table: DIM_Member
  *
  * @author akshay rochwani
  **/

class PhoneEmailNameMatch(nameFormatUDF: UDF) extends MatchTrait {

  //source file in Customer Matching process
  override def matchFunction(source: DataFrame): DataFrame = {
    import MatchColumnAliases._
    val MemberAlias = "member"
    val UnmatchedPhoneEmailNameAlias = "UnmatchedPhoneEmailName"
    val MemberMultiEmailAlias = "membermultiemail"
    val MemberMultiPhoneAlias = "membermultiphone"
    val JoinedDFAlias = "joineddf"
    val DistinctPhoneEmailName = "UnionTiedDistinctPhoneEmailName"
    val sqlContext = source.sqlContext
    import sqlContext.implicits._

    val tieBreakerCols = List(FormattedPhoneColumn, FormattedEmailColumn, FormattedNameColumn)
    val functionName = "PhoneEmailNameMatch"

    //filter out records with space
    val unmatchedWithSpace = source
      .withColumn(FormattedPhoneColumn, formatPhoneNumber(source(PhoneAlias)))
      .withColumn(FormattedEmailColumn, formatEmailUDF(source(EmailAddressAlias)))
      .withColumn(FormattedNameColumn, nameFormatUDF.apply)
      .filter(CheckEmptyUDF($"$PhoneAlias")
        or CheckEmptyUDF($"$EmailAddressAlias")
        or (CheckEmptyUDF($"$FormattedNameColumn.firstName") and CheckEmptyUDF($"$FormattedNameColumn.lastName")))
      .select(source.getColumns: _*)
    val unmatched = source
      .withColumn(FormattedPhoneColumn, formatPhoneNumber(source(PhoneAlias)))
      .withColumn(FormattedEmailColumn, formatEmailUDF(source(EmailAddressAlias)))
      .withColumn(FormattedNameColumn, nameFormatUDF.apply)
      .filter(not(CheckEmptyUDF($"$PhoneAlias")
        or CheckEmptyUDF($"$EmailAddressAlias")
        or (CheckEmptyUDF($"$FormattedNameColumn.firstName") and CheckEmptyUDF($"$FormattedNameColumn.lastName"))))
      .alias(SourceAlias)

    //filtering out distinct Phone, Email and Name
    val distinctUnmatchedPhoneEmailName = unmatched
      .dropDuplicates(Seq(FormattedPhoneColumn, FormattedEmailColumn, s"$FormattedNameColumn.firstName", s"$FormattedNameColumn.lastName"))
      .select(FormattedPhoneColumn, FormattedEmailColumn, FormattedNameColumn)
      .alias(UnmatchedPhoneEmailNameAlias)

    // load the look-up for Member and MemberMuliEmail
    val lookUpDFOnMember = getLookupTable(MemberDimensionLookup, this)
      .withColumn(FormattedNameColumn, MemberNameFormatUDF.apply).withColumnRenamed(FormattedNameColumn, "members_formatted_name_column").alias(MemberAlias)


    val lookUpDFOnMemberMultiEmail = getLookupTable(MemberMultiEmailDimensionLookup).withColumnRenamed("email_address", "email_address_from_multi_email_table").alias(MemberMultiEmailAlias)

    // join member and multiMemberEmail table on member_key
    val joinedDFOfMemberAndMemberMultiEmail = lookUpDFOnMember.join(lookUpDFOnMemberMultiEmail,
      $"$MemberAlias.$MemberKeyColumn" === $"$MemberMultiEmailAlias.$MemberKeyColumn").drop($"$MemberAlias.$MemberKeyColumn").alias(JoinedDFAlias)

    val lookUpDFOnMemberMultiPhone = getLookupTable(MemberMultiPhoneDimensionLookup).alias(MemberMultiPhoneAlias)

    val joinedDFOfInputWithMemberMultiPhone = joinedDFOfMemberAndMemberMultiEmail.join(lookUpDFOnMemberMultiPhone,
      $"$MemberMultiPhoneAlias.$MemberKeyColumn" === $"$JoinedDFAlias.$MemberKeyColumn").drop($"$MemberMultiPhoneAlias.$MemberKeyColumn")

    // join on phone number, email_address and name fields :: *** Needs to be changed after Address Formatting Logic ***
    /*    val tmpJoinedDF = distinctUnmatchedPhoneEmailName.join(joinedDFOfMemberAndMemberMultiEmail,
          $"$UnmatchedPhoneEmailNameAlias.$FormattedPhoneColumn" === formatPhoneNumber($"$MemberAlias.phone_nbr")
            && formatEmailUDF($"$MemberMultiEmailAlias.email_address") === $"$UnmatchedPhoneEmailNameAlias.$FormattedEmailColumn"
            && $"$UnmatchedPhoneEmailNameAlias.$FormattedNameColumn.firstName" === $"$MemberAlias.$FormattedNameColumn.firstName"
            && $"$UnmatchedPhoneEmailNameAlias.$FormattedNameColumn.lastName" === $"$MemberAlias.$FormattedNameColumn.lastName", "left")
          .select(distinctUnmatchedPhoneEmailName.getColumns(UnmatchedPhoneEmailNameAlias):+ $"$MemberAlias.$MemberKeyColumn": _*)*/

    val tmpJoinedDF = distinctUnmatchedPhoneEmailName.join(joinedDFOfInputWithMemberMultiPhone,
      $"$UnmatchedPhoneEmailNameAlias.$FormattedPhoneColumn" === formatPhoneNumber($"$MemberMultiPhoneAlias.phone_number")
        && formatEmailUDF($"$JoinedDFAlias.email_address_from_multi_email_table") === $"$UnmatchedPhoneEmailNameAlias.$FormattedEmailColumn"
        && $"$UnmatchedPhoneEmailNameAlias.$FormattedNameColumn.firstName" === $"$JoinedDFAlias.members_formatted_name_column.firstName"
        && $"$UnmatchedPhoneEmailNameAlias.$FormattedNameColumn.lastName" === $"$JoinedDFAlias.members_formatted_name_column.lastName", "left")
      .select(distinctUnmatchedPhoneEmailName.getColumns(UnmatchedPhoneEmailNameAlias) :+ $"$JoinedDFAlias.$MemberKeyColumn": _*)
    // Calling Tie Breaker
    val UnionTiedDistinctPhoneEmailName = TieBreaker.tieBreakerCheck(tieBreakerCols, tmpJoinedDF, functionName)
      .alias(DistinctPhoneEmailName)

    val joinedDF = unmatched.join(UnionTiedDistinctPhoneEmailName, Seq(FormattedPhoneColumn, FormattedEmailColumn, FormattedNameColumn), "left")

    // unmatched Phone number,  email_address and name
    val emailAddressNameUnmatched = joinedDF.filter(isnull($"$DistinctPhoneEmailName.$MemberKeyColumn"))
      .select(unmatched.getColumns(SourceAlias): _*).drop(FormattedNameColumn).drop(FormattedEmailColumn).drop(FormattedPhoneColumn)

    // Matched Phone number, emailAddress and name  set the member id from matched records
    val emailAddressNameMatched = joinedDF.filter(not(isnull($"$DistinctPhoneEmailName.$MemberKeyColumn")))
      .select(unmatched.getColumns(SourceAlias, CMMetaColumns) :+ $"$DistinctPhoneEmailName.$MemberKeyColumn": _*)
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.PhoneEmailName))
      .select(unmatched.getColumns: _*).drop(FormattedNameColumn).drop(FormattedEmailColumn).drop(FormattedPhoneColumn)

    // create union and return the result
    emailAddressNameMatched.unionAll(emailAddressNameUnmatched).unionAll(unmatchedWithSpace)
  }
}