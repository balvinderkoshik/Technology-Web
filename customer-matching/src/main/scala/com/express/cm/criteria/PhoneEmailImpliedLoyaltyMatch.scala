package com.express.cm.criteria

import com.express.cdw.MatchColumnAliases._
import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.express.cm._
import com.express.cm.lookup.LookUpTable.{MemberMultiEmailDimensionLookup, MemberMultiPhoneDimensionLookup, MemberDimensionLookup}
import com.express.cm.lookup.LookUpTableUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Phone Email match function
  *
  * @author akshay rochwani
  **/

class PhoneEmailImpliedLoyaltyMatch(nameFormatUDF: UDF) extends MatchTrait {

  //source file in Customer Matching process
  override def matchFunction(source: DataFrame): DataFrame = {

    val UnmatchedPhoneEmailAlias = "UnmatchedPhoneEmail"
    val MemberMultiEmailAlias = "membermultiemail"
    val MemberMultiPhoneAlias = "membermultiphone"
    val MemberAlias = "member"
    val DistinctPhoneEmail = "UnionTiedDistinctPhoneEmail"
    val sqlContext = source.sqlContext
    import sqlContext.implicits._

    val tieBreakerCols = List(FormattedPhoneColumn, FormattedEmailColumn)
    val functionName = "PhoneEmailMatch"

    val (invalidForMatch, validForMatch) = source
      .withColumn(FormattedPhoneColumn, formatPhoneNumber(source(PhoneAlias)))
      .withColumn(FormattedEmailColumn, formatEmailUDF(source(EmailAddressAlias)))
      .withColumn(FormattedNameColumn, nameFormatUDF.apply)
      .partition {
        CheckEmptyUDF($"$PhoneAlias") or
          CheckEmptyUDF($"$EmailAddressAlias")
      }

    val unmatched = validForMatch.alias(SourceAlias)

    //filtering out distinct phone and email
    val distinctUnmatchedPhoneEmail = unmatched.select(FormattedPhoneColumn,FormattedEmailColumn)
      .distinct()
      .alias(UnmatchedPhoneEmailAlias)

/*
    // load the look-up for Member and MemberMuliEmail
    val lookUpDFOnMember = getLookupTable(MemberDimensionLookup).select(MemberKeyColumn, MemberLoyaltyIDColumn)
      .alias(MemberAlias)*/

    val lookUpDFOnMemberMultiEmail = getLookupTable(MemberMultiEmailDimensionLookup).alias(MemberMultiEmailAlias)
      //.filter("valid_email='Y'")
    val lookUpDFOnMemberMultiPhone = getLookupTable(MemberMultiPhoneDimensionLookup).alias(MemberMultiPhoneAlias)
    // join member and multiMemberEmail table on member_key
    val joinedDFOfMemberMultiPhoneAndMemberMultiEmail = lookUpDFOnMemberMultiPhone.join(lookUpDFOnMemberMultiEmail,
      $"$MemberMultiPhoneAlias.$MemberKeyColumn" === $"$MemberMultiEmailAlias.$MemberKeyColumn").drop($"$MemberMultiPhoneAlias.$MemberKeyColumn").filter("is_loyalty_flag='YES' and is_loyalty_email='YES'")

    // join on email_address and phone fields :: *** Needs to be changed after Address Formatting Logic ***
    val tmpJoinedDF = distinctUnmatchedPhoneEmail.join(joinedDFOfMemberMultiPhoneAndMemberMultiEmail,
      $"$UnmatchedPhoneEmailAlias.$FormattedPhoneColumn" === formatPhoneNumber($"$MemberMultiPhoneAlias.phone_number")
        && formatEmailUDF($"$MemberMultiEmailAlias.email_address") === $"$UnmatchedPhoneEmailAlias.$FormattedEmailColumn", "left")
      .select(distinctUnmatchedPhoneEmail.getColumns(UnmatchedPhoneEmailAlias):+ $"$MemberKeyColumn": _*)

    val UnionTiedDistinctPhoneEmail = TieBreaker.tieBreakerCheck(tieBreakerCols, tmpJoinedDF,functionName)
                                      .alias(DistinctPhoneEmail)

    val joinedDF = unmatched.join(UnionTiedDistinctPhoneEmail, Seq(FormattedPhoneColumn, FormattedEmailColumn), "left")
      //.join(lookUpDFOnMember, $"$DistinctPhoneEmail.$MemberKeyColumn" === $"$MemberAlias.$MemberKeyColumn", "left")

    //Filter the data matching Implied loyalty criteria
    val phoneEmailImpliedMatched = joinedDF
      .filter(not(isnull($"$DistinctPhoneEmail.$MemberKeyColumn")))

    // unmatched email_address and email
    val phoneEmailUnmatched = joinedDF.filter(isnull($"$DistinctPhoneEmail.$MemberKeyColumn"))
      .select(unmatched.getColumns(SourceAlias): _*)
     // .unionAll(phoneEmailImpliedUnMatched .select(unmatched.getColumns(SourceAlias): _*))
      .drop(FormattedEmailColumn).dropColumns(Seq(FormattedEmailColumn, FormattedPhoneColumn, FormattedNameColumn))

    // Matched emailAddress and email  set the member id from matched records
    val phoneEmailMatched = phoneEmailImpliedMatched.filter(not(isnull($"$DistinctPhoneEmail.$MemberKeyColumn")))
      .select(unmatched.getColumns(SourceAlias, CMMetaColumns) :+ $"$DistinctPhoneEmail.$MemberKeyColumn": _*)
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.PhoneEmailLoyalty))
      .select(unmatched.getColumns: _*).dropColumns(Seq(FormattedEmailColumn, FormattedPhoneColumn, FormattedNameColumn))

    // create union and return the result
    phoneEmailMatched.unionAll(phoneEmailUnmatched).unionAll(invalidForMatch.select(source.getColumns: _*))

  }
}
