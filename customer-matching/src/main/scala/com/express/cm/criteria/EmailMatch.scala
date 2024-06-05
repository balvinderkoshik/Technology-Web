package com.express.cm.criteria

import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.express.cm._
import com.express.cm.lookup.LookUpTable.MemberMultiEmailDimensionLookup
import com.express.cm.lookup.LookUpTableUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Email Match Function
  * Match Table: DIM_MEMBER & DIM_MEMBER_MULTI_EMAIL
  *
  * @author Gaurav.Maheshwari
  */
class EmailMatch(nameFormatUDF: Option[UDF] = None) extends MatchTrait {


  override def matchFunction(source: DataFrame): DataFrame = {
    import MatchColumnAliases._
    val MemberMultiEmailAlias = "membermultiemail"
    val DistinctEmail = "UnionTiedDistinctEmail"
    val functionName = "EmailMatch"
    val UnmatchedEmail = "distinctUnmatchedEmail"

    val sqlContext = source.sqlContext
    import sqlContext.implicits._

    /*
      If the input dataset has name columns, filter out records consisting of names.
      This match function if only applied for emails without name information.
     */
    val (invalidForMatch, validForMatch) = nameFormatUDF match {
      case None =>
        source
          .withColumn(EmailAddressAlias, formatEmailUDF($"$EmailAddressAlias"))
          .partition(CheckEmptyUDF($"$EmailAddressAlias"))
      case Some(nfUDF) =>
        source
          .withColumn(EmailAddressAlias, formatEmailUDF($"$EmailAddressAlias"))
          .withColumn(FormattedNameColumn, nfUDF.apply)
          .partition {
            CheckEmptyUDF($"$EmailAddressAlias") or
              (CheckNotEmptyUDF($"$FormattedNameColumn.firstName") and CheckNotEmptyUDF($"$FormattedNameColumn.lastName"))
          }
    }

    val unmatched = validForMatch.alias(SourceAlias)
    val tieBreakerCols = List(EmailAddressAlias)


    //filtering out distinct EMail
    val distinctUnmatchedEmail = unmatched.select(EmailAddressAlias).distinct().alias(UnmatchedEmail)

    // load the look-up
    //val lookUpDFOnMember = getLookupTable(MemberDimensionLookup, this).alias(MemberAlias)
    val lookUpDFOnMemberMultiEmail = getLookupTable(MemberMultiEmailDimensionLookup).alias(MemberMultiEmailAlias)


    // join member and multiMemberEmail table
    //val joinedDFOfMemberAndMemberMultiEmail = lookUpDFOnMember.join(lookUpDFOnMemberMultiEmail, Seq("member_key"))
    // join on email_address and name fields :: *** Needs to be changed after Address Formatting Logic ***

    val tmpJoinedDF = distinctUnmatchedEmail.join(lookUpDFOnMemberMultiEmail,
      formatEmailUDF($"$MemberMultiEmailAlias.email_address") === $"$UnmatchedEmail.$EmailAddressAlias", "left")
      .select(distinctUnmatchedEmail.getColumns(UnmatchedEmail) :+ $"$MemberMultiEmailAlias.$MemberKeyColumn": _*)

    //calling Tie Breaker

    val UnionTiedDistinctEmail = TieBreaker.tieBreakerCheck(tieBreakerCols, tmpJoinedDF, functionName)
      .alias(DistinctEmail)

    val joinedDF = unmatched.withColumnRenamed(EmailAddressAlias, "email")
      .join(UnionTiedDistinctEmail.withColumn(EmailAddressAlias, formatEmailUDF($"$EmailAddressAlias")).withColumnRenamed(EmailAddressAlias, "email"), Seq("email"), "left")

    // unmatched emailAddress
    val emailAddressUnmatched = joinedDF.filter(isnull($"$DistinctEmail.$MemberKeyColumn"))
      .select(source.getColumns(SourceAlias, List(EmailAddressAlias)) :+ $"email": _*)
      .withColumnRenamed("email", EmailAddressAlias)
      .select(source.getColumns: _*)

    // Matched emailAddress, set the member id from matched records
    val emailAddressMatched = joinedDF.filter(not(isnull($"$DistinctEmail.$MemberKeyColumn")))
      //.select(unmatched.getColumns(SourceAlias, CMMetaColumns) :+ $"$DistinctEmail.$MemberKeyColumn": _*)
      .select(unmatched.getColumns(SourceAlias, CMMetaColumns :+ EmailAddressAlias) :+ $"$DistinctEmail.$MemberKeyColumn" :+ $"email": _*)
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.Email))
      .withColumnRenamed("email", EmailAddressAlias)
      .select(source.getColumns: _*)

    // create union and return the result
    emailAddressMatched.unionAll(emailAddressUnmatched).unionAll(invalidForMatch.select(source.getColumns: _*))
  }


}
