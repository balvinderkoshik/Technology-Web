package com.express.cm.criteria

import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import com.express.cm.{UDF, formatPhoneNumber}
import com.express.cm.lookup.LookUpTable._
import com.express.cm.lookup.LookUpTableUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by bhautik.patel on 11/04/17.
  */
class PhoneMatch(nameFormatUDF: Option[UDF] = None) extends MatchTrait {


  override def matchFunction(source: DataFrame): DataFrame = {

    import MatchColumnAliases._

    val sqlContext = source.sqlContext
    import sqlContext.implicits._
    val DistinctPhone = "distinctUnmatchedPhone"
    val MultiPhone = "lookUpDF_MemberMultiPhone"
    val DistinctUnionPhone = "unionTiedDistinctPhone"
    val tieBreakerCols = List(PhoneAlias)
    val functionName = "PhoneMatch"


    /*
    If the input dataset has non empty name columns, filter out records consisting of names.
    This match function if only applied for phones without name information.
   */
    val (invalidForMatch, validForMatch) = nameFormatUDF match {
      case None =>
        source
          .withColumn(FormattedPhoneColumn, formatPhoneNumber(source(PhoneAlias)))
          .partition(CheckEmptyUDF($"$PhoneAlias"))
      case Some(nfUDF) =>
        source
          .withColumn(FormattedPhoneColumn, formatPhoneNumber(source(PhoneAlias)))
          .withColumn(FormattedNameColumn, nfUDF.apply)
          .partition {
            CheckEmptyUDF($"$PhoneAlias") or
              (CheckNotEmptyUDF($"$FormattedNameColumn.firstName") and CheckNotEmptyUDF($"$FormattedNameColumn.lastName"))
          }
    }

    //filter out records with space
    val unmatched = validForMatch.alias(SourceAlias)

    val distinctUnmatchedPhone = unmatched.dropDuplicates(Seq(PhoneAlias)).select(PhoneAlias).alias(DistinctPhone)

    // load the look-up
    val lookUpDF_MemberMultiPhone = getLookupTable(MemberMultiPhoneDimensionLookup, this).alias(MultiPhone)
    // join
    val tempJoinedDF = distinctUnmatchedPhone.join(lookUpDF_MemberMultiPhone, $"$DistinctPhone.$PhoneAlias" === $"$MultiPhone.phone_number", "left")
      .select(distinctUnmatchedPhone.getColumns(DistinctPhone) :+ $"$MultiPhone.$MemberKeyColumn": _*)
      .distinct()


    val unionTiedDistinctPhone = TieBreaker.tieBreakerCheck(tieBreakerCols, tempJoinedDF, functionName).alias(DistinctUnionPhone)

    val joinedDF = unmatched.join(unionTiedDistinctPhone, Seq(PhoneAlias), "left")


    // unmatched phone
    val phoneUnmatched = joinedDF.filter(isnull($"$DistinctUnionPhone.$MemberKeyColumn"))
      .select(source.getColumns(SourceAlias): _*)

    // Matched phone, set the member id from matched records
    val phoneMatched = joinedDF.filter(not(isnull($"$DistinctUnionPhone.$MemberKeyColumn")))
      .select(source.getColumns(SourceAlias, CMMetaColumns) :+ $"$DistinctUnionPhone.$MemberKeyColumn": _*)
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.Phone))
      .select(source.getColumns: _*)

    // create union and return the result
    phoneMatched.unionAll(phoneUnmatched).unionAll(invalidForMatch.select(source.getColumns: _*))
  }

}
