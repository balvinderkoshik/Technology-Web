package com.express.cm.criteria

import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.{MatchStatusColumn, MatchTypeKeyColumn, MatchTypeKeys, MemberKeyColumn}
import com.express.cm.lookup.LookUpTable.MemberDimensionLookup
import com.express.cm.lookup.LookUpTableUtil
import com.express.cm.processing.LoyaltyWareFile.IpCodeCol
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Ip Code Match Function
  *
  * @author mbadgujar
  */
object IPCodeMatch extends MatchTrait {

  override def matchFunction(source: DataFrame): DataFrame = {
    import source.sqlContext.implicits._
    val memberData = LookUpTableUtil.getLookupTable(MemberDimensionLookup).alias(LookUpAlias)
    val ipCodeSourceData = source.alias(SourceAlias)
    val ipCodeJoined = ipCodeSourceData.join(memberData.filter(col(IpCodeCol).isNotNull), Seq("ip_code"), "left")

    val ipCodeMatched = ipCodeJoined.filter(not(isnull($"$LookUpAlias.$MemberKeyColumn")))
      .select(source.getColumns(SourceAlias, CMMetaColumns) :+ $"$LookUpAlias.$MemberKeyColumn": _*)
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.IpCode))
      .select(source.getColumns: _*)

    val ipCodeUnMatched = ipCodeJoined.filter(isnull($"$LookUpAlias.$MemberKeyColumn"))
      .select(source.getColumns(SourceAlias): _*)

    ipCodeMatched.unionAll(ipCodeUnMatched)
  }
}