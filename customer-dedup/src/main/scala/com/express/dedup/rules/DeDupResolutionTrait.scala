package com.express.dedup.rules

import com.express.cdw.spark.DataFrameUtils._
import com.express.dedup.model.Member
import com.express.dedup.utils.DedupUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Column, DataFrame, Row}

/**
  * Created by aman.jain on 6/14/2017.
  */
trait DeDupResolutionTrait extends LazyLogging {


  /**
    * Get the Grouping Criteria ID for Dedup function
    *
    * @return Integer ID
    */
  def groupingCriteriaId: Int

  /**
    * Get grouping identification tile for Dedup function
    *
    * @return [[Column]]
    */
  def groupingIdentificationRule: Column

  /**
    * Get columns on which grouping is to be performed
    *
    * @return List of Columns
    */
  def groupingColumns: Seq[String]

  private val AssociatedMembers = "AssociatedMembers"
  private val SimilarGroupRnk = "SimilarGroupRnk"

  // Member resolution function
  private val resolutionFunc: (Seq[Row]) => (Int, Long, Boolean, Row, String) = (members: Seq[Row]) => {
    val memberObjects = members.map(Member.fromRow)
    val mr: MemberResolution = new MemberResolution(memberObjects)
    val (collectionID, resolvedMemberId, isResolved, collapsedMember) = mr.resolveBestMember
    val associatedMembers = memberObjects.map(_.member_key).sorted.mkString(":")
    (collectionID, resolvedMemberId, isResolved, Option(collapsedMember).fold(null: Row)(_.getRow), associatedMembers)
  }

  /**
    * Member resolution UDF
    *
    * @param schema Input schema created by Grouping conditions
    * @return Column expression
    */
  //noinspection ScalaDeprecation
  def resolutionUDF(schema: StructType): Column = {
    callUDF(
      resolutionFunc,
      StructType(
        Seq(
          StructField(CollectionIDColumn, IntegerType),
          StructField(ResolvedMemberIDColumn, LongType),
          StructField(ResolvedFlagColumn, BooleanType),
          StructField(CollapsedMemberColumn, StructType(schema.fields)),
          StructField(AssociatedMembers, StringType)
        )
      ),
      col(GroupedColumn)
    )
  }

  /**
    *
    * Groups and Resolves members in a group based on [[MemberResolution]] rules
    *
    * @param dataFrame [[DataFrame]] with groups identified
    * @return [[DataFrame]] with resolution details
    */
  def resolveGroup(dataFrame: DataFrame, nonMemberColumns: Seq[String] = Nil): DataFrame = {
    import dataFrame.sqlContext.implicits._
    // check if column is array type, is yes explode
    val columnTypeMap = dataFrame.dtypes.toMap
    val groupingColumnMap = groupingColumns
      .map { name =>
        val fieldType = columnTypeMap(name)
        if (fieldType.toLowerCase.contains("array"))
          s"${name}_exploded" -> s"explode($name)"
        else if (fieldType.toLowerCase.contains("string"))
          s"${name}_to_be_grouped" -> s"UPPER($name)"
        else
          s"${name}_to_be_grouped" -> s"$name"
      }

    val newGroupingColumns = groupingColumnMap.map(_._1)
    logger.debug("Grouping Columns: {}", newGroupingColumns.mkString(", "))
    /* Update columns based on grouping info
    1.
    */
    dataFrame
      .applyExpressions(groupingColumnMap.toMap)
      .groupByAsList(newGroupingColumns, dataFrame.columns)
      .withColumn(ResolvedColumn, resolutionUDF(dataFrame.dropColumns(nonMemberColumns).schema))
      .withColumn(GroupedColumn, explode($"$GroupedColumn"))
      .withColumn(GroupIDColumn, concat_ws("~", newGroupingColumns.map(col): _*))
      .dropColumns(newGroupingColumns)
      .unstruct(GroupedColumn, dataFrame.schema)
      .withColumn(SimilarGroupRnk, rank() over Window.partitionBy("member_key", s"$ResolvedColumn.$AssociatedMembers").orderBy(GroupIDColumn))
      .filter(s"$SimilarGroupRnk = 1")
      .withColumn("member_cnt", count("member_key") over Window.partitionBy("member_key"))
      .withColumn("multiple_mbr_grp_cnt", max("member_cnt") over Window.partitionBy(GroupIDColumn))
      .withColumn("is_invalid", when(col("multiple_mbr_grp_cnt") > 1, lit(true)).otherwise(lit(false)))
      .dropColumns(Seq(SimilarGroupRnk, "member_cnt", "multiple_mbr_grp_cnt"))
      .withColumn(GroupingCriteriaIDColumn, lit(groupingCriteriaId))
  }
}