package com.express.dedup.rules

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._
import com.express.cdw.spark.DataFrameUtils._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

/**
  * Created by aman.jain on 6/28/2017.
  */
object DeDupOnFuzzyName extends FuzzyDeDupResolutionTrait {


  override def groupingCriteriaId: Int = 2

  override def groupingIdentificationRule: Column = null

  override def groupingColumns = Seq("address1_scrubbed", "address2_scrubbed", "zip_code_scrubbed", "city_scrubbed", "state_scrubbed", "identifier")

  override val fuzzyColStruct = StructType(
    Seq(
      StructField("member_key", LongType),
      StructField("first_name", StringType),
      StructField("last_name", StringType),
      StructField("identifier", StringType),
      StructField("score", IntegerType),
      StructField("result", BooleanType)
    )
  )

  //noinspection ScalaDeprecation
  def fuzzyDF: Column = {
    val structType = fuzzyColStruct
    callUDF(
      fuzzyCalculation(_: Seq[Row]),
      ArrayType(structType),
      col("grouped_data")
    )
  }

  def fuzzyResolveOnName(dataFrame: DataFrame): (DataFrame, DataFrame) = {

    val (beforeFuzzyNameMatchGrouped, beforeFuzzyNameMatchUnGrouped) = dataFrame
      .filter("grouped_count = 1")
      .partition(expr("not_empty(state_scrubbed) and state_scrubbed<>'UNKNOWN' and not_empty(city_scrubbed) and city_scrubbed<>'UNKNOWN'"))

    val exactMatchCols = Seq("address1_scrubbed", "address2_scrubbed", "zip_code_scrubbed", "city_scrubbed", "state_scrubbed")
    val exactMatchColsMap = exactMatchCols.map { name => s"${name}_to_be_grouped" -> s"UPPER($name)" }

    val (fuzzyNameMatchGrouped, fuzzyNameMatchUnGrouped) = beforeFuzzyNameMatchGrouped
      .select("first_name", "last_name", "address1_scrubbed", "address2_scrubbed", "zip_code_scrubbed", "city_scrubbed", "state_scrubbed", "member_key")
      .applyExpressions(exactMatchColsMap.toMap)
      .groupByAsList(exactMatchColsMap.map(_._1), Seq("member_key", "first_name", "last_name"))
      .withColumn("fuzzyColumn", fuzzyDF)
      .withColumn("fuzzyColumnExploded", explode(col("fuzzyColumn")))
      .unstruct("fuzzyColumnExploded", fuzzyColStruct)
      .select("member_key", "identifier", "result")
      .withColumn("rank", row_number() over Window.partitionBy("member_key").orderBy(desc("result")))
      .filter("rank=1")
      .join(dataFrame, Seq("member_key"))
      .select(dataFrame.getColumns ++ Seq("identifier", "result").map(col): _*)
      .dropColumns(Seq("resolved", "grouped_count", "grouping_id", "grouping_criteria_id"))
      .partition(col("result"))

    //Exact Match on Address
    val fuzzyNameMatchGroupedFinal = fuzzyNameMatchGrouped.drop("result")
    val nonColumnFamily = Seq("identifier")
    val fuzzyMatchName = resolveGroup(fuzzyNameMatchGroupedFinal, nonColumnFamily).drop("identifier")
    val fuzzyNameMatchUnGroupedFinal = fuzzyNameMatchUnGrouped
      .dropColumns(Seq("result", "identifier"))
      .unionAll(beforeFuzzyNameMatchUnGrouped
        .dropColumns(Seq("resolved", "grouped_count", "grouping_id", "grouping_criteria_id")))

    (fuzzyMatchName, fuzzyNameMatchUnGroupedFinal)
  }
}
