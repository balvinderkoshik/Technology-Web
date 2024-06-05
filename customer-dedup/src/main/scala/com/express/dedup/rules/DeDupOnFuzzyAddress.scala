package com.express.dedup.rules

import com.express.cdw.spark.DataFrameUtils._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}

/**
  * Created by aman.jain on 6/28/2017.
  */
object DeDupOnFuzzyAddress extends FuzzyDeDupResolutionTrait {


  override def groupingCriteriaId: Int = 2

  override def groupingIdentificationRule: Column = null

  override def groupingColumns = Seq("zip_code_scrubbed", "first_name", "last_name", "identifier")

  //Schema for Address columns
  override val fuzzyColStruct = StructType(
    Seq(
      StructField("member_key", LongType),
      StructField("address1_scrubbed", StringType),
      StructField("address2_scrubbed", StringType),
      StructField("city_scrubbed", StringType),
      StructField("state_scrubbed", StringType),
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

  def fuzzyResolveOnAddress (dataFrame: DataFrame) : (DataFrame,DataFrame) = {

    val (beforeFuzzyNameMatchGrouped, beforeFuzzyNameMatchUnGrouped) = dataFrame
      .partition(expr("not_empty(state_scrubbed) and state_scrubbed<>'UNKNOWN' and not_empty(city_scrubbed) and city_scrubbed<>'UNKNOWN'"))

    val exactMatchCols = Seq("zip_code_scrubbed", "first_name", "last_name")
    val exactMatchColsMap = exactMatchCols.map { name => s"${name}_to_be_grouped" -> s"UPPER($name)" }


    val (fuzzyMatchAddressGrouped, fuzzyAddressMatchUnGrouped) = beforeFuzzyNameMatchGrouped
      .select("first_name", "last_name", "address1_scrubbed", "address2_scrubbed", "zip_code_scrubbed", "city_scrubbed", "state_scrubbed", "member_key")
        .applyExpressions(exactMatchColsMap.toMap)
      .groupByAsList(exactMatchColsMap.map(_._1), Seq("member_key", "address1_scrubbed", "address2_scrubbed", "city_scrubbed", "state_scrubbed"))
      .withColumn("fuzzyColumn", fuzzyDF)
      .withColumn("fuzzyColumnExploded", explode(col("fuzzyColumn")))
      .unstruct("fuzzyColumnExploded", fuzzyColStruct)
      .select("member_key", "identifier", "result")
      .withColumn("rank", row_number() over Window.partitionBy("member_key").orderBy(desc("result")))
      .filter("rank=1")
      .join(dataFrame, Seq("member_key"))
      .select(dataFrame.getColumns ++ Seq("identifier", "result").map(col) :_*)
      .dropColumns(Seq("resolved", "grouped_count", "grouping_id", "grouping_criteria_id"))
      .partition(col("result"))

    val fuzzyMatchAddressGroupedFinal = fuzzyMatchAddressGrouped.drop("result")
    val nonColumnFamily = Seq("identifier")
    val fuzzyMatchAddress = resolveGroup(fuzzyMatchAddressGroupedFinal, nonColumnFamily)
      .drop("identifier")
    val fuzzyAddressMatchUnGroupedFinal = fuzzyAddressMatchUnGrouped
      .dropColumns(Seq("identifier","result"))
      .unionAll(beforeFuzzyNameMatchUnGrouped
        .dropColumns(Seq("resolved", "grouped_count", "grouping_id", "grouping_criteria_id")))

    (fuzzyMatchAddress, fuzzyAddressMatchUnGroupedFinal)
  }
}
