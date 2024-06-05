package com.express.cdw.test.utils

import org.apache.spark.sql.types.{DecimalType, _}

/**
  * Hive Schema parser
  *
  * @author mbadgujar
  */
object SchemaParser {

  private val ColumnExtractionRegex = "(\\(.*\\))".r
  private val DecimalTypeRegex = """decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)""".r
  private val DecimalTypeRegexReplaced = """decimal(\d+)\s*#\s*(\d+)\s*""".r


  /**
    * Get the spark [[DataType]] from the hive column info
    *
    * @param columnInfo Column name and data type
    * @return [[StructField]]
    */
  private def hiveToSparkType(columnInfo: (String, String)): StructField = {
    val (columnName, hiveType) = columnInfo
    val sparkType = hiveType match {
      case "bigint" => LongType
      case "int" => IntegerType
      case "float" => FloatType
      case "double" => DoubleType
      case DecimalTypeRegexReplaced(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case "boolean" => BooleanType
      case "string" => StringType
      case "timestamp" => TimestampType
      case "date" => DateType
      case _ => throw new Exception(s"$hiveType not supported yet.")
    }
    StructField(columnName, sparkType)
  }

  /**
    * Get Spark Schema from hive create query
    *
    * @param hiveCreateQuery Hive create ddl
    * @return [[StructType]]
    */
  def getSparkSchema(hiveCreateQuery: String): StructType = {
    val hql = DecimalTypeRegex.replaceAllIn(hiveCreateQuery.trim.toLowerCase, "decimal$1#$2").replaceAll("(\\r\\n)|(\\n)", "")
    val matches = ColumnExtractionRegex.findAllMatchIn(hql)
    val columnInfo = matches.mkString.split("partitioned.*by")
      .map(_.trim.replaceAll("(^\\()|(\\)$)", ""))
      .flatMap(_.split(","))
      .map(_.trim.split("\\s+"))
      .map(arr => (arr(0), arr(1)))
      .map(hiveToSparkType).toSeq
    StructType(columnInfo)
  }
}
