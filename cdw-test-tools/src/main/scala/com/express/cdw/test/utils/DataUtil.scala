package com.express.cdw.test.utils

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.col
import com.express.cdw.spark.DataFrameUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging


/**
  * Express CDW Data Utility
  *
  * @author mbadgujar
  */
object DataUtil {

  implicit class SQLContextImplicit(sqlContext: SQLContext) extends LazyLogging {

    /**
      * Get Dataframe from CSV
      *
      * @param csvPath      CSV file path
      * @param schema       [[DataFrame]] schema
      * @param extraOptions Extra parsing options
      * @return [[DataFrame]]
      */
    def getDfFromCSV(csvPath: String, schema: StructType, extraOptions: Map[String, String]): DataFrame = {
      sqlContext.read
        .format("com.databricks.spark.csv")
        .options(extraOptions)
        .schema(schema)
        .load(csvPath)
    }


    /**
      * Get Dataframe from CSV
      *
      * @param csvPath      CSV file path
      * @param table        Express CDW Table to use for schema.
      * @param extraOptions Extra parsing options
      * @return [[DataFrame]]
      */
    def getDfFromCSV(csvPath: String,
                     table: String,
                     extraOptions: Map[String, String]): DataFrame = {
      val schemaIns = this.getClass.getClassLoader.getResourceAsStream(s"schema/${table.trim.toLowerCase}.hql")
      val createTableQuery = scala.io.Source.fromInputStream(schemaIns).mkString
      val sparkSchema = SchemaParser.getSparkSchema(createTableQuery)
      getDfFromCSV(csvPath, sparkSchema, extraOptions)
    }


    /**
      * Get Dataframe from CSV
      *
      * @param csvPath         CSV file path
      * @param table           Express CDW Table to use for schema.
      * @param providedColumns Columns provided in Data, Other columns will be set as null
      * @param extraOptions    Extra parsing options
      * @return [[DataFrame]]
      */
    def getDfFromCSV(csvPath: String,
                     table: String,
                     providedColumns: Seq[String],
                     extraOptions: Map[String, String] =
                     Map("delimiter" -> "\\t", "dateFormat" -> "yyyy-MM-dd", "nullValue" -> "NULL", "header" -> "true")): DataFrame = {
      logger.info("Reading Delimited Test file for Table: {}", table)
      val schemaIns = this.getClass.getClassLoader.getResourceAsStream(s"schema/${table.trim.toLowerCase}.hql")
      val createTableQuery = scala.io.Source.fromInputStream(schemaIns).mkString
      val sparkSchema = SchemaParser.getSparkSchema(createTableQuery)
      val includedFields = providedColumns.flatMap(col => sparkSchema.find(field => field.name == col))
      val excludedFields = sparkSchema.diff(includedFields)
      logger.info("Fields found in file: {}", includedFields.map(_.name).mkString(","))
      logger.info("Fields not found in file: {}", excludedFields.map(_.name).mkString(","))
      getDfFromCSV(csvPath, StructType(includedFields), extraOptions)
        .applyExpressions(excludedFields.map(field => field.name -> "null").toMap)
        .select(sparkSchema.fieldNames.map(col): _*)
    }
  }

}
