package com.express.cdw.spark

import com.datametica.ecat._
import com.express.cdw.Settings
import com.express.cdw.spark.udfs._
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/**
  * Dataframe implicit methods, import to use
  *
  * @author mbadgujar
  */
object DataFrameUtils {

  implicit class DataFrameImplicits(dataframe: DataFrame) extends LazyLogging {

    import dataframe.sqlContext.implicits._

    /**
      * Get Spark [[DataType]] given the string name of Type
      *
      * @param name [[String]] string
      * @return [[DataType]]
      */
    private def nameToType(name: String): DataType = {
      val nonDecimalNameToType = {
        Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
          DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType)
          .map(t => t.getClass.getSimpleName.replace("$", "") -> t).toMap
      }
      val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)""".r
      name match {
        case "decimal" => DecimalType.USER_DEFAULT
        case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
        case other => nonDecimalNameToType(other)
      }
    }

    /**
      * Parses the Spark Logical Plan to check if it is of Hive Metastore Relation type
      *
      * @param json Json String
      * @return [[Tuple2]] of Boolean if it is hive table, Table name
      */
    private def hiveMetaStoreRelationParser(json: String): (Boolean, String) = {
      val mapper = new ObjectMapper
      val jsonElem = mapper.readTree(json).elements().next()
      val clazz = jsonElem.get("class").textValue
      if (clazz.contains("hive.MetastoreRelation"))
        (true, s"${jsonElem.get("databaseName").textValue}.${jsonElem.get("tableName").textValue}")
      else
        (false, null)
    }

    /**
      * Gets the Hive tables involved in transformation for the given Dataframe
      *
      * @param df DataFrame
      * @return [[Seq]] of fully qualified table names
      */
    private def getDFInputTables(df: DataFrame): Seq[String] = {
      // Get the Hive tables involved in transformations for given Dataframe
      val dfPlans = df.queryExecution.logical.map(plan => (plan.isInstanceOf[LeafNode], plan.toJSON))
      dfPlans.filter { case (isLeafNode, _) => isLeafNode }
        .map { case (_, planJson) => hiveMetaStoreRelationParser(planJson) }
        .filter { case (isHiveMetaRelation, _) => isHiveMetaRelation }
        .map { case (_, sourceTable) => sourceTable }
    }

    /**
      * Hive writer utility method
      *
      * @param saveMode    Save mode
      * @param partitionBy Partition column
      * @param outputTable Output table to be written
      */
    private def hiveWriter(saveMode: SaveMode, partitionBy: Option[String] = None, outputTable: String): Unit = {
      val coalescePartitions: Int = Settings.getCoalescePartitions
      val dfWriter = dataframe.coalesce(coalescePartitions).write.mode(saveMode)
      partitionBy match {
        case None => dfWriter.insertInto(outputTable)
        case Some(column) => dfWriter.partitionBy(column).insertInto(outputTable)
      }
    }


    /**
      * Get [[DataFrame]] columns
      *
      * @return [[Array]] of [[ColumnName]]
      */
    def getColumns: Array[ColumnName] = dataframe.columns.map(columnName => $"$columnName")

    /**
      * Get [[DataFrame]] columns excluding the provided columns
      *
      * @param excludeColumns Columns to be excluded
      * @return [[Array]] of [[ColumnName]]
      */
    def getColumns(excludeColumns: List[String]): Array[ColumnName] = dataframe.columns
      .filterNot(excludeColumns.contains(_))
      .map(columnName => $"$columnName")

    /**
      * Get [[DataFrame]] columns with Table alias
      *
      * @param alias Table alias
      * @return [[Array]] of [[ColumnName]]
      */
    def getColumns(alias: String): Array[ColumnName] = dataframe.columns.map(columnName => $"$alias.$columnName")

    /**
      * Get [[DataFrame]] columns excluding the provided columns and with Table alias
      *
      * @param alias          Table alias
      * @param excludeColumns Columns to be excluded
      * @return [[Array]] of [[ColumnName]]
      */
    def getColumns(alias: String, excludeColumns: List[String]): Array[ColumnName] = dataframe.columns
      .filterNot(excludeColumns.contains(_))
      .map(columnName => $"$alias.$columnName")


    /**
      * Check is Dataframe is not empty
      *
      * @return [[Boolean]]
      *
      */
    def isNotEmpty: Boolean = dataframe.head(1).nonEmpty

    /**
      * Check is Dataframe is empty
      *
      * @return [[Boolean]]
      *
      */
    def isEmpty: Boolean = dataframe.head(1).isEmpty


    /**
      * Drop Columns from Dataframe
      *
      * @param columns [[Seq]] of column [[String]]
      * @return [[DataFrame]]
      */
    def dropColumns(columns: Seq[String]): DataFrame = {
      columns.foldLeft(dataframe)((df, column) => df.drop(column))
    }


    /**
      * Rename Columns as provided in the Map's key & value.
      * If reverse is false, key will be renamed to value else values will be renamed to keys
      *
      * @param nameMap [[Map[String, String]] name -> rename string map
      * @param reverse [[Boolean]] consider map values as columns to be renamed
      * @return [[DataFrame]] with columns renamed
      */
    def renameColumns(nameMap: Map[String, String], reverse: Boolean = false): DataFrame = {
      val renameMap = if (reverse) nameMap.map { case (k, v) => (v, k) } else nameMap
      renameMap.keys.foldLeft(dataframe) {
        (df, key) => df.withColumnRenamed(key, renameMap(key))
      }
    }

    /**
      * Applies the expression to provided columns. If the column is existing, it will be replaced with expression output,
      * else a new column will be created
      *
      * @param exprMap [[Map[String, String]] name -> rename string map
      * @return [[DataFrame]] with expressions applied to columns
      */
    def applyExpressions(exprMap: Map[String, String]): DataFrame = {
      exprMap.keys.foldLeft(dataframe) {
        (df, key) => df.withColumn(key, expr(exprMap(key)))
      }
    }

    /**
      * Cast given columns to provided [[DataType]]
      *
      * @param columns  Column [[Seq]]
      * @param dataType Spark [[DataType]]
      * @return [[DataFrame]] with columns casted to given type
      */
    def castColumns(columns: Seq[String], dataType: DataType): DataFrame = {
      columns.foldLeft(dataframe)((df, column) => df.withColumn(column, $"$column".cast(dataType)))
    }

    /**
      * Cast all columns from current [[DataType]] to provided [[DataType]]
      *
      * @param currentDataType Current [[DataType]]
      * @param toDataType      Requred  [[DataType]]
      * @return [[DataFrame]] with columns casted to given type
      */
    def castColumns(currentDataType: DataType, toDataType: DataType): DataFrame = {
      val columnMD = dataframe.dtypes.map { case (columnName, dType) => (columnName, nameToType(dType)) }
      val columnsToCast = columnMD.filter(_._2 == currentDataType).map(_._1)
      columnsToCast.foldLeft(dataframe)((df, column) => df.withColumn(column, $"$column".cast(toDataType)))
    }

    /**
      * Partitions dataframe based on the filter conditions
      * First dataframe in returned tuple will contain matched records to filter condition
      * Second dataframe will consist unmatched records
      *
      * @param filterCondition [[Column]] filter condition
      * @return [[Tuple2]] [Dataframe, Dataframe]
      */
    def partition(filterCondition: Column): (DataFrame, DataFrame) = {
      (dataframe.filter(filterCondition), dataframe.filter(not(filterCondition)))
    }

    /**
      * Get Max Value of a Key column
      *
      * @param keyColumn Key [[Column]]
      * @return Max column value
      */
    def maxKeyValue(keyColumn: Column): Long = {
      dataframe.select(max($"$keyColumn"))
        .collect()
        .headOption match {
        case None => 0
        case Some(row) => row.get(0).asInstanceOf[Long]
      }
    }

    /**
      * Generate Sequence numbers for a column based on the previous Maximum sequence value
      *
      * @param previousSequenceNum Previous maximum sequence number
      * @param seqColumn           Optional column name if new sequence column is to be added.
      *                            If sequence column is present, it should be first column in Row of LongType
      * @return [[DataFrame]] with Sequence generated
      */
    def generateSequence(previousSequenceNum: Long, seqColumn: Option[String] = None): DataFrame = {
      // generate sequence
      val rowWithIndex = dataframe.rdd.zipWithIndex
      // Get schema
      val (rankedRows, schema) = seqColumn match {
        case None =>
          (rowWithIndex.map { case (row, id) => Row.fromSeq((id + previousSequenceNum + 1) +: row.toSeq.tail) }, dataframe.schema)
        case Some(seqColName) =>
          (rowWithIndex.map { case (row, id) => Row.fromSeq((id + previousSequenceNum + 1) +: row.toSeq) },
            StructType(StructField(seqColName, LongType, nullable = false) +: dataframe.schema.fields))
      }
      // Convert back to dataframe
      dataframe.sqlContext.createDataFrame(rankedRows, schema)
    }


    /**
      * Group by on Columns and return dataframe as grouped list of row structure of remaining columns
      * The returned dataframe has two columns 'grouped_data' which has the grouped data
      * and 'grouped_count' which has the element counts in the grouped list
      *
      * @param groupByColumns     Columns to Group on
      * @param toBeGroupedColumns Optional Columns to be grouped in to List of spark struct, else all columns will be considered
      * @return Grouped [[DataFrame]]
      */
    def groupByAsList(groupByColumns: Seq[String], toBeGroupedColumns: Seq[String] = Seq()): DataFrame = {
      val arraySizeUDF = udf((array: Seq[Row]) => array.size)
      val dataframeFieldMap = dataframe.schema.fields.map(field => field.name -> field).toMap

      // Get fields to be grouped
      val toBeGroupedCols = toBeGroupedColumns match {
        case Nil => dataframe.columns.toSeq
        case _ => toBeGroupedColumns
      }

      // Struct of fields to be grouped
      val toBeGroupedColsStruct = struct(toBeGroupedCols.head, toBeGroupedCols.tail: _*).as("to_be_grouped_cols")

      // Instantiate UDAF
      val collectStructUDAF = new CollectStruct(StructType(toBeGroupedCols.map(dataframeFieldMap(_))))

      // Return grouped result
      dataframe.select(groupByColumns.map(col) :+ toBeGroupedColsStruct: _*)
        .groupBy(groupByColumns.head, groupByColumns.tail: _*)
        .agg(collectStructUDAF($"to_be_grouped_cols").as("grouped_data"))
        .withColumn("grouped_count", arraySizeUDF($"grouped_data"))
    }

    /**
      * Unstruct the fields in a given Struct as columns
      *
      * @param unstructCol  Column to unstruct
      * @param structSchema Struct schema
      * @return [[DataFrame]]
      */
    def unstruct(unstructCol: String, structSchema: StructType): DataFrame = {
      structSchema.fields.map(field => (field.name, s"$unstructCol.${field.name}")).foldLeft(dataframe) {
        case (df, column) => df.withColumn(column._1, col(column._2))
      }.drop(unstructCol)
    }


    /**
      * Update/Add Surrogate key columns by joining on the 'Key' dimension table.
      * It is assumed that the key column naming convention is followed. e.g. 'sale_date' column will have key column 'sale_date_key'
      *
      * @param columns     Columns for which key columns are to be generated.
      * @param keyTable    Key Dimension table
      * @param joinColumn  Join column
      * @param valueColumn 'Key' value column
      * @return Updated [[DataFrame]]
      */
    def updateKeys(columns: Seq[String], keyTable: DataFrame, joinColumn: String, valueColumn: String): DataFrame = {
      columns.foldLeft(dataframe) {
        (df, column) =>
          df.join(broadcast(keyTable.select(joinColumn, valueColumn)), col(column) === col(joinColumn), "left")
            .withColumn(s"${column}_key", col(valueColumn))
            .drop(joinColumn).drop(valueColumn)
      }.na.fill(-1, columns.map(_ + "_key"))
    }

    def updateKeys(columns: Map[String,String], keyTable: DataFrame, joinColumn: String, valueColumn: String): DataFrame = {
      updateKeys(columns.keys.toSeq,keyTable,joinColumn,valueColumn)
        .renameColumns(columns.map{case (k, v) => k + "_key" -> v})
    }


    /**
      * Writes the output in Hive table and also logs the Ecat lineage.
      * The input tables involved in lineage are extracted from the dataframe being written
      *
      * @param saveMode    output [[SaveMode]]
      * @param outputTable output table
      * @param partitionBy partition column
      * @param batchID     Batch ID
      */
    def insertIntoHive(saveMode: SaveMode, outputTable: String, partitionBy: Option[String], batchID: String): Unit = {
      val hiveSources = getDFInputTables(dataframe)
      insertIntoHive(saveMode, hiveSources, outputTable, partitionBy, batchID)
    }

    /**
      * Writes the output in Hive table and also logs the Ecat lineage.
      * The input tables involved in lineage are extracted from the dataframe provided.
      * Use if the dataframe being written has lost its lineage information.
      *
      * @param saveMode         output [[SaveMode]]
      * @param transformationDF Dataframe having the lineage info of input tables involved in Transformation
      * @param outputTable      output table
      * @param partitionBy      partition column
      * @param batchID          Batch ID
      */
    def insertIntoHive(saveMode: SaveMode, transformationDF: DataFrame, outputTable: String,
                       partitionBy: Option[String] = None, batchID: String = "UNKNOWN"): Unit = {
      val hiveSources = getDFInputTables(transformationDF)
      insertIntoHive(saveMode, hiveSources, outputTable, partitionBy, batchID)
    }

    /**
      * Writes the output in Hive table and also logs the Ecat lineage.
      *
      * @param saveMode    output [[SaveMode]]
      * @param inputTables List of input tables involved in Transformation
      * @param outputTable output table
      * @param partitionBy partition column
      * @param batchID     Batch ID
      */
    def insertIntoHive(saveMode: SaveMode, inputTables: Seq[String], outputTable: String,
                       partitionBy: Option[String], batchID: String): Unit = {
      hiveWriter(saveMode, partitionBy, outputTable)
      logEcatLineage(dataframe.sqlContext.sparkContext, inputTables, outputTable, batchID)
    }
  }

  def sortTrimDF(df: DataFrame, cols: Seq[String]): DataFrame = {

    val trimmedDF = cols.foldLeft(df) {
      (df, column) =>
        df
          .withColumn(s"${column}_trimmed", when(length(trim(col(column))) > 0 or (col(column).isNotNull),col(column)).otherwise(lit("NA")))
    }
    val trimmedColList = cols.map(name=>s"$name"+"_trimmed")

    trimmedDF
      .sort(trimmedColList.map(col): _*)
      .dropColumns(trimmedColList)
  }

}