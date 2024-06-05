package com.express.processing.dim

import com.express.cdw.{Settings, _}
import com.express.cdw.spark.DataFrameUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Dimension Load trait that is to be extended for all Dimension table loads
  *
  * @author mbadgujar
  */
trait DimensionLoad extends LazyLogging {

  private val sc = new SparkContext(Settings.sparkConf)

  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
  hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

  val goldDB: String = Settings.getGoldDB
  val workDB: String = Settings.getWorkDB
  val backUpDB: String = Settings.getBackupDB


  /**
    * Get Dimension Table
    */
  def dimensionTableName: String

  /**
    * Get Surrogate Key Column
    */
  def surrogateKeyColumn: String

  /**
    * Get Natural Key Columns for Dimension table
    */
  def naturalKeys: Seq[String]

  /**
    * Get Derived Columns for Dimension table
    */
  def derivedColumns: Seq[String]

  /**
    * Get Transformation DF for Dimension
    *
    * @return [[DataFrame]]
    */
  def transform: DataFrame

  /**
    * Perform Deduplication on Transformed records
    *
    * @return Deduplicated [[DataFrame]]
    */
  def performDedup(dataframe: DataFrame): DataFrame = {
    logger.info("Performing dedup on Natural keys: {}", naturalKeys.mkString(", "))
    val dedupColumnMap = naturalKeys.map(key => s"${key}_dedup" -> key).toMap
    val dedupSource = dataframe.applyExpressions(dedupColumnMap.map{case(k,v) => (k, s"lower(trim($v))")})
    if (dataframe.columns.contains(ETLUniqueIDColumn)) {
      logger.info("{} column present, Using file date for Deduplication", ETLUniqueIDColumn)
      val dfWithFileDate = dedupSource.withColumn("file_date", udf(fileDateFunc _).apply(col(ETLUniqueIDColumn)))
      val windowSpec = Window.partitionBy(naturalKeys.map(col): _*)
      val ranked = dfWithFileDate.withColumn("rank", row_number().over(windowSpec.orderBy(desc("file_date"))))
      ranked.filter("rank = 1").dropDuplicates(dedupColumnMap.keys.toSeq).dropColumns(dedupColumnMap.keys.toSeq)
    }
    else {
      logger.warn("{} column not found, records will be deduped randomly", ETLUniqueIDColumn)
      dedupSource.dropDuplicates(dedupColumnMap.keys.toSeq).dropColumns(dedupColumnMap.keys.toSeq)
    }
  }


  /**
    * Get Dimension table [[DataFrame]]
    *
    * @return [[DataFrame]]
    */
  def dimensionTableDF: DataFrame = hiveContext.table(s"$backUpDB.bkp_$dimensionTableName")


  // Write dimension data to respective output tables
  private def writePartitions(partition: DataFrame, partitionType: String, partitionOnProcess: Boolean, batchID: String): Unit = {
    val partitionTable = s"$workDB.${dimensionTableName}_$partitionType"
    val partitionColumns = hiveContext.table(partitionTable).getColumns
    val outputTable = s"$workDB.${dimensionTableName}_$partitionType"

    val data = partition.select(partitionColumns: _*)
    if (partitionOnProcess)
      data.insertIntoHive(SaveMode.Overwrite, transform, outputTable, Some("process"), batchID)
    else
      data.insertIntoHive(SaveMode.Overwrite, transform, outputTable, None, batchID)
  }

  /**
    * Perform Dimension load for Dimensions for which MD5 comparision is to be done for identifying insert update records
    *
    * @param currentBatchId Current Batch ID
    */
  def load(currentBatchId: String): Unit = {

    val dimensionTable = derivedColumns match {
      case Nil => new DimensionTable(dimensionTableDF, surrogateKeyColumn, naturalKeys)
      case _ => new DimensionTable(dimensionTableDF, surrogateKeyColumn, naturalKeys, derivedColumns)
    }

    val dimensionUpdates = dimensionTable.type2Load(performDedup(transform), currentBatchId)
    writePartitions(dimensionUpdates.currentPartitionRecords, "current", partitionOnProcess = false, currentBatchId)
    writePartitions(dimensionUpdates.historyPartitionRecords, "history", partitionOnProcess = false, currentBatchId)
  }


  /**
    * Perform Dimension load for Dimensions for which Insert and Updates are pre-identified. The process from which the dimension load
    * is triggered should be provided.
    *
    * @param currentBatchId Current Batch ID
    * @param process        Process from which dimension load is invoked
    */
  def load(currentBatchId: String, process: String, incrementSKForUpdates: Boolean = true): Unit = {
    val dimensionTable = derivedColumns match {
      case Nil => new DimensionTable(dimensionTableDF, surrogateKeyColumn, naturalKeys)
      case _ => new DimensionTable(dimensionTableDF, surrogateKeyColumn, naturalKeys, derivedColumns)
    }
    logger.info("Type 2 SCD for process:{}", process)
    val transformedDF = transform.filter(s"process = '$process'").transform(performDedup)

    val dimensionUpdate =
      if (process.trim == "dedup" || process.trim == "dim_household")
        dimensionTable.type2Load(transformedDF, currentBatchId, incrementalLoad = false, incrementSKForUpdates)
      else
        dimensionTable.type2Load(transformedDF, currentBatchId, incrementSKForUpdates = incrementSKForUpdates)

    val (currentPartition, historyPartition) =
      (dimensionUpdate.currentPartitionRecords.withColumn("process", lit(process.trim)),
        dimensionUpdate.historyPartitionRecords.withColumn("process", lit(process.trim)))

    writePartitions(currentPartition, "current", partitionOnProcess = true, currentBatchId)
    writePartitions(historyPartition, "history", partitionOnProcess = true, currentBatchId)
  }

  def loader(currentBatchId: String, process: String, incrementSKForUpdates: Boolean = true, derivedNew: Seq[String] = derivedColumns): Unit = {
    logger.info(s"========New loader function is called for the columns:======= $derivedNew")
    val dimensionTable = derivedNew match {
      case Nil => new DimensionTable(dimensionTableDF, surrogateKeyColumn, naturalKeys)
      case _ => new DimensionTable(dimensionTableDF, surrogateKeyColumn, naturalKeys, derivedNew)
    }
    logger.info("Type 2 SCD for process:{}", process)
    val transformedDF = transform.filter(s"process = '$process'").transform(performDedup)

    val dimensionUpdate =
      if (process.trim == "dedup" || process.trim == "dim_household")
        dimensionTable.type2Load(transformedDF, currentBatchId, incrementalLoad = false, incrementSKForUpdates)
      else
        dimensionTable.type2Load(transformedDF, currentBatchId, incrementSKForUpdates = incrementSKForUpdates)

    val (currentPartition, historyPartition) =
      (dimensionUpdate.currentPartitionRecords.withColumn("process", lit(process.trim)),
        dimensionUpdate.historyPartitionRecords.withColumn("process", lit(process.trim)))

    writePartitions(currentPartition, "current", partitionOnProcess = true, currentBatchId)
    writePartitions(historyPartition, "history", partitionOnProcess = true, currentBatchId)
  }

}