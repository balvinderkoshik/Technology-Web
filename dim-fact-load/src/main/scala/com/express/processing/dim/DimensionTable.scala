package com.express.processing.dim

import com.express.cdw.spark.DataFrameUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.joda.time.format.DateTimeFormat


/**
  * Dimension table class to support Dimension load process
  *
  * @param dimensionDF    [[DataFrame]] Dimension table dataframe
  * @param surrogateKey   [[String]] Surrogate key column name
  * @param naturalKeys    [[List[String]] List of natural key columns for the Dimension
  * @param derivedColumns [[List[String]] List of derived columns for the Dimension
  * @author mbadgujar
  */
class DimensionTable(dimensionDF: DataFrame,
                     surrogateKey: String,
                     naturalKeys: Seq[String],
                     derivedColumns: Seq[String] = Seq()) extends LazyLogging {

  import DimensionTable._
  import dimensionDF.sqlContext.implicits._


  // Columns to skip for MD5 calculation
  private val columnsToSkip = Seq(surrogateKey, StatusColumn, LastUpdateColumn, BatchIDColumn) ++ derivedColumns

  // create key and non-key column list
  private val (keyColumns, nonKeyColumns) = dimensionDF.columns.toSeq
    .filterNot(columnsToSkip.contains(_))
    .map(new Column(_))
    .partition(column => naturalKeys.contains(column.toString))

  // Get maximum batch id from Backup table
  private def getMaxBackUpBatchId = dimensionDF.filter(s"$StatusColumn = '$CurrentStatus'")
    .select(col(BatchIDColumn))
    .distinct()
    .withColumn(BatchIDColumn, unix_timestamp(col(BatchIDColumn), BatchIDColumnFormat))
    .sort(desc(BatchIDColumn))
    .head(1).toList match {
    case Nil => "0"
    case record :: _ => DateTimeFormat.forPattern(BatchIDColumnFormat).print(record.getAs[Long](BatchIDColumn) * 1000)
  }

  // Dataframe with current data from backup table
  private lazy val currentDimensionDF = {
    val filterCondition = s"$StatusColumn = '$CurrentStatus' and $BatchIDColumn = '$getMaxBackUpBatchId'"
    logger.info("Applying filter for Backup table: {}", filterCondition)
    dimensionDF.filter(filterCondition)
  }


  /**
    * Get Column list according to 'Source' or 'Target' table alias
    *
    * @param from               'Source' or 'Target'
    * @param sourceColumnList   Source Columns, required to check the derived colums
    * @param derivedSource      Derived columns source: 'Source' or 'Target' alias
    * @param selectSKFromTarget Get SK column for Target
    * @return [[Column]]s to Select
    */
  private def getColumnsList(from: String,
                             sourceColumnList: Array[String] = Array(),
                             derivedSource: String = SourceAlias,
                             selectSKFromTarget: Boolean = false): Seq[Column] = {
    val columnList = {
      if (selectSKFromTarget)
        Seq(new Column(s"$TargetAlias.$surrogateKey"))
      else Nil
    } ++ (keyColumns ++ nonKeyColumns).map(column => new Column(s"$from.${column.toString}"))

    // For derived columns, if target select as is or select from source
    val derivedColumnList = from match {
      case TargetAlias => derivedColumns.map(columnName => $"$from.$columnName")
      case SourceAlias => derivedColumns.map(columnName =>
        if (sourceColumnList.contains(columnName))
          $"$derivedSource.$columnName"
        else
          lit(null).as(columnName))
    }

    val finalColumnList = columnList ++ derivedColumnList :+ $"$ActionCodeColumn"
    logger.info("Columns selected for alias: {} :- {}", from, finalColumnList.mkString(", "))
    finalColumnList
  }

  /**
    * Generate MD5 values for key & non-key Columns in Source and Dimension table
    *
    * @param sourceData [[DataFrame]] for source
    * @return [[Tuple2]]
    */
  private def generateMD5Columns(sourceData: DataFrame): (DataFrame, DataFrame) = {
    // source md5 calculation
    val sourceWithMD5 = sourceData
      .withColumn(MD5SourceKeyValueColumn, md5(lower(concat_ws(Separator, keyColumns.map(trim): _*))))
      .withColumn(MD5SourceNonKeyValueColumn, md5(lower(concat_ws(Separator, nonKeyColumns: _*))))

    // dimension data md5 calculation
    val currentTargetWithMD5 = currentDimensionDF
      .withColumn(MD5TargetKeyValueColumn, md5(lower(concat_ws(Separator, keyColumns.map(trim): _*))))
      .withColumn(MD5TargetNonKeyValueColumn, md5(lower(concat_ws(Separator, nonKeyColumns: _*))))

    (sourceWithMD5.alias(SourceAlias), currentTargetWithMD5.alias(TargetAlias))
  }

  /**
    * Get Dimension change set based on the MD5 values for key and non-key columns
    *
    * @param sourceData [[DataFrame]] of source data
    * @return [[DimensionChangeSet]]
    */
  private def getDimensionChangeSet(sourceData: DataFrame, incrementalLoad: Boolean = false): DimensionChangeSet = {

    val (sourceWithMD5, currentTargetWithMD5) = generateMD5Columns(sourceData)

    val joinedMD5 = sourceWithMD5.join(currentTargetWithMD5,
      $"$MD5SourceKeyValueColumn" === $"$MD5TargetKeyValueColumn", FullJoinType)
      .persist()

    logger.debug("SCD Source Data count: {}", joinedMD5.filter(not(isnull($"$MD5SourceKeyValueColumn"))).count.toString)
    logger.debug("SCD Dataset count after full join: {}", joinedMD5.count.toString)

    val insertRecords = joinedMD5.filter(isnull($"$MD5TargetKeyValueColumn"))

    val updateRecords = joinedMD5.filter(($"$MD5SourceKeyValueColumn" === $"$MD5TargetKeyValueColumn")
      .and($"$MD5SourceNonKeyValueColumn" !== $"$MD5TargetNonKeyValueColumn"))

    val noChangeOrDeleteRecords = if (incrementalLoad) {
      joinedMD5.filter(($"$MD5SourceKeyValueColumn" === $"$MD5TargetKeyValueColumn")
        .and($"$MD5SourceNonKeyValueColumn" === $"$MD5TargetNonKeyValueColumn")
        .or(isnull($"$MD5SourceKeyValueColumn")))
    }
    else {
      joinedMD5.filter(($"$MD5SourceKeyValueColumn" === $"$MD5TargetKeyValueColumn")
        .and($"$MD5SourceNonKeyValueColumn" === $"$MD5TargetNonKeyValueColumn"))
    }

    logger.debug("Insert data count: {}", insertRecords.count.toString)
    logger.debug("Update data count: {}", updateRecords.count.toString)
    logger.debug("NCD data count: {}", noChangeOrDeleteRecords.count.toString)

    DimensionChangeSet(insertRecords.withColumn(ActionCodeColumn, lit("I")),
      updateRecords.withColumn(ActionCodeColumn, lit("U")),
      noChangeOrDeleteRecords.withColumn(ActionCodeColumn, lit("NCD")))
  }


  /**
    * SCD type 2 process
    *
    * @param sourceData            [[DataFrame]] of source data
    * @param currentBatchID        Batch ID
    * @param incrementalLoad       Incremental Load Flag
    * @param incrementSKForUpdates Flag to Increment surrogate key for updates, defaults to False
    * @return [[DimensionUpdates]]
    */
  def type2Load(sourceData: DataFrame, currentBatchID: String, incrementalLoad: Boolean = true,
                incrementSKForUpdates: Boolean = false): DimensionUpdates = {

    val dimensionChangeSet = getDimensionChangeSet(sourceData, incrementalLoad)

    // Get the max surrogate key
    val dimensionMaxSurrogateKey = dimensionDF.filter(s"$StatusColumn = '$CurrentStatus'").maxKeyValue($"$surrogateKey")
    logger.info("Max surrogate key for column {} : {}", surrogateKey, dimensionMaxSurrogateKey.toString)
    logger.info("Surrogate Key will be incremented for Update Records: {}", incrementSKForUpdates.toString)

    // Insert records
    val insertRecords = dimensionChangeSet.insertRecords.select(getColumnsList(SourceAlias, sourceData.columns): _*)

    // Update records
    val updateRecords = dimensionChangeSet.updateRecords
      .select(getColumnsList(SourceAlias, sourceData.columns, derivedSource = TargetAlias,
        selectSKFromTarget = !incrementSKForUpdates): _*)

    // no change records
    val noChangeRecords = dimensionChangeSet.noChangeOrDeleteRecords
      .select($"$surrogateKey" +: getColumnsList(TargetAlias) :+ $"$LastUpdateColumn": _*)
      .withColumn(BatchIDColumn, lit(currentBatchID))

    /*
      Sequence records according to the current surrogate key in Dimension table,
      If incrementSKForUpdates is true, new surrogate key will be assigned for updates,
      else the target SK will be used for update records
     */
    val finalInsertUpdateRecords = {
      if (incrementSKForUpdates)
        updateRecords.unionAll(insertRecords).generateSequence(dimensionMaxSurrogateKey, Some(surrogateKey))
      else
        updateRecords.unionAll(insertRecords.generateSequence(dimensionMaxSurrogateKey, Some(surrogateKey)))
    }
      .withColumn(LastUpdateColumn, current_timestamp)
      .withColumn(BatchIDColumn, lit(currentBatchID))

    DimensionUpdates(
      noChangeRecords.unionAll(finalInsertUpdateRecords).withColumn(StatusColumn, lit(CurrentStatus)),
      finalInsertUpdateRecords.withColumn(StatusColumn, lit(HistoryStatus))
    )
  }

}

object DimensionTable {

  private case class DimensionChangeSet(insertRecords: DataFrame, updateRecords: DataFrame,
                                        noChangeOrDeleteRecords: DataFrame)

  case class DimensionUpdates(currentPartitionRecords: DataFrame, historyPartitionRecords: DataFrame)

  // Dimension table specific columns
  val LastUpdateColumn = "last_updated_date"
  val StatusColumn = "status"
  val BatchIDColumn = "batch_id"
  val ActionCodeColumn = "action_cd"
  val CurrentStatus = "current"
  val HistoryStatus = "history"


  // Constants
  val MD5SourceKeyValueColumn = "md5_key_value_source"
  val MD5TargetKeyValueColumn = "md5_key_value_target"
  val MD5SourceNonKeyValueColumn = "md5_nonkey_value_source"
  val MD5TargetNonKeyValueColumn = "md5_nonkey_value_target"
  //val BatchIDColumnFormat = "yyyyddMMHHmmss"
  val BatchIDColumnFormat = "yyyyMMddHHmmss"
  val SourceAlias = "source"
  val TargetAlias = "target"
  val FullJoinType = "full"
  val Separator = ":"
}