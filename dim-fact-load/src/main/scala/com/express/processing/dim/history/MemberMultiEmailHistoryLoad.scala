package com.express.processing.dim.history

/**
  * Created by Gaurav.Maheshwari on 5/17/2017.
  */
import com.express.cdw.Settings
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}



class MemberMultiEmailHistoryLoad (cmOutputDF: DataFrame) extends LazyLogging {

  // TODO: Logging
  import MemberMultiEmailHistoryLoad._


  /**
    *
    * @return
    */
  def insertMemberRecords: Unit = {

    val sqlContext = cmOutputDF.sqlContext


    // Insert the new member records to table
    val memberMultiEmailLoad = dimInsert(cmOutputDF)


    logger.info("creating temp table for current and history")
    memberMultiEmailLoad.currentPartitionRecords.registerTempTable("cm_member_insert_records_current")


    logger.info("Prinitng dimensionUpdates.currentPartitionRecords.columns --------------------------------"+memberMultiEmailLoad.currentPartitionRecords.columns.mkString(","))
    memberMultiEmailLoad.historyPartitionRecords.registerTempTable("cm_member_insert_records_history")

    logger.info("inserting current data into " + Settings.getWorkDB + ".work_dim_membermultiemail_seq ")
    sqlContext.sql("INSERT OVERWRITE  TABLE  " + Settings.getWorkDB + "." + "work_dim_membermultiemail_seq  select * from cm_member_insert_records_current")
    logger.info("inserted into " + Settings.getWorkDB + ".dim_member current table successfully")

  }


  /**
    * Insert process
    *
    * @return [[MemberMultiEmailLoad]]
    */
  private def dimInsert (newMemberKeysDF: DataFrame): MemberMultiEmailLoad = {
    logger.info("executing dimInsert() Function")
    // Get the max surrogate key

    /**
      * Dropping the member_id, last_updated_date and status column as the same will be adding in the code again.
      */
    val newMemberWithoutSKDF = newMemberKeysDF.drop("sequence_number")
    val rankedRows = newMemberWithoutSKDF.rdd
      .zipWithIndex()
      .map { case (row, id) => Row.fromSeq((id +  1) +: row.toSeq) }
    val rankedRowsSchema = StructType(StructField(MemberMultiEmailSeqKey, LongType, nullable = false) +: newMemberWithoutSKDF.schema.fields)
    val finalInsertRecords = newMemberKeysDF.sqlContext.createDataFrame(rankedRows, rankedRowsSchema)


    logger.info("inserting the member records into the History and current table ")
    // TODO: re-partitioning logic
    MemberMultiEmailLoad(
      finalInsertRecords
        .withColumn(StatusColumn, lit(CurrentStatus))
      ,
      finalInsertRecords.withColumn(StatusColumn, lit(HistoryStatus))

    )
  }


}


object MemberMultiEmailHistoryLoad extends LazyLogging {

  case class MemberMultiEmailLoad (currentPartitionRecords: DataFrame, historyPartitionRecords: DataFrame)

  // Dimension table specific columns
  val LastUpdateColumn = "last_updated_date"
  val StatusColumn = "status"
  val CurrentStatus = "current"
  val HistoryStatus = "history"
  val MemberSeqSurrogateKey = "member_id"
  val MemberMultiEmailSeqKey = "sequence_number"


  def main (args: Array[String]): Unit = {

    val source_db = args(0)
    val source_table = args(1)
    val sparkConf = Settings.sparkConf
    val sparkContext = new SparkContext(sparkConf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)

    logger.info("Reading " +   source_db + "." + source_table  +"data from Work layer")
    val memberWork = hiveContext.sql("select * from " + source_db + "." + source_table)

    /**
      * creating member mapping for each file
      */
    val targetDim = new MemberMultiEmailHistoryLoad(memberWork)
    targetDim.insertMemberRecords

  }
}
