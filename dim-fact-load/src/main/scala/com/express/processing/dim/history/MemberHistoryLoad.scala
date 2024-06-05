package com.express.processing.dim.history

/**
  * Created by Gaurav.Maheshwari on 5/17/2017.
  */
import com.express.cdw.Settings
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}



class MemberHistoryLoad (cmOutputDF: DataFrame) extends LazyLogging {

  // TODO: Logging
  import MemberHistoryLoad._

  // val memberDimDF = cmOutputDF.sqlContext.table("gold.dim_member").filter("status='current'")

  /**
    *
    * @return
    */
  def insertMemberRecords: Unit = {

    val sqlContext = cmOutputDF.sqlContext

    val sourceCols = cmOutputDF.columns
    logger.info("retrieving member columns")
    //  val memberColumns = memberDimDF.columns


    // logger.info("Member target  table column are  ::----------------------  '" + memberDimDF.columns.mkString(","))

    logger.info("Work source table columns are :: ---------------------------"+ cmOutputDF.columns.mkString(",") )


    // Insert the new member records to table
    val dimensionUpdates = dimInsert(cmOutputDF)


    logger.info("Prinitng dimensionUpdates.currentPartitionRecords.columns --------------------------------"+dimensionUpdates.currentPartitionRecords.columns.mkString(","))


    logger.info("creating temp table for current and history")
    dimensionUpdates.currentPartitionRecords.registerTempTable("cm_member_insert_records_current")

    dimensionUpdates.historyPartitionRecords.registerTempTable("cm_member_insert_records_history")

    logger.info("inserting current data into " + Settings.getWorkDB + ".work_dim_member_seq ")
    sqlContext.sql("INSERT OVERWRITE  TABLE  " + Settings.getWorkDB + "." + "work_dim_member_seq  select * from cm_member_insert_records_current")

    logger.info("inserted into " + Settings.getWorkDB + ".work_dim_member_seq current table successfully")



  }


  /**
    * Insert process
    *
    * @return [[MemberLoad]]
    */
  private def dimInsert (newMemberKeysDF: DataFrame): MemberLoad = {
    logger.info("executing dimInsert() Function")
    // Get the max surrogate key



    /**
      * Dropping the member_id, last_updated_date and status column as the same will be adding in the code again.
      */
    val newMemberWithoutSKDF = newMemberKeysDF.drop("sequence_number")
    val rankedRows = newMemberWithoutSKDF.rdd
      .zipWithIndex()
      .map { case (row, id) => Row.fromSeq((id +  1) +: row.toSeq) }
    val rankedRowsSchema = StructType(StructField(MemberSeqKey, LongType, nullable = false) +: newMemberWithoutSKDF.schema.fields)
    val finalInsertRecords = newMemberKeysDF.sqlContext.createDataFrame(rankedRows, rankedRowsSchema)


    logger.info("printing finalInsertRecords schema after adding seq_key  : -----------------------------------------"+ finalInsertRecords.columns.mkString(","))


    logger.info("inserting the member records into the History and current table ")
    // TODO: re-partitioning logic
    MemberLoad(
      finalInsertRecords
        .withColumn(StatusColumn, lit(CurrentStatus))

      ,
      finalInsertRecords.withColumn(StatusColumn, lit(HistoryStatus))


    )
  }


}


object MemberHistoryLoad extends LazyLogging {

  case class MemberLoad (currentPartitionRecords: DataFrame, historyPartitionRecords: DataFrame)

  // Dimension table specific columns
  val LastUpdateColumn = "last_updated_date"
  val StatusColumn = "status"
  val CurrentStatus = "current"
  val HistoryStatus = "history"
  val MemberSeqKey = "sequence_number"

  def main (args: Array[String]): Unit = {

    val source_db = args(0)
    val source_table = args(1)

    val sparkConf = Settings.sparkConf
    val sparkContext = new SparkContext(sparkConf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)

    logger.info("Reading " +   source_db + "." + source_table  +"data from Work layer")


    val memberWork = hiveContext.sql("select * from " + source_db + "." + source_table).persist()

    /**
      * creating member mapping for each file
      */
    val targetDim = new MemberHistoryLoad(memberWork)
    targetDim.insertMemberRecords

  }
}
