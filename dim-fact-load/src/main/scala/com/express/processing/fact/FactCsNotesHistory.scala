package com.express.processing.fact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions._
/**
  * Created by hemanth.reddy on 7/11/2017.
  * Modified by Mahendranadh.Dasari on 13-07-2017.
  */

object FactCsNotesHistory extends FactLoad {

  override def factTableName = "fact_cs_notes_history"

  override def surrogateKeyColumn = "cs_notes_history_id"

  override def transform: DataFrame = {
    val dimMemberDF = hiveContext.sql(s"select ip_code,member_key from $goldDB.dim_member where status='current' and ip_code is not null")
      .withColumn("rank",row_number().over(Window.partitionBy(col("ip_code")) orderBy(desc("member_key"))))
      .filter("rank=1")
      .drop("rank")

    val factCsNotesHistoryTransformed = hiveContext.table(s"$workDB.work_bp_csnote_dataquality")
      .withColumnRenamed("id","notes_id")
      .withColumnRenamed("note", "notes")
      .withColumnRenamed("createdby","createdby_from_lw")
      .withColumnRenamed("createdate","createdate_from_lw")
      .withColumnRenamed("updatedate","updatedate_from_lw")
      .withColumn("last_updated_date", current_timestamp)
      .join(dimMemberDF,col("memberid")===col("ip_code"),"left")
      .drop("ip_code")
      .withColumn("ip_code",col("memberid"))
      .withColumn("member_key",when(col("member_key").isNull,lit(-1)).otherwise(col("member_key")))

    logger.debug("Input record count for fact Cs Notes History Fact Load:{}", factCsNotesHistoryTransformed.count.toString)
    factCsNotesHistoryTransformed
  }

  def main(args: Array[String]): Unit = {
    load()
  }
}