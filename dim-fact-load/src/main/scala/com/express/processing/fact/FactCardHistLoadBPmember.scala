package com.express.processing.fact

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.express.cdw.spark.DataFrameUtils._

/**
  * Created by aditi.chauhan on 11/2/2017.
  */
object FactCardHistLoadBPmember extends FactLoad {

  override def factTableName = "fact_card_history_bp_member"

  override def surrogateKeyColumn = "card_history_id"

  override def backUpFactTableDF: DataFrame = hiveContext.table(s"$goldDB.fact_card_history")

  override def transform: DataFrame = {

      import hiveContext.implicits._

    val factCardHistoryTableDF = hiveContext.table(s"$goldDB.fact_card_history")

    /*Fetching work_bp_member_cm table dataset*/

    val bpmemberDataset = hiveContext.sql(
      s"""select member_key as member_key_cdh,ipcode as ip_code_cdh,a_tokennbrassociationrequest,
         membercreatedate,a_enrollmentstorenumber,memberclosedate,match_key from $workDB.work_bp_member_cm
         where a_tokennbrassociationrequest is not null and a_tokennbrassociationrequest != ''""")

    /*Fetching dim_date dataset*/

    val dimDateDataset = hiveContext.sql(s"select sdate, nvl(date_key,-1) as date_key from $goldDB.dim_date where status = 'current'")

    /*Indentifying columns to be populated as null */

    val populated_cols_list = List("member_key","tokenized_cc_nbr","account_open_date","account_open_date_key","application_store_key","closed_date","closed_date_key","ip_code",
      "open_close_ind","match_type_key","source_key")

    val null_cols_list = factCardHistoryTableDF.columns.toList diff populated_cols_list

        /* Creating the InsertDF for all the unmatched records from the work_bp_member_cm table */
    val InsertDF = bpmemberDataset
      .withColumn("member_key",col("member_key_cdh"))
      .withColumn("tokenized_cc_nbr", col("a_tokennbrassociationrequest"))
      .withColumn("account_open_date", col("membercreatedate"))
      .withColumn("application_store_key", col("a_enrollmentstorenumber"))
      .withColumn("closed_date", col("memberclosedate"))
      .withColumn("ip_code", col("ip_code_cdh"))
      .withColumn("open_close_ind", when(isnull(col("memberclosedate")), lit("O")).otherwise(lit("C")))
      .withColumn("match_type_key", col("match_key"))
      .withColumn("source_key", lit(300))

    val DateColumns = List("closed_date", "account_open_date")

    val InsertDF_withDateKeys = DateColumns.foldLeft(InsertDF) { case (inDF, column) =>
      inDF.join(dimDateDataset, inDF.col(column) === dimDateDataset.col("sdate"), "left")
        .select("date_key", inDF.columns: _*)
        .withColumnRenamed("date_key", s"${column}_key")
    }

    val Final_InsertDF = null_cols_list.foldLeft(InsertDF_withDateKeys) {
      case (df, colName) => df.withColumn(s"$colName",lit(null))}
      .withColumn("last_updated_date",current_timestamp)
      .withColumn("batch_id",lit(batchId))
      .select(factCardHistoryTableDF.getColumns:_*)
      .drop(col("card_history_id"))
      .drop(col("status"))

     Final_InsertDF

  }

  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }

}
