package com.express.processing.fact

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.express.cdw.spark.DataFrameUtils._
import org.apache.spark.sql.types.{LongType, StructField, StructType}

/**
  * Created by aditi.chauhan on 11/2/2017.
  */
object FactCardHistLoadBPvirtualcard extends FactLoad {

  override def factTableName = "fact_card_history_bp_virtualcard"

  override def surrogateKeyColumn = "card_history_id"

  override def backUpFactTableDF: DataFrame = hiveContext.table(s"$goldDB.fact_card_history")

  override def transform: DataFrame = {

    /*Fetching fact card history dataset*/

    val factCardHistoryDataset = hiveContext.sql(s"select * from $goldDB.fact_card_history")

    /*Fetching work_bp_member_cm table dataset*/

    val bp_virtualcardDF = hiveContext.sql(s"""select * from $workDB.work_bp_virtualcard_dataquality""")

    /*Fetching the complete fact_card_history dataset*/

    val memberDF = hiveContext.sql(s"select member_key,ip_code from $goldDB.dim_member where status='current'")

    /*Identifying the matched and unmatched records from bpMember table and fact card history table over member_key*/

    val join_member_vc =   bp_virtualcardDF.join(memberDF,col("member_id") === col("ip_code"),"left")
      .withColumn("member_key_vc",when(not(isnull(col("member_key"))),col("member_key")).otherwise(lit(-1)))
      .drop(col("member_key"))

    /*Creating insert DF for all the records from work_bp_virtualcard_dataquality table */

    val populated_cols_list = List("card_type_key","member_key","loyalty_id","vc_key","lw_link_key","status_code","ip_code",
      "issued_date","expiration_date","registration_date",
      "lw_is_primary","match_type_key","source_key","is_primary_account_holder",
      "account_open_date","last_purchase_date","closed_date","cycle_update_date",
      "card_issue_date","e_statement_change_date","last_return_date",
      "unregistration_date","first_card_trxn_date","last_card_trxn_date")

    val null_cols_list = factCardHistoryDataset.columns.toList diff populated_cols_list

    val col_valuesDF =  join_member_vc
      .withColumn("card_type_key",lit(39))
      .withColumn("member_key",col("member_key_vc"))
      .withColumn("ip_code",col("member_id"))
      .withColumn("loyalty_id", col("loyalty_id_number"))
      .withColumn("vc_key",col("virtual_card_key"))
      .withColumn("lw_link_key",col("link_key"))
      .withColumn("status_code",col("status"))
      .withColumn("lw_is_primary",col("is_primary"))
      .withColumn("issued_date",when(not(isnull(col("date_issued"))),to_date(col("date_issued"))).otherwise(to_date(lit("1900-01-01"))))
      .withColumn("expiration_date",when(not(isnull(col("expiration_date"))),to_date(col("expiration_date"))).otherwise(to_date(lit("1900-01-01"))))
      .withColumn("registration_date",when(not(isnull(col("date_registered"))),to_date(col("date_registered"))).otherwise(to_date(lit("1900-01-01"))))
      .withColumn("match_type_key",lit(22))
      .withColumn("source_key",lit(300))
      .withColumn("is_primary_account_holder",lit("NO"))
      .withColumn("account_open_date",to_date(lit("1900-01-01")))
      .withColumn("last_purchase_date",to_date(lit("1900-01-01")))
      .withColumn("closed_date",to_date(lit("1900-01-01")))
      .withColumn("cycle_update_date",to_date(lit("1900-01-01")))
      .withColumn("card_issue_date",to_date(lit("1900-01-01")))
      .withColumn("e_statement_change_date",to_date(lit("1900-01-01")))
      .withColumn("last_return_date",to_date(lit("1900-01-01")))
      .withColumn("unregistration_date",to_date(lit("1900-01-01")))
      .withColumn("first_card_trxn_date",to_date(lit("1900-01-01")))
      .withColumn("last_card_trxn_date",to_date(lit("1900-01-01")))


    val InsertDF = null_cols_list.foldLeft(col_valuesDF) {
      case (df, colName) => df.withColumn(s"$colName",lit(null))}
      .withColumn("last_updated_date",current_timestamp)
      .withColumn("batch_id",lit(batchId))
      .select(factCardHistoryDataset.getColumns:_*)
      .drop(col("card_history_id"))
      .drop(col("status"))

    InsertDF
  }

  def main(args: Array[String]): Unit = {

    batchId = args(0)
    load()
  }

}
