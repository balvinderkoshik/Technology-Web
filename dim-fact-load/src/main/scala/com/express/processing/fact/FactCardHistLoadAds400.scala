package com.express.processing.fact

import java.text.SimpleDateFormat
import java.util.Calendar
import com.express.cdw.spark.DataFrameUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp,current_date, lit, when}
/**
  * Created by akshay.rochwani on 5/5/2017.
  */
object FactCardHistLoadAds400 extends  FactLoad {

  override def factTableName = "fact_card_history_ads_400"

  override def surrogateKeyColumn = "card_history_id"

  override def backUpFactTableDF: DataFrame = hiveContext.table(s"$goldDB.fact_card_history")

  override def transform: DataFrame = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val dateInstance = format.format(Calendar.getInstance().getTime)

    //Loading dim_date table
    val dimDateDataset = hiveContext.sql(s"select sdate,nvl(date_key,-1) as date_key from $goldDB.dim_date where status='current'")

    val factTransformationDataset = hiveContext.sql(s"select * from $workDB.work_ads_400_cm")
      .selectExpr(
        "member_key",
        "source_key",
        "match_key",
        "account_number",
        "open_date",
        "application_store_number",
        "credit_term_number",
        "transferred_from_account_number",
        "division_number",
        "last_4_digits_of_account_number",
        "logo",
        "indicator",
        "promotability_indicator",
        "closed_date")
      .withColumnRenamed("account_number","tokenized_cc_nbr")
      .withColumnRenamed("credit_term_number","credit_term_nbr")
      .withColumnRenamed("application_store_number","application_store_key")
      .withColumnRenamed("transferred_from_account_number","previous_account_nbr")
      .withColumnRenamed("last_4_digits_of_account_number","last_4_of_account_nbr")
      .withColumnRenamed("match_key","match_type_key")
      .withColumnRenamed("division_number","division_nbr")
      .withColumn("email_change_date",lit(dateInstance))
      .withColumn("batch_id",lit(batchId))
      .withColumn("last_updated_date",current_timestamp)
      .withColumn("card_type_key", lit(1))
      .withColumn("loyalty_id", lit(null))
      .withColumn("is_primary_account_holder",lit("YES"))
      .withColumn("rid",lit(null))
      .withColumn("last_purchase_date",lit(null))
      .withColumn("current_otb",lit(null))
      .withColumn("ytd_purchase_amt",lit(null))
      .withColumn("behavior_score",lit(null))
      .withColumn("cycle_nbr",lit(null))
      .withColumn("mgn_id",lit(null))
      .withColumn("credit_limit",lit(null))
      .withColumn("balance_all_plans",lit(null))
      .withColumn("application_source_code",lit(null))
      .withColumn("recourse_ind",lit(null))
      .withColumn("nbr_of_cards_issued",lit(null))
      .withColumn("activation_flag",lit(null))
      .withColumn("amt_of_last_payment",lit(null))
      .withColumn("amt_of_last_purchase",lit(null))
      .withColumn("cycle_update_date",lit(null))
      .withColumn("card_issue_date",lit(null))
      .withColumn("e_statement_ind",lit(null))
      .withColumn("e_statement_change_date",lit(null))
      .withColumn("last_return_date",lit(null))
      .withColumn("new_account_clerk_nbr",lit(null))
      .withColumn("old_open_close_ind",lit(null))
      .withColumn("do_not_statement_insert",lit(null))
      .withColumn("do_not_sell_name",lit(null))
      .withColumn("return_mail_ind",lit(null))
      .withColumn("ip_code",lit(null))
      .withColumn("vc_key",lit(null))
      .withColumn("lw_link_key",lit(null))
      .withColumn("status_code",lit(null))
      .withColumn("issued_date",lit(null))
      .withColumn("expiration_date",lit(null))
      .withColumn("registration_date",lit(null))
      .withColumn("unregistration_date",lit(null))
      .withColumn("first_card_txn_date",lit(null))
      .withColumn("last_card_txn_date",lit(null))
      .withColumn("lw_is_primary",lit(null))
      .withColumn("closed_date_key",lit(null))
      .withColumn("cycle_update_date_key",lit(null))
      .withColumn("card_issue_date_key",lit(null))
      .withColumn("e_statement_change_date_key",lit(null))
      .withColumn("last_return_date_key",lit(null))
      .withColumn("expiration_date_key",lit(null))
      .withColumn("registration_date_key",lit(null))
      .withColumn("unregistration_date_key",lit(null))
      .withColumn("first_card_txn_date_key",lit(null))
      .withColumn("last_card_txn_date_key",lit(null))
      .withColumn("last_purchase_date_key",lit(null))

    logger.debug("Input record count for ADS 400 fact load: {}", factTransformationDataset.count.toString)

    // load the prerequisite Data Set
    val factTransformationDatasetDF = factTransformationDataset
      .withColumn("open_close_ind", when((factTransformationDataset.col("closed_date").isNull or factTransformationDataset.col("closed_date")>lit(current_date())),lit("O")).otherwise(lit("C")))
      .withColumn("marketable_ind", when(factTransformationDataset.col("promotability_indicator") === "Y", lit("Y")).otherwise(lit("N")))
      .withColumn("spam_indicator",lit(null))
      .withColumn("issued_date_key",lit(null))
      .withColumn("account_open_date",factTransformationDataset.col("open_date"))
      .drop("promotability_indicator")
      .drop("open_date")

    val dateColumns = Seq("account_open_date")

    factTransformationDatasetDF.updateKeys(dateColumns, dimDateDataset, "sdate", "date_key")

  }

  def main (args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }
}