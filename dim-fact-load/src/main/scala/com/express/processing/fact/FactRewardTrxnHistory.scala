package com.express.processing.fact

import com.express.cdw.MatchColumnAliases._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.spark.udfs._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, lit, _}
import org.apache.spark.sql.types.LongType

/**
  * Created by poonam.mishra on 8/12/2017.
  */

object FactRewardTrxnHistory extends FactLoad {

  private val TransactionID = "trxn_id"
  // Source Work Tables
  private val TlogAdminWorkTable = s"$workDB.work_tlog_admin_dedup"
  private val TlogDiscountWorkTable = s"$workDB.work_tlog_discount_dedup"
  private val TlogSaleWorkTable = s"$workDB.work_tlog_sale_dedup"
  private val TLogTenderWorkTable = s"$workDB.work_tlog_tender_dedup"
  private val TLogTransactionWorkTable = s"$workDB.work_tlog_transaction_dedup"
  private val TlogCustomerWorkTable = s"$workDB.work_customer_dedup"

  // Source to CM alias mapping
  private val AliasMap = Map("phone_number" -> PhoneAlias, "loyalty" -> ImpliedLoyaltyIdAlias,
    "tender_number" -> CardAlias, "tender_type" -> CardTypeAlias, "name_prefix" -> NamePrefixAlias,
    "first_name" -> FirstNameAlias, "middle_initial" -> MiddleInitialAlias, "last_name" -> LastNameAlias,
    "name_suffix" -> NameSuffixAlias, "address_line_1" -> AddressLineOneAlias, "address_line_2" -> AddressLineTwoAlias,
    "city" -> CityAlias, "state" -> StateNameAlias, "zip_code" -> ZipCodeAlias, "non_usa_postal_code" -> NonUSAPostalCodeAlias,
    "country" -> CountryAlias, "email_address" -> EmailAddressAlias)

  override def factTableName = "fact_reward_trxn_history"

  override def surrogateKeyColumn = "fact_reward_trxn_history_id"

  override def transform: DataFrame = {

    import hiveContext.implicits._

    val tlogTrxnIDUDF = GetTrxnIDUDF($"transaction_date_iso", $"store_id", $"register_id", $"transaction_number")
    val tlogSaleWork = hiveContext.table(TlogSaleWorkTable)
      .withColumn(TransactionID, tlogTrxnIDUDF)
      .select($"$TransactionID", $"transaction_date_iso", $"store_id", $"register_id", $"transaction_number", $"cashier_id_header")

    val tlogNonMerchandise = hiveContext.table(TLogTransactionWorkTable)
      .withColumn(TransactionID, tlogTrxnIDUDF)
      .select($"$TransactionID", $"transaction_date_iso", $"store_id", $"register_id", $"transaction_number", $"cashier_id_header")

    val tlogDiscount = hiveContext.table(TlogDiscountWorkTable)
      .withColumn(TransactionID, tlogTrxnIDUDF)
      .select($"$TransactionID", $"transaction_date_iso", $"store_id", $"register_id", $"transaction_number", $"cashier_id_header")

    val tlogAdminWorkTable = hiveContext.table(TlogAdminWorkTable)
      .withColumn(TransactionID, tlogTrxnIDUDF).select(TransactionID, "employee_id")

    val tlogTenderWork = hiveContext.table(TLogTenderWorkTable).withColumn(TransactionID, tlogTrxnIDUDF)
      .select(TransactionID, "transaction_date_iso", "store_id", "register_id", "transaction_number", "tender_number", "tender_type", "cashier_id_header")

    val tlogCustomerWork = hiveContext.table(TlogCustomerWorkTable)
      .withColumn(TransactionID, GetTrxnIDUDF($"transaction_date", $"transaction_store", $"transaction_register", $"transaction_nbr"))
      .dropColumns(Seq("record_type", "transaction_date", "transaction_store", "transaction_register", "transaction_nbr"))

    // Union the merchandise, non-merchandise and discount transactions
    val transactions = {
      tlogSaleWork
        .join(tlogAdminWorkTable, Seq(TransactionID), "left")
        .join(tlogTenderWork.select(TransactionID, "tender_number", "tender_type"), Seq(TransactionID), "left")
    }.unionAll {
      tlogNonMerchandise
        .join(tlogAdminWorkTable, Seq(TransactionID), "left")
        .join(tlogTenderWork.select(TransactionID, "tender_number", "tender_type"), Seq(TransactionID), "left")
    }.unionAll {
      tlogDiscount
        .join(tlogAdminWorkTable, Seq(TransactionID), "left")
        .join(tlogTenderWork.select(TransactionID, "tender_number", "tender_type"), Seq(TransactionID), "left")
    }.unionAll {
      tlogTenderWork.select($"$TransactionID", $"transaction_date_iso", $"store_id", $"register_id", $"transaction_number", $"cashier_id_header")
        .join(tlogAdminWorkTable, Seq(TransactionID), "left")
        .join(tlogTenderWork.select(TransactionID, "tender_number", "tender_type"), Seq(TransactionID), "left")
    }

    //Join All five Tlog Tables with Customer based on the generated trxn_id
    val joinedTlogCustomerData = transactions.distinct
      .join(tlogCustomerWork, Seq(TransactionID))
      .renameColumns(AliasMap)

    /**.withColumn("certificate_nbr", when(not(isnull(col("redeemed_reward_cert_number_1"))), col("redeemed_reward_cert_number_1")))
      .withColumn("certificate_nbr", when(not(isnull(col("redeemed_reward_cert_number_2"))), col("redeemed_reward_cert_number_2")))
      .withColumn("certificate_nbr", when(not(isnull(col("redeemed_reward_cert_number_3"))), col("redeemed_reward_cert_number_3")))
      .withColumn("certificate_nbr", when(not(isnull(col("redeemed_reward_cert_number_4"))), col("redeemed_reward_cert_number_4")))
      .withColumn("certificate_nbr", when(not(isnull(col("redeemed_reward_cert_number_5"))), col("redeemed_reward_cert_number_5")))
      * */

    val joinedTlogCustomerDataCrtNbr= joinedTlogCustomerData
      .withColumn("certificate_nbr_array",array(col("redeemed_reward_cert_number_1"),col("redeemed_reward_cert_number_2"),col("redeemed_reward_cert_number_3"),col("redeemed_reward_cert_number_4"),col("redeemed_reward_cert_number_5")))
      .withColumnRenamed("transaction_date_iso", "trxn_date")
      .withColumnRenamed("register_id", "trxn_register")
      .withColumnRenamed("transaction_number", "trxn_nbr")
      .withColumnRenamed("cashier_id_header", "cashier_id")
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_updated_date", current_timestamp)
      .withColumn("trxn_id",col(TransactionID))

    joinedTlogCustomerDataCrtNbr
      .withColumn("certificate_nbr", explode(col("certificate_nbr_array"))).withColumnRenamed("transaction_date_iso", "trxn_date")
      .filter("certificate_nbr is not null and trim(certificate_nbr)<>''")

  }


  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }
}