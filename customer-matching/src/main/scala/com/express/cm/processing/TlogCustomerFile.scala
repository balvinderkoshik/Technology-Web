package com.express.cm.processing

import com.express.cdw._
import com.express.cdw.spark.udfs._
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.MatchColumnAliases._
import com.express.cm.criteria.RecordInfo.assignRecordInfoKey
import com.express.cm.criteria._
import com.express.cm.lookup.LookUpTableUtil
import com.express.cm.lookup.LookUpTable.MemberDimensionLookup
import com.express.cm.spark.DataFrameUtils._
import com.express.cm.{AddressFormat, NameFormat, UDF}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SaveMode}
import org.apache.spark.sql.DataFrame
/**
  * Tlog and Customer file Customer Matching Process file
  *
  * @author mbadgujar
  */
object TlogCustomerFile extends CustomerMatchProcess with LazyLogging {

  // Source Work Tables
  private val TlogAdminWorkTable = s"$workDB.work_tlog_admin_dedup"
  private val TlogDiscountWorkTable = s"$workDB.work_tlog_discount_dedup"
  private val TlogSaleWorkTable = s"$workDB.work_tlog_sale_dedup"
  private val TLogTenderWorkTable = s"$workDB.work_tlog_tender_dedup"
  private val TLogTransactionWorkTable = s"$workDB.work_tlog_transaction_dedup"
  private val TlogCustomerWorkTable = s"$workDB.work_customer_dedup"
  private val FactTransactionDetailGoldTable = s"$goldDB.fact_transaction_detail"

  // Tlog Customer Match output result table
  private val TlogCustomerCMResultTable = s"$workDB.work_tlog_customer_cm"
  private val TlogCustomerCMResultArchiveTable=s"$workDB.work_tlog_customer_cm_archive"

  // Constants
  private val TransactionID = "trxn_id"
  private val StoreID = "store_id"
  private val RetailWebFlag = "retail_web_flag"

  private val todayDate = java.time.LocalDate.now

  private val sortTrimCols = Seq(CardAlias,CardTypeAlias,ImpliedLoyaltyIdAlias,"loyalty_type",FirstNameAlias,LastNameAlias,AddressLineOneAlias,AddressLineTwoAlias,PhoneAlias,EmailAddressAlias)


  // Source key UDF based on Retail flag (used for Tlog,Customer file)
  private val sourceKeyTlogUDF = udf((retailWeb: String, matchKey: Int) => {
    val webFlag = "WEB"
    import com.express.cdw.MatchTypeKeys._
    val customerFieldsMatchStatuses = List(LoyaltyId, PhoneEmail, NameAddress, PhoneEmailName, EmailName, Email, PhoneName)
    if (customerFieldsMatchStatuses.contains(matchKey))
      if (retailWeb == webFlag) 47 else 7
    else if (retailWeb == webFlag) 5 else 6
  })

  // Source to CM alias mapping
  private val CMAliasMap = Map("phone_number" -> PhoneAlias, "loyalty" -> ImpliedLoyaltyIdAlias,
    "tender_number" -> CardAlias, "tender_type" -> CardTypeAlias, "name_prefix" -> NamePrefixAlias,
    "first_name" -> FirstNameAlias, "middle_initial" -> MiddleInitialAlias, "last_name" -> LastNameAlias,
    "name_suffix" -> NameSuffixAlias, "address_line_1" -> AddressLineOneAlias, "address_line_2" -> AddressLineTwoAlias,
    "city" -> CityAlias, "state" -> StateNameAlias, "zip_code" -> ZipCodeAlias, "non_usa_postal_code" -> NonUSAPostalCodeAlias,
    "country" -> CountryAlias, "email_address" -> EmailAddressAlias)

  // for padding columns
  def transactionColumnPadding(transactionData: DataFrame): DataFrame={
    transactionData.withColumn("concat_col",concat(
      lpad(col("record_type"),1," "),
      lpad(col("transaction_type"),1," "),
      lpad(col("division_id"),4,"0"),
      lpad(col("store_id"),5,"0"),
      lpad(col("register_id"),3,"0"),
      lpad(col("cashier_id_header"),3,"0"),
      lpad(col("salesperson"),10,"0"),
      lpad(col("file_date"),6," "),
      lpad(col("transaction_time"),6,"0"),
      lpad(col("transaction_number"),5,"0"),
      lpad(col("transaction_modifier"),1,"0"),
      lpad(col("post_void_flag"),1,"0"),
      lpad(col("transaction_sequence_number"),4,"0"),
      lpad(col("original_transaction_number"),5,"0"),
      lpad(col("associate_sales_flag"),1,"0"),
      lpad(col("promotion_flag"),1,"0"),
      lpad(col("non_merchandise_type"),2," "),
      lpad(col("non_merchandise_number"),18," "),
      lpad(col("non_merchandise_tax"),1," "),
      lpad(col("non_merchandise_quantity"),2,"0"),
      lpad(col("non_merchandise_amount_sign"),1," "),
      lpad(col("non_merchandise_amount"),6,"0"),
      lpad(col("filler_1"),34," "),
      lpad(col("non_merchandise_cashier_number"),9,"0"),
      lpad(col("non_merchandise_salesperson_number"),9,"0"),
      lpad(col("filler_2"),1," "),
      lpad(col("transaction_date_iso"),8,"0"),
      lpad(col("class_number"),4,"0"),
      lpad(col("vendor_number"),5,"0"),
      lpad(col("style_number"),8,"0"),
      lpad(col("color_number"),4,"0"),
      lpad(col("size_number"),4,"0"),
      lpad(col("item_number"),8,"0"),
      lpad(col("filler_footer"),1," ")
    )).select("concat_col")

  }

  def saleColumnPadding(saleData: DataFrame): DataFrame={
    saleData.withColumn("concat_col",concat(
      lpad(col("record_type"),1," "),
      lpad(col("transaction_type"),1," "),
      lpad(col("division_id"),4,"0"),
      lpad(col("store_id"),5,"0"),
      lpad(col("register_id"),3,"0"),
      lpad(col("cashier_id_header"),3,"0"),
      lpad(col("salesperson"),10,"0"),
      lpad(col("file_date"),6," "),
      lpad(col("transaction_time"),6,"0"),
      lpad(col("transaction_number"),5,"0"),
      lpad(col("transaction_modifier"),1,"0"),
      lpad(col("post_void_flag"),1,"0"),
      lpad(col("transaction_sequence_number"),4,"0"),
      lpad(col("original_transaction_number"),5,"0"),
      lpad(col("associate_sales_flag"),1,"0"),
      lpad(col("promotion_flag"),1,"0"),
      lpad(col("sku"),20," "),
      lpad(col("sku_type"),1," "),
      lpad(col("retail_quantity"),2,"0"),
      lpad(col("retail_sales_discount_amount_sign"),1," "),
      lpad(col("retail_sales_discount_amount"),6,"0"),
      lpad(col("retail_amount_sign"),1," "),
      lpad(col("retail_amount"),6,"0"),
      lpad(col("tax_status"),1," "),
      lpad(col("price_modifier_indicator"),1," "),
      lpad(col("original_price_sign"),1," "),
      lpad(col("original_price"),6,"0"),
      lpad(col("offline_flag"),1," "),
      lpad(col("sku_scanned_indicator"),1," "),
      lpad(col("plu_indicator"),1," "),
      lpad(col("plu_feature_id"),1," "),
      lpad(col("in_cecpct"),1," "),
      lpad(col("filler_1"),9," "),
      lpad(col("pos_indicator"),1," "),
      lpad(col("filler_2"),3," "),
      lpad(col("cashier_id_data"),9," "),
      lpad(col("salesperson_id"),9," "),
      lpad(col("transaction_void_indicator"),1," "),
      lpad(col("transaction_date_iso"),8,"0"),
      lpad(col("class_number"),4,"0"),
      lpad(col("vendor_number"),5,"0"),
      lpad(col("style_number"),8,"0"),
      lpad(col("color_number"),4,"0"),
      lpad(col("size_number"),4,"0"),
      lpad(col("item_number"),8,"0"),
      lpad(col("filler_footer"),1," ")
    )).select("concat_col")


  }

  def discountColumnPadding(discountData: DataFrame): DataFrame= {
   discountData.withColumn("concat_col", concat(
      lpad(col("record_type"), 1," "),
      lpad(col("transaction_type"), 1," "),
      lpad(col("division_id"), 4,"0"),
      lpad(col("store_id"), 5,"0"),
      lpad(col("register_id"), 3,"0"),
      lpad(col("cashier_id_header"), 3,"0"),
      lpad(col("salesperson"), 10,"0"),
      lpad(col("file_date"), 6," "),
      lpad(col("transaction_time"), 6,"0"),
      lpad(col("transaction_number"), 5,"0"),
      lpad(col("transaction_modifier"), 1,"0"),
      lpad(col("post_void_flag"), 1,"0"),
      lpad(col("transaction_sequence_number"), 4,"0"),
      lpad(col("original_transaction_number"), 5,"0"),
      lpad(col("associate_sales_flag"), 1,"0"),
      lpad(col("promotion_flag"), 1,"0"),
      lpad(col("reference_number"), 8,"0"),
      lpad(col("discount_amount_sign"), 1," "),
      lpad(col("discount_amount"), 6,"0"),
      lpad(col("discount_type"), 2,"0"),
      lpad(col("discount_percentage"), 3,"0"),
      lpad(col("coupon_type"), 2,"0"),
      lpad(col("ring_code"), 8,"0"),
      lpad(col("discount_type_1_discount_deal_code"), 2,"0"),
      lpad(col("mobile_pos_indicator"), 1," "),
      lpad(col("filler_1"), 50," "),
      lpad(col("transaction_date_iso"), 8,"0"),
      lpad(col("class_number"), 4,"0"),
      lpad(col("vendor_number"), 5,"0"),
      lpad(col("style_number"), 8,"0"),
      lpad(col("color_number"), 4,"0"),
      lpad(col("size_number"), 4,"0"),
      lpad(col("item_number"), 8,"0"),
      lpad(col("filler_footer"), 1," ")
    )).select("concat_col")

  }

  def tenderColumnPadding(tenderData: DataFrame): DataFrame={
   tenderData.withColumn("concat_col", concat(
      lpad(col("record_type"),1," "),
      lpad(col("transaction_type"),1," "),
      lpad(col("division_id"),4,"0"),
      lpad(col("store_id"),5,"0"),
      lpad(col("register_id"),3,"0"),
      lpad(col("cashier_id_header"),3,"0"),
      lpad(col("salesperson"),10,"0"),
      lpad(col("file_date"),6," "),
      lpad(col("transaction_time"),6,"0"),
      lpad(col("transaction_number"),5,"0"),
      lpad(col("transaction_modifier"),1,"0"),
      lpad(col("post_void_flag"),1,"0"),
      lpad(col("transaction_sequence_number"),4,"0"),
      lpad(col("original_transaction_number"),5,"0"),
      lpad(col("associate_sales_flag"),1,"0"),
      lpad(col("promotion_flag"),1,"0"),
      lpad(col("tender_type"),2," "),
      lpad(col("re_issue_flag"),1," "),
      lpad(col("tender_amount_sign"),1," "),
      lpad(col("tender_amount"),6,"0"),
      lpad(col("tender_number"),18," "),
      lpad(col("change_due_sign"),1," "),
      lpad(col("change_due"),6,"0"),
      lpad(col("check_authorization_enable"),1," "),
      lpad(col("check_swiped"),1," "),
      lpad(col("check_authorization_type"),1," "),
      lpad(col("check_authorization_number"),6,"0"),
      lpad(col("filler_1"),39," "),
      lpad(col("transaction_date_iso"),8,"0"),
      lpad(col("class_number"),4,"0"),
      lpad(col("vendor_number"),5,"0"),
      lpad(col("style_number"),8,"0"),
      lpad(col("color_number"),4,"0"),
      lpad(col("size_number"),4,"0"),
      lpad(col("item_number"),8,"0"),
      lpad(col("filler_footer"),1," ")
    )).select("concat_col")

  }

  def adminColumnPadding(adminData: DataFrame): DataFrame= {
    adminData.withColumn("concat_col", concat(
      lpad(col("record_type"),1," "),
      lpad(col("transaction_type"),1," "),
      lpad(col("division_id"),4,"0"),
      lpad(col("store_id"),5,"0"),
      lpad(col("register_id"),3,"0"),
      lpad(col("cashier_id_header"),3,"0"),
      lpad(col("salesperson"),10,"0"),
      lpad(col("file_date"),6," "),
      lpad(col("transaction_time"),6,"0"),
      lpad(col("transaction_number"),5,"0"),
      lpad(col("transaction_modifier"),1,"0"),
      lpad(col("post_void_flag"),1,"0"),
      lpad(col("transaction_sequence_number"),4,"0"),
      lpad(col("original_transaction_number"),5,"0"),
      lpad(col("associate_sales_flag"),1,"0"),
      lpad(col("promotion_flag"),1,"0"),
      lpad(col("filler_1"),6," "),
      lpad(col("employee_id"),7,"0"),
      lpad(col("associate_type"),1," "),
      lpad(col("associate_discount_percentage"),2,"0"),
      lpad(col("filler_2"),67," "),
      lpad(col("transaction_date_iso"),8,"0"),
      lpad(col("class_number"),4,"0"),
      lpad(col("vendor_number"),5,"0"),
      lpad(col("style_number"),8,"0"),
      lpad(col("color_number"),4,"0"),
      lpad(col("size_number"),4,"0"),
      lpad(col("item_number"),8,"0"),
      lpad(col("filler_footer"),1," ")
    )).select("concat_col")

  }


  def customerColumnPadding(custData: DataFrame): DataFrame={
    custData.withColumn("concat_col", concat_ws("",
      lpad(col("record_type"),1," "),
      lpad(col("address_indicator"),1," "),
      lpad(col("intr1"),10,"0"),
      lpad(col("intr2"),5,"0"),
      lpad(col("transaction_date"),8," "),
      lpad(col("transaction_store"),10,"0"),
      lpad(col("transaction_register"),10,"0"),
      lpad(col("transaction_nbr"),10,"0"),
      lpad(col("intr3"),1," "),
      lpad(col("intr4"),1," "),
      lpad(col("phone_number"),10," "),
      lpad(col("zip_code"),9," "),
      lpad(col("email_address"),40," "),
      lpad(col("gender"),1," "),
      lpad(col("first_name"),15," "),
      lpad(col("middle_initial"),1," "),
      lpad(col("last_name"),20," "),
      lpad(col("ean"),16," "),
      lpad(col("ean_type"),5,"0"),
      lpad(col("loyalty"),21," "),
      lpad(col("loyalty_type"),5,"0"),
      lpad(col("intr9"),19," "),
      lpad(col("intr10"),5,"0"),
      lpad(col("intr11"),19," "),
      lpad(col("intr12"),5,"0"),
      lpad(col("do_not_email"),1," "),
      lpad(col("do_not_rent"),1," "),
      lpad(col("do_not_mail"),1," "),
      lpad(col("do_not_telemarket"),1," "),
      lpad(col("do_not_map"),1," "),
      lpad(col("do_not_promote"),1," "),
      lpad(col("birth_date"),8," "),
      lpad(col("customer_number"),21," "),
      lpad(col("intr20"),10,"0"),
      lpad(col("intr21"),50," "),
      lpad(col("intr22"),64," "),
      lpad(col("name_prefix"),3," "),
      lpad(col("name_suffix"),3," "),
      lpad(col("company_name"),30," "),
      lpad(col("apartment_number"),10," "),
      lpad(col("address_line_1"),32," "),
      lpad(col("address_line_2"),32," "),
      lpad(col("city"),25," "),
      lpad(col("country"),3," "),
      lpad(col("state"),2," "),
      lpad(col("non_usa_postal_code"),10," "),
      lpad(col("state_name"),25," "),
      lpad(col("redeemed_reward_cert_number_1"),13," "),
      lpad(col("redeemed_reward_cert_number_2"),13," "),
      lpad(col("redeemed_reward_cert_number_3"),13," "),
      lpad(col("redeemed_reward_cert_number_4"),13," "),
      lpad(col("redeemed_reward_cert_number_5"),13," ")
    )).select("concat_col")


  }

  // find Kill list for customer data
  def customerKillList(custData: DataFrame,tlogData : DataFrame) : DataFrame ={
    val joindDF = custData.join(tlogData,col(TransactionID)===col(TransactionID+"_tlog"),"left")
      .filter(isnull(col(TransactionID+"_tlog"))).drop(TransactionID+"_tlog")
    customerColumnPadding(joindDF)
  }

  def tlogKillList(tlogCustomerWork: DataFrame, tlogNonMerchandiseSet : DataFrame,tlogDiscountSet : DataFrame,tlogAdminWorkTableSet : DataFrame,
                   tlogTenderWorkSet : DataFrame,tlogSaleWorkSet : DataFrame) : DataFrame ={

    val joindTransactionDF = tlogNonMerchandiseSet.join(tlogCustomerWork,col(TransactionID)===col(TransactionID+"_cust"),"left")
                  .filter(isnull(col(TransactionID+"_cust"))).drop(TransactionID+"_cust").drop(TransactionID)

    val joindDiscountDF = tlogDiscountSet.join(tlogCustomerWork,col(TransactionID)===col(TransactionID+"_cust"),"left")
      .filter(isnull(col(TransactionID+"_cust"))).drop(TransactionID+"_cust").drop(TransactionID)

    val joindAdminDF = tlogAdminWorkTableSet.join(tlogCustomerWork,col(TransactionID)===col(TransactionID+"_cust"),"left")
      .filter(isnull(col(TransactionID+"_cust"))).drop(TransactionID+"_cust").drop(TransactionID)

    val joindTenderDF = tlogTenderWorkSet.join(tlogCustomerWork,col(TransactionID)===col(TransactionID+"_cust"),"left")
      .filter(isnull(col(TransactionID+"_cust"))).drop(TransactionID+"_cust").drop(TransactionID)

    val joindSaleDF = tlogSaleWorkSet.join(tlogCustomerWork,col(TransactionID)===col(TransactionID+"_cust"),"left")
      .filter(isnull(col(TransactionID+"_cust"))).drop(TransactionID+"_cust").drop(TransactionID)

    val transactionKillList = transactionColumnPadding(joindTransactionDF)
    val discountKillList = discountColumnPadding(joindDiscountDF)
    val adminKillList = adminColumnPadding(joindAdminDF)
    val tenderKillList = tenderColumnPadding(joindTenderDF)
    val saleKillList = saleColumnPadding(joindSaleDF)

    transactionKillList.unionAll(discountKillList).unionAll(adminKillList).unionAll(tenderKillList).unionAll(saleKillList)

  }

  /**
    * Get the Source key for Input file
    *
    * @return [[Column]]
    */
  override def getSourceKey: Column = sourceKeyTlogUDF(col(RetailWebFlag), col(MatchTypeKeyColumn))

  /**
    * Tlog Customer file Customer Matching Process Entry point
    *
    * @param args arguments
    */
  def main(args: Array[String]): Unit = {

    logger.info("Starting Customer Matching process for Tlog & Customer Data")
    val hiveContext = initCMContext
    import hiveContext.implicits._

    val tlogTrxnIDUDF = GetTrxnIDUDF($"transaction_date_iso", $"store_id", $"register_id", $"transaction_number")

    // Work data for Kill list
    val tlogNonMerchandiseSet = hiveContext.table(TLogTransactionWorkTable).withColumn(TransactionID, tlogTrxnIDUDF)
    val tlogDiscountSet = hiveContext.table(TlogDiscountWorkTable).withColumn(TransactionID, tlogTrxnIDUDF)
    val tlogAdminWorkTableSet = hiveContext.table(TlogAdminWorkTable).withColumn(TransactionID, tlogTrxnIDUDF)
    val tlogTenderWorkSet = hiveContext.table(TLogTenderWorkTable).withColumn(TransactionID, tlogTrxnIDUDF)
    val tlogSaleWorkSet = hiveContext.table(TlogSaleWorkTable).withColumn(TransactionID, tlogTrxnIDUDF)



    // Work table for Customer Matching Process
    val tlogSaleWork = hiveContext.table(TlogSaleWorkTable)
      .withColumn(TransactionID, tlogTrxnIDUDF)
      .select($"$TransactionID", $"transaction_date_iso", $"store_id", $"register_id", $"transaction_number")

    val tlogNonMerchandise = hiveContext.table(TLogTransactionWorkTable)
      .withColumn(TransactionID, tlogTrxnIDUDF)
      .select($"$TransactionID", $"transaction_date_iso", $"store_id", $"register_id", $"transaction_number")

    val tlogDiscount = hiveContext.table(TlogDiscountWorkTable)
      .withColumn(TransactionID, tlogTrxnIDUDF)
      .select($"$TransactionID", $"transaction_date_iso", $"store_id", $"register_id", $"transaction_number")

    val tlogAdminWorkTable = hiveContext.table(TlogAdminWorkTable)
      .withColumn(TransactionID, tlogTrxnIDUDF).select(TransactionID, "employee_id")

    val tlogTenderWork = hiveContext.table(TLogTenderWorkTable).withColumn(TransactionID, tlogTrxnIDUDF)
      .select(TransactionID, "transaction_date_iso", "store_id", "register_id", "transaction_number", "tender_number", "tender_type")

    val tlogCustomerWork = hiveContext.table(TlogCustomerWorkTable)
      .withColumn(TransactionID, GetTrxnIDUDF($"transaction_date", $"transaction_store", $"transaction_register", $"transaction_nbr"))
     //.dropColumns(Seq("record_type", "transaction_date", "transaction_store", "transaction_register", "transaction_nbr"))

    val factTransactionDetail = hiveContext.table(FactTransactionDetailGoldTable).select(TransactionID).withColumnRenamed(TransactionID,"trxn_id_fact")

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
      tlogTenderWork.select($"$TransactionID", $"transaction_date_iso", $"store_id", $"register_id", $"transaction_number")
        .join(tlogAdminWorkTable, Seq(TransactionID), "left")
        .join(tlogTenderWork.select(TransactionID, "tender_number", "tender_type"), Seq(TransactionID), "left")
    }


    //Join All five Tlog Tables with Customer based on the generated trxn_id
    val joinedTlogCustomerData = transactions.distinct
      .join(tlogCustomerWork.dropColumns(Seq("record_type", "transaction_date", "transaction_store", "transaction_register", "transaction_nbr")), Seq(TransactionID))
      .renameColumns(CMAliasMap)


    // Call exception list function
    val custKillListData = customerKillList(tlogCustomerWork
      ,transactions.select(TransactionID).withColumnRenamed(TransactionID,TransactionID+"_tlog"))

    val tlogKillListData = tlogKillList(tlogCustomerWork.select(TransactionID).withColumnRenamed(TransactionID,TransactionID+"_cust"),
      tlogNonMerchandiseSet,tlogDiscountSet,tlogAdminWorkTableSet,tlogTenderWorkSet,tlogSaleWorkSet)

    // write exception list in HDFS
    if (custKillListData.count > 0)
    custKillListData.write.mode(SaveMode.Overwrite).text("/apps/cdw/outgoing/exception_list/customerExceptionList/ExceptionList-" + todayDate)

    if (tlogKillListData.count > 0)
    tlogKillListData.write.mode(SaveMode.Overwrite).text("/apps/cdw/outgoing/exception_list/tlogExceptionList/ExceptionList-" +  todayDate)



    val uniqJoinedTlogCustomerData = joinedTlogCustomerData.join(factTransactionDetail,col(TransactionID)===col("trxn_id_fact"),"left")
      .filter("trxn_id_fact is null").drop("trxn_id_fact")

    // Initialize required UDFs
    val nameFormatUDF = UDF(udf(NameFormat), Seq(NamePrefixAlias, FirstNameAlias, MiddleInitialAlias, LastNameAlias, NameSuffixAlias))

    val addressFormatUDF = UDF(udf(AddressFormat(_: String, _: String, _: String, _: String, _: String, "", "", _: String
      , _: String)), Seq(AddressLineOneAlias, AddressLineTwoAlias, CityAlias, StateNameAlias, ZipCodeAlias,
      NonUSAPostalCodeAlias, CountryAlias))


    /*
       The unmatched records from Loyalty match function containing loyalty ids should not go down further for other matching functions.
       They should create new customers directly
     */
    val (Some(loyaltyMatched), loyaltyUnmatched) = uniqJoinedTlogCustomerData.applyCustomerMatch(Seq(LoyaltyIDMatch))
    val (loyaltyUnmatchedWithIds, loyaltyUnmatchedWithoutIds) = loyaltyUnmatched.persist.partition(CheckNotEmptyUDF($"$ImpliedLoyaltyIdAlias"))

    logger.info("Unmatched records counts with loyalty IDs: {}", loyaltyUnmatchedWithIds.count.toString)
    logger.info("Unmatched records counts without loyalty IDs: {}", loyaltyUnmatchedWithoutIds.count.toString)

    // Create function list
    val customerMatchFunctionList: Seq[MatchTrait] = Seq(new PhoneEmailImpliedLoyaltyMatch(nameFormatUDF),
      new PLCCNameImpliedLoyaltyMatch(nameFormatUDF), PLCCMatch, new PLCCNameMatch(nameFormatUDF),
      EmployeeIDMatch, new BankCardNameMatch(nameFormatUDF), new BankCardMatch(nameFormatUDF),
      new NameAddressMatch(nameFormatUDF, addressFormatUDF), new PhoneEmailNameMatch(nameFormatUDF),
      new PhoneEmailMatch(nameFormatUDF), new EmailNameMatch(nameFormatUDF), new EmailMatch,
      new PhoneNameMatch(nameFormatUDF), new PhoneMatch)

    // Apply the match functions to employee data
    val (Some(matched), unmatched) = loyaltyUnmatchedWithoutIds.applyCustomerMatch(customerMatchFunctionList)

    // Create new customers for matched records
    val unmatchedUnionLoyaltyUnmatch = unmatched.unionAll(loyaltyUnmatchedWithIds)
    val sortUnmatched = sortTrimDF(unmatchedUnionLoyaltyUnmatch,sortTrimCols)


      //unmatchedUnionLoyaltyUnmatch
      //.sort(PhoneAlias,ImpliedLoyaltyIdAlias,CardAlias,CardTypeAlias,NamePrefixAlias,FirstNameAlias,MiddleInitialAlias,LastNameAlias,NameSuffixAlias,AddressLineOneAlias,AddressLineTwoAlias,CityAlias,StateNameAlias,ZipCodeAlias,NonUSAPostalCodeAlias,CountryAlias,EmailAddressAlias)

    val unmatchedWithNewMemberKeys = CreateNewCustomerRecords.create(sortUnmatched)
    val tlogCustomerMatchedRecords = loyaltyMatched.unionAll(matched)
      .unionAll(unmatchedWithNewMemberKeys.select(matched.getColumns: _*))

    // Assign record info key
    val updatedWithRecordInfo = assignRecordInfoKey(tlogCustomerMatchedRecords)

    // Get store dimension dataframe
    val dimStoreMasterDF = hiveContext.table(s"$goldDB.dim_store_master").where("status='current'")
      .select(StoreID, RetailWebFlag)

    // Calculate source key
    val updatedWithSourceKey = updatedWithRecordInfo
      .renameColumns(CMAliasMap, reverse = true)
      .join(dimStoreMasterDF, Seq("store_id"))
      .withColumn(SourceKeyColumn, getSourceKey)

    // Get the output table reference
    val tlogCustomerCMTable = hiveContext.table(TlogCustomerCMResultTable)

    val updatedWithMemberLoyalty = updatedWithSourceKey
      .join(LookUpTableUtil.getLookupTable(MemberDimensionLookup)
        .selectExpr(MemberKeyColumn, "loyalty_id as member_loyalty"), Seq(MemberKeyColumn), "left")
      .select(tlogCustomerCMTable.getColumns: _*).persist()

    // Write the Customer match results to output file
    updatedWithMemberLoyalty.insertIntoHive(SaveMode.Overwrite, transactions, TlogCustomerCMResultTable)

    logger.info("Customer Matching process for Tlog & Customer Data completed successfully")



    // Get the output table reference
    val tlogCustomerCMTable2 = hiveContext.table(TlogCustomerCMResultTable)
    val tlogCustomerCMArchiveTable = hiveContext.table(TlogCustomerCMResultArchiveTable)

    // Write the Customer match results to archive output file

    tlogCustomerCMTable2.select(tlogCustomerCMArchiveTable.getColumns: _*).write.partitionBy("batch_id").mode(SaveMode.Overwrite).insertInto(TlogCustomerCMResultArchiveTable)

    logger.info("Customer Matching process for Tlog & Customer Archive Data completed successfully")


  }

}
