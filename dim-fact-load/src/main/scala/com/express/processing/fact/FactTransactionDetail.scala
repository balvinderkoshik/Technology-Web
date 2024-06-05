package com.express.processing.fact

import com.express.cdw.MatchTypeKeys
import org.apache.spark.sql.functions.{current_timestamp, lit, udf, _}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable


/**
  * Created by akshay.rochwani on 4/27/2017.
  */
object FactTransactionDetail extends FactLoad {

  /*
    Constants
   */
  val TrxnIdCol = "trxn_id"
  val MemberKeyCol = "member_key"
  val MatchTypeCol = "match_key"


  // Header trailer required for joins on other tables
  val TlogHeaderTrailersColumns = Seq("store_id", "division_id", "transaction_type", "transaction_time", "transaction_sequence_number",
    "transaction_date_iso", "item_number")

  /*
     UDFs
   */
  private val generateTrxnID = udf((trxnDate: String, storeNum: Int, registerNum: Int, trxnNum: Int) => {
    trxnDate.replaceAll("-", "") + f"$storeNum%05d" + f"$registerNum%03d" + f"$trxnNum%05d"
  })

  private val amountUDF = udf((sign: String, amount: Int) => sign match {
    case "+" => amount / 100f
    case "-" => -amount / 100f
  })

  private val unitsConversionUDF = udf((sign: String, quantity: Int) => sign match {
    case "+" => quantity
    case "-" => -quantity
  })


  /*
     Column Transformations
   */
  private val commonTransformations = {
    mutable.LinkedHashMap(
      "trxn_detail_id" -> concat(col("trxn_id"), lpad(col("transaction_sequence_number"), 4, "0")),
      "member_key" -> col(MemberKeyCol),
      "trxn_id" -> col(TrxnIdCol),
      "trxn_nbr" -> col("transaction_number"),
      "register_nbr" -> col("register_id"),
      "trxn_seq_nbr" -> col("transaction_sequence_number"),
      "trxn_date" -> col("transaction_date_iso"),
      "cashier_id" -> col("cashier_id_header"),
      "orig_trxn_nbr" -> col("original_transaction_number"),
      "post_void_ind" -> col("post_void_flag"),
      "salesperson" -> col("salesperson"),
      "match_type_key" -> col("match_key"),
      "record_info_key" -> col("record_info_key"),
      "loyalty_ind" -> when((not(isnull(col("loyalty")))) and (trim(col("loyalty")) !== ""), "Y").otherwise("N"),
      "promotion_code" -> lit(null),
      "markdown_type" -> lit(null),
      "rid" -> lit(null),
      "promotion_key" -> lit(null),
      "discount_ref_nbr" -> lit(null),
      "ean_used" -> lit(null),
      "associate_key" -> lit(-1),
      "associate_nbr" -> lit(null),
      "captured_loyalty_id" -> col("loyalty"),
      "implied_loyalty_id" -> when(col("match_key") === MatchTypeKeys.PhoneEmailLoyalty, col("member_loyalty")).
        when(col("match_key") === MatchTypeKeys.PlccNameLoyalty, col("member_loyalty")).
        when(col("match_key") === MatchTypeKeys.PlccLoyalty, col("member_loyalty")).otherwise(lit(null)),
      "cogs" -> lit(null),
      "margin" -> lit(null)
    )
  }

  private val saleTransformations = {
    mutable.LinkedHashMap(
      "sku" -> col("sku"),
      "purchase_qty" -> col("retail_quantity"),
      "shipped_qty" -> col("retail_quantity"),
      "units" -> unitsConversionUDF(col("retail_sales_discount_amount_sign"),col("retail_quantity")),
      "original_price" -> amountUDF(col("original_price_sign"), col("original_price")),
      "retail_amount" -> amountUDF(col("retail_amount_sign"), col("retail_amount")),
      "total_line_amnt_after_discount" -> amountUDF(col("retail_sales_discount_amount_sign"), col("retail_sales_discount_amount")) * col("retail_quantity"),
      "unit_price_after_discount" -> amountUDF(col("retail_sales_discount_amount_sign"), col("retail_sales_discount_amount")),
      "discount_pct" -> lit(0.0),
      "is_shipping_cost" -> lit("NO"),
      "discount_amount" -> lit(0.0),
      "trxn_void_ind" -> col("transaction_void_indicator"),
      "plu_ind" -> col("plu_indicator"),
      "plu_feature_id" -> col("plu_feature_id"),
      "gift_card_sales_ind" -> lit("N"),
      "lbi_trxn_line_type_cat_code" -> when(col("transaction_modifier") === 0, "SALE").otherwise("RET"),
      "lbi_trxn_line_type_code" -> lit("IA"),
      "price_modified_ind" -> when(col("price_modifier_indicator") === 0, "N").otherwise("Y"),
      "mobile_pos_ind" -> when(col("pos_indicator") === 0, "N").otherwise("Y"),
      "associate_trxn_flag" -> when(col("associate_sales_flag") === 0, "N").otherwise("Y"),
      "tax_status_flag" -> when(col("tax_status") === 0, "N").otherwise("Y"),
      "discount_type_code" -> lit(null),
      "ring_code_used" -> lit(null),
      "markdown_type" -> lit(null)
    )
  }


  private val nonMerchandiseTransformations = {
    val amount = amountUDF(col("non_merchandise_amount_sign"), col("non_merchandise_amount"))
    val sign_units = unitsConversionUDF(col("non_merchandise_amount_sign"),col("non_merchandise_quantity"))
    mutable.LinkedHashMap(
      "sku" -> lit(null),
      "purchase_qty" -> when(col("non_merchandise_type") === 25 ,lit(0)).otherwise(col("non_merchandise_quantity")),
      "shipped_qty" -> when(col("non_merchandise_type") === 25 ,lit(0)).otherwise(col("non_merchandise_quantity")),
      "units" -> when(col("non_merchandise_type") === 25 ,lit(0)).otherwise(sign_units),
      "original_price" -> amount,
      "retail_amount" -> amount,
      "total_line_amnt_after_discount" -> amount * col("non_merchandise_quantity"),
      "unit_price_after_discount" -> amount,
      "discount_pct" -> lit(0.0),
      "is_shipping_cost" -> when(col("non_merchandise_type") === 25, lit("YES")).otherwise(lit("NO")),
      "discount_amount" -> lit(0.0),
      "trxn_void_ind" -> lit(null),
      "plu_ind" -> lit(null),
      "plu_feature_id" -> lit(null),
      "gift_card_sales_ind" -> when(col("non_merchandise_type") === 15, "Y").otherwise("N"),
      "lbi_trxn_line_type_cat_code" -> when(col("transaction_modifier") === 0, "SALE").otherwise("RET"),
      "lbi_trxn_line_type_code" -> lit("N/A"),
      "price_modified_ind" -> lit("N/A"),
      "mobile_pos_ind" -> lit("N/A"),
      "associate_trxn_flag" -> lit("N/A"),
      "tax_status_flag" -> when(col("non_merchandise_tax") === 0, "N").otherwise("Y"),
      "discount_type_code" -> lit(null),
      "ring_code_used" -> lit(null),
      "markdown_type" -> lit(null)
    )
  }

  private val discountTrxnTransformations = {
    mutable.LinkedHashMap(
      "sku" -> lit(null),
      "purchase_qty" -> lit(0),
      "shipped_qty" -> lit(0),
      "units" -> lit(0),
      "original_price" -> lit(0),
      "retail_amount" -> lit(0),
      "total_line_amnt_after_discount" -> when(concat(lpad(col("discount_type"), 2, "0"),
        lpad(col("discount_type_1_discount_deal_code"), 2, "0")) === "0610",amountUDF(col("discount_amount_sign"), col("discount_amount"))).otherwise(lit(0)),
      "unit_price_after_discount" -> lit(0),
      "is_shipping_cost" -> lit("NO"),
      "discount_pct" -> col("discount_percentage"),
      "discount_amount" -> amountUDF(col("discount_amount_sign"), col("discount_amount")),
      "trxn_void_ind" -> lit(null),
      "plu_ind" -> lit(null),
      "plu_feature_id" -> lit(null),
      "gift_card_sales_ind" -> lit("N"),
      "lbi_trxn_line_type_cat_code" -> lit("DISC"),
      "lbi_trxn_line_type_code" -> lit("N/A"),
      "price_modified_ind" -> lit("N/A"),
      "mobile_pos_ind" -> when(col("mobile_pos_indicator") === 0, "N").otherwise("Y"),
      "associate_trxn_flag" -> when(col("associate_sales_flag") === 0, "N").otherwise("Y"),
      "tax_status_flag" -> lit("N/A"),
      "discount_type_code" -> concat(lpad(col("discount_type"), 2, "0"),
        lpad(col("discount_type_1_discount_deal_code"), 2, "0")),
      "ring_code_used" -> col("ring_code"),
      "markdown_type" -> when(col("ring_code_used") === 1417, lit("CERT")).when(col("ring_code_used").between(0,1000), lit("POS"))
                        .when(col("ring_code_dim").between(1001,9999) or col("ring_code_used") === 8888,lit("CRM")).otherwise(lit(null))
    )
  }

  /**
    * Function for applying transformations
    *
    * @param transformMap Map of target columns names & source data transformations
    * @param inputDF      Input [[DataFrame]]
    * @return [[DataFrame]]
    */
  private def applyTransformations(transformMap: mutable.LinkedHashMap[String, Column])(inputDF: DataFrame): DataFrame = {
    transformMap.keys.foldLeft(inputDF) {
      (df, column) => df.withColumn(column, transformMap(column))
    }
  }


  /**
    * Transform the input for Fact load
    *
    * @param trxnWithMembers Unique transaction with member_key assinged
    * @return Transformed [[DataFrame]]
    */
  private def transformTlogCMOutput(trxnWithMembers: DataFrame): DataFrame = {

    // select single member key for each trxn_id TODO: tie breaker.
    val sqlContext = trxnWithMembers.sqlContext

    import sqlContext.implicits._

    val trxnIdUDF = generateTrxnID($"transaction_date_iso", $"store_id", $"register_id", $"transaction_number")

    val tlogTenderData = sqlContext.table(s"$workDB.work_tlog_tender_dedup").withColumn(TrxnIdCol, trxnIdUDF)
      .groupBy(TrxnIdCol).agg(collect_list("tender_type").as("tender_type"))

    val saleTransactions = sqlContext.table(s"$workDB.work_tlog_sale_dedup")
      .withColumn(TrxnIdCol, trxnIdUDF)
      .join(trxnWithMembers, Seq(TrxnIdCol))
      .join(tlogTenderData, Seq(TrxnIdCol), "left")

    logger.debug("Sale Transaction counts :- {}", saleTransactions.count.toString)

    val nonMerchandiseTransactions = sqlContext.table(s"$workDB.work_tlog_transaction_dedup")
      .withColumn(TrxnIdCol, trxnIdUDF)
      .join(trxnWithMembers, Seq(TrxnIdCol))
      .join(tlogTenderData, Seq(TrxnIdCol), "left")

    logger.debug("Non Merchandise Transaction counts :- {}", nonMerchandiseTransactions.count.toString)
    val ringCodeDF = sqlContext.table(s"$goldDB.dim_ring_code").filter("status='current' and ring_code_id not in (0,-1)").selectExpr("ring_code as ring_code_dim").distinct()
    val discountTransactions = sqlContext.table(s"$workDB.work_tlog_discount_dedup")
      .withColumn(TrxnIdCol, trxnIdUDF)
      .join(trxnWithMembers, Seq(TrxnIdCol))
      .join(tlogTenderData, Seq(TrxnIdCol), "left")
      .join(ringCodeDF, lpad($"ring_code_dim", 8, "0") === lpad($"ring_code", 8, "0"), "left")

    logger.debug("Discount Transaction counts :- {}", discountTransactions.count.toString)

    val transformedSaleTransactions = (applyTransformations(commonTransformations)(_))
      .andThen(applyTransformations(saleTransformations))(saleTransactions)

    val transformedNonMerchandiseTransactions = (applyTransformations(commonTransformations)(_))
      .andThen(applyTransformations(nonMerchandiseTransformations))(nonMerchandiseTransactions)

    val transformedDiscountTransactions = (applyTransformations(commonTransformations)(_))
      .andThen(applyTransformations(discountTrxnTransformations))(discountTransactions)

    val columnsToFilter = (commonTransformations.keys ++ saleTransformations.keys).toSeq ++ TlogHeaderTrailersColumns

    transformedSaleTransactions.select(columnsToFilter.head, columnsToFilter.tail: _*)
      .unionAll(transformedNonMerchandiseTransactions.select(columnsToFilter.head, columnsToFilter.tail: _*))
      .unionAll(transformedDiscountTransactions.select(columnsToFilter.head, columnsToFilter.tail: _*))
  }


  override def factTableName = "fact_transaction_detail"

  override def surrogateKeyColumn = "transaction_detail_id"

  override def transform: DataFrame = {
    logger.info("Reading Tlog file Customer matching results")
    val tlogCustomerCMResultTable = hiveContext.table(s"$workDB.work_tlog_customer_cm")

    // TODO : Apply Tie Breaker
    val trxnWithMembers = tlogCustomerCMResultTable.select(TrxnIdCol, MemberKeyCol, "match_key", "record_info_key", "loyalty", "member_loyalty")
      .dropDuplicates(Seq(TrxnIdCol))

    logger.info("Transforming & joining Tlog work dedup with  Tlog Customer matching results")
    val tlogTransformedFactData = transformTlogCMOutput(trxnWithMembers)
    tlogTransformedFactData.persist
    logger.info("Transformation count : {} ", tlogTransformedFactData.count.toString)

    /**
      * Join/lookup tables for obtaining keys
      */
    val dimDateDataset = hiveContext.sql(s"select sdate, nvl(date_key,-1) as date_key from " +
      s"$goldDB.dim_date where status='current'")

    val dimCurrencyDataset = hiveContext.sql(s"select currency_code, nvl(currency_key,-1) as currency_key from " +
      s"$goldDB.dim_currency where status='current'")

    val dimStoreMasterDataset = hiveContext.sql(s"select store_id,nvl(store_key,-1) as store_key, currency_code from " +
      s"$goldDB.dim_store_master where status='current'").join(dimCurrencyDataset, Seq("currency_code"))

    val dimTimeDataset = hiveContext.sql(s"select regexp_replace(time_in_24hr_day,':','') as transaction_time, time_key from " +
      s"$goldDB.dim_time where status='current'")

    val dimProductDataset = hiveContext.sql(s"select item_nbr as item_number ,max(case item_nbr when 0 then 0 else product_key end) as product_key from " +
      s"$goldDB.dim_product where status='current' group by item_nbr")

    val dimTransactionTypeDataset = hiveContext.sql(s"select transaction_type_key, lbi_trxn_line_type_code, lbi_trxn_line_type_cat_code, price_modified_ind," +
      s"mobile_pos_ind, associate_trxn_flag, tax_status_flag, loyalty_ind from $goldDB.dim_transaction_type where status='current'")

    val dimDiscountType = hiveContext.sql(s"select discount_type_key, concat(discount_type_code, discount_deal_code) as discount_type_code" +
      s" from $goldDB.dim_discount_type where status='current'")


    /*
      Applying all the join logic
     */
    val joinedTransformationWithProduct = tlogTransformedFactData.join(dimProductDataset, Seq("item_number"), "left")

    val joinedTransformationWithStoreMaster = joinedTransformationWithProduct.join(dimStoreMasterDataset, Seq("store_id"), "left")

    val joinedTransformationWithDate = joinedTransformationWithStoreMaster.join(dimDateDataset,
      joinedTransformationWithStoreMaster.col("transaction_date_iso") === dimDateDataset.col("sdate"), "left")
      .withColumnRenamed("date_key", "trxn_date_key")

    val joinedTransformationWithTime = joinedTransformationWithDate.join(dimTimeDataset, Seq("transaction_time"), "left")
      .withColumnRenamed("time_key", "trxn_time_key")

    val joinedTransactionTypeKey = joinedTransformationWithTime.join(dimTransactionTypeDataset,
      Seq("lbi_trxn_line_type_code", "lbi_trxn_line_type_cat_code", "price_modified_ind", "mobile_pos_ind",
        "associate_trxn_flag", "tax_status_flag", "loyalty_ind"), "left")

    val joinedDiscountTypeKey = joinedTransactionTypeKey.join(dimDiscountType, Seq("discount_type_code"), "left")

    joinedDiscountTypeKey.na.fill(-1, joinedDiscountTypeKey.columns.filter(_.endsWith("key")))
      .withColumn("last_updated_date", current_timestamp)
      .withColumn("batch_id", lit(batchId))

  }

  /**
    * Transaction Detail Fact load Entry Point
    *
    * @param args program args
    */
  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }
}
