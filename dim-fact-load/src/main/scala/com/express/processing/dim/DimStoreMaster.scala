package com.express.processing.dim

import com.express.cdw.spark.DataFrameUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
  * Created by akshay.rochwani on 7/14/2017.
  */
object DimStoreMaster extends DimensionLoad with LazyLogging {

  import hiveContext.implicits._

  private val TlogDiscountWorkTable = s"$workDB.work_tlog_discount_dedup"
  private val TlogSaleWorkTable = s"$workDB.work_tlog_sale_dedup"
  private val TLogTenderWorkTable = s"$workDB.work_tlog_tender_dedup"
  private val TLogTransactionWorkTable = s"$workDB.work_tlog_transaction_dedup"
  private val TlogAdminWorkTable = s"$workDB.work_tlog_admin_dedup"
  private val TlogStoreIdCol = "tlog_store_id"
  private val StoreIdCol = "store_id"


  override def dimensionTableName: String = "dim_store_master"

  override def surrogateKeyColumn: String = "store_key"

  override def naturalKeys: Seq[String] = Seq("store_id")

  override def derivedColumns: Seq[String] = Nil

  /**
    * Get the transformed store dataframe from store reference files
    *
    * @return [[DataFrame]]
    */
  private def getStoreFilesTransformation: DataFrame = {

    val StoreHierWorkTable = s"$workDB.work_storehier_dataquality"
    logger.info("Reading Store Hier Work Table: {}", StoreHierWorkTable)
    val StoreHierDF = hiveContext.table(StoreHierWorkTable)
      .select("site_code", "district_name", "region_name", "zone_name", "district_code", "region_code", "zone_code")

    val StoreMasterWorkTable = s"$workDB.work_storemaster_dataquality"
    logger.info("Reading Store Master Work Table: {}", StoreMasterWorkTable)
    val StoreMasterDF = hiveContext.table(StoreMasterWorkTable)
      .select("shop_number", "district_number", "retail_center_location", "shop_name", "shop_address_line_1",
        "shop_address_line_2", "shop_city", "state_province_code", "shop_zip_code", "country_code", "shop_profile_code",
        "rtl_flag", "number_of_registers", "comp_bld", "comp_rank", "total_annualized_sales_volume", "price_group_code",
        "currency_code", "telephone_number_line_1", "telephone_number_line_2", "phone_alt_1", "phone_alt_2", "fax_line", "latitude", "longitude")

    val StoreOwnerWorkTable = s"$workDB.work_storeowner_dataquality"
    logger.info("Reading Store Owner Work Table: {}", StoreOwnerWorkTable)
    val StoreOwnerDF = hiveContext.table(StoreOwnerWorkTable)
      .select("shop_nbr", "shop_dsgn_cd", "shop_type_cd", "shop_stat_cd", "act_open_date", "orig_plan_open_date",
        "last_relo_date", "last_close_date", "vol_class_cd", "annualized_vol", "open_to_buy", "open_to_buy_comments",
        "last_project_type", "gross_sq_ft", "sell_sq_ft", "linear_frontage", "nbr_of_rooms", "nbr_of_cabinets",
        "last_activity_date", "pln_nbr_coprops", "like_shop_flag", "pln_nbr_ft_non_mgt", "base_week", "labor_sched_enabled", "labor_sched_copy_store")

    val joinSmSDF = StoreMasterDF.join(StoreHierDF,
      StoreMasterDF.col("shop_number") === StoreHierDF.col("site_code"),
      "left"
    )
      .withColumnRenamed("shop_number", "store_id")
      .withColumn("retail_web_flag", when($"district_number" === 97, lit("WEB")).otherwise(lit("RETAIL")))
      .withColumnRenamed("shop_name", "store_name")
      .withColumnRenamed("shop_address_line_1", "address_1")
      .withColumnRenamed("shop_address_line_2", "address_2")
      .withColumnRenamed("shop_city", "city")
      .withColumnRenamed("state_province_code", "state")
      .withColumnRenamed("shop_zip_code", "zip_code")
      .withColumn("profile_code", col("shop_profile_code").cast(IntegerType))
      .withColumn("profile_description", when($"profile_code" === 0, lit("No Profile Defined"))
        .when($"profile_code" === 1, lit("Single Entrance"))
        .when($"profile_code" === 2, lit("Multiple Entrances"))
        .when($"profile_code" === 3, lit("EXPBBW SXS"))
        .when($"profile_code" === 4, lit("EXPSTR SXS"))
        .when($"profile_code" === 5, lit("EXP/STR/BBW SXS"))
        .when($"profile_code" === 6, lit("EXP/LTD SXS"))
        .when($"profile_code" === 7, lit("EXP/LTD/STR SXS"))
        .when($"profile_code" === 8, lit("STR/BBW SXS"))
        .otherwise(lit("UNKNOWN"))
      )
      .withColumnRenamed("total_annualized_sales_volume", "total_annualized_volume")
      .withColumnRenamed("number_of_registers", "register_count")
      .withColumnRenamed("telephone_number_line_1", "phone_line_1")
      .withColumnRenamed("telephone_number_line_2", "phone_line_2")


    joinSmSDF.join(StoreOwnerDF,
      joinSmSDF.col("store_id") === StoreOwnerDF.col("shop_nbr"),
      "left"
    )
      .withColumn("lbi_location_id", lit(null))
      .withColumn("lw_pilot_market_tier", lit(null))
      .withColumn("open_closed_ind", when($"act_open_date" < current_date() &&
        ($"last_close_date".isNull || $"last_close_date" > current_date() ) &&
        $"sell_sq_ft" > 1, lit("OPEN")).otherwise(lit("CLOSED")))
      .withColumnRenamed("shop_dsgn_cd", "store_design_code")
      .withColumn("store_design_description", when($"store_design_code" === 0, lit("Undefined"))
        .when($"store_design_code" === 1, lit("White EXP"))
        .when($"store_design_code" === 2, lit("White EXP"))
        .when($"store_design_code" === 3, lit("Black & White"))
        .when($"store_design_code" === 4, lit("Slatwall"))
        .when($"store_design_code" === 5, lit("French"))
        .when($"store_design_code" === 6, lit("Old"))
        .when($"store_design_code" === 7, lit("Old Italian"))
        .when($"store_design_code" === 8, lit("New Italian"))
        .when($"store_design_code" === 9, lit("Industrial"))
        .when($"store_design_code" === 10, lit("Brown Leather"))
        .when($"store_design_code" === 11, lit("Gen XY"))
        .when($"store_design_code" === 12, lit("Gen X"))
        .when($"store_design_code" === 13, lit("Gen Y"))
        .when($"store_design_code" === 14, lit("Haywood"))
        .when($"store_design_code" === 15, lit("Outlet"))
        .otherwise(lit("UNKNOWN")))
      .withColumnRenamed("shop_type_cd", "store_type_code")
      .withColumn("store_type_description", when($"store_type_code" === "D", lit("dual gender"))
        .when($"store_type_code" === "W", lit("womens"))
        .when($"store_type_code" === "M", lit("mens")).otherwise(lit("UNKNOWN")))
      .withColumn("store_status_code", when($"shop_stat_cd" === "AHD", lit("Ahead"))
        .when($"shop_stat_cd" === "BHD", lit("Behind"))
        .when($"shop_stat_cd" === "CMP", lit("Complete"))
        .when($"shop_stat_cd" === "HLD", lit("On Hold"))
        .when($"shop_stat_cd" === "LGL", lit("Legal Hold"))
        .when($"shop_stat_cd" === "OPN", lit("Open"))
        .when($"shop_stat_cd" === "PLP", lit("Pending LL Pkg"))
        .when($"shop_stat_cd" === "SCH", lit("On Schedule"))
        .when($"shop_stat_cd" === "SPC", lit("Special"))
        .when($"shop_stat_cd" === "TBD", lit("To Be Determined"))
        .otherwise(lit("UNKNOWN")))
      .withColumnRenamed("shop_stat_cd", "store_status")
      .withColumnRenamed("act_open_date", "actual_open_date")
      .withColumnRenamed("orig_plan_open_date", "original_planned_open_date")
      .withColumnRenamed("last_relo_date", "last_relocation_date")
      .withColumnRenamed("last_close_date", "last_closed_date")
      .withColumnRenamed("vol_class_cd", "sales_volume_class_code")
      .withColumn("sales_volume_class", when($"sales_volume_class_code" === "A", lit("$3.00 - $4.99 | 3,000,000"))
        .when($"sales_volume_class_code" === "A+", lit("$5.00 and higher | 5,000,000"))
        .when($"sales_volume_class_code" === "B", lit("'$1.80 - $2.99 | 1,800,000"))
        .when($"sales_volume_class_code" === "C", lit("$0.00 - $1.80 | 0"))
        .when($"sales_volume_class_code" === "U", lit("Undefined | 0")).otherwise(lit("UNKNOWN")))
      .withColumnRenamed("annualized_vol", "annualized_sales_volume")
      .withColumnRenamed("sell_sq_ft", "selling_sq_ft")
      .withColumnRenamed("linear_frontage", "frontage_linear_ft")
      .withColumnRenamed("nbr_of_rooms", "number_of_rooms")
      .withColumnRenamed("nbr_of_cabinets", "number_of_cabinets")
      .withColumnRenamed("pln_nbr_coprops", "plan_number_coprops")
      .withColumnRenamed("like_shop_flag", "like_store_indicator")
      .withColumnRenamed("pln_nbr_ft_non_mgt", "plan_nbr_ft_non_management")
      .withColumnRenamed("labor_sched_enabled", "labor_schedule_enabled")
      .withColumnRenamed("labor_sched_copy_store", "labor_schedule_copy_store")
      .withColumn("is_lw_pilot_store", lit(null))
      .selectExpr("(case when store_id is null or store_id  = '' then NULL else store_id end) as store_id",
        "cast(lbi_location_id as bigint)", "retail_web_flag", "open_closed_ind", "retail_center_location", "store_name",
        "phone_line_1", "phone_line_2", "fax_line", "phone_alt_1", "phone_alt_2", "address_1", "address_2", "city",
        "state", "zip_code", "country_code", "profile_code", "profile_description", "rtl_flag", "register_count",
        "comp_bld", "comp_rank", "store_design_code", "store_design_description", "store_type_code",
        "store_type_description", "store_status_code", "store_status", "actual_open_date", "original_planned_open_date",
        "last_relocation_date", "last_closed_date", "sales_volume_class_code", "sales_volume_class", "annualized_sales_volume",
        "total_annualized_volume", "open_to_buy", "open_to_buy_comments", "last_project_type", "gross_sq_ft",
        "selling_sq_ft", "frontage_linear_ft", "number_of_rooms", "number_of_cabinets", "last_activity_date",
        "plan_number_coprops", "like_store_indicator", "plan_nbr_ft_non_management", "base_week",
        "labor_schedule_enabled", "labor_schedule_copy_store", "district_name", "region_name", "zone_name",
        "district_code", "region_code", "zone_code", "latitude", "longitude", "cast(is_lw_pilot_store as string)",
        "cast(lw_pilot_market_tier as int)", "price_group_code", "currency_code")
  }

  /**
    * Get Store Ids from Transaction Log Files
    *
    * @return [[DataFrame]]
    */
  private def getTlogStoreIds: DataFrame = {
    val tlogSaleWork = hiveContext.table(TlogSaleWorkTable)
    val tlogNonMerchandise = hiveContext.table(TLogTransactionWorkTable)
    val tlogDiscount = hiveContext.table(TlogDiscountWorkTable)
    val tlogTenderWork = hiveContext.table(TLogTenderWorkTable)
    val tlogAdminWorkTable = hiveContext.table(TlogAdminWorkTable)
    Seq(tlogSaleWork, tlogNonMerchandise, tlogDiscount, tlogDiscount, tlogTenderWork, tlogAdminWorkTable)
      .map(_.selectExpr(s"$StoreIdCol as $TlogStoreIdCol"))
      .reduce(_ unionAll _)
      .distinct
  }


  /**
    * Create default data rows for new store ids
    *
    * @param storeKeys New store keys from Tlog files
    * @param columns   columns to be populated
    * @return [[DataFrame]]
    */
  private def generateDefaultStoreRecords(storeKeys: DataFrame, columns: Seq[String]): DataFrame = {
    val defaultValuesMap = Map("retail_web_flag" -> "'UNKNOWN'", "store_name" -> "'UNKNOWN'",
      "profile_code" -> "0", "profile_description" -> "'NO PROFILE DEFINED'", "rtl_flag" -> "'Y'", "store_design_code" -> "0",
      "store_design_description" -> "'UNDEFINED'", "sales_volume_class_code" -> "'U'", "sales_volume_class" -> "'UNDEFINED'",
      "district_name" -> "'UNKNOWN'", "region_name" -> "'UNKNOWN'", "zone_name" -> "'UNKNOWN'","district_code"-> "0",
      "region_code"-> "0", "zone_code"-> "0")
    val notNullCols = defaultValuesMap.keys.toSeq :+ StoreIdCol
    storeKeys
      .selectExpr(s"$TlogStoreIdCol as $StoreIdCol")
      .applyExpressions(defaultValuesMap)
      .applyExpressions(columns.filterNot(column => notNullCols.contains(column)).map(column => column -> "null").toMap)
  }


  override def transform: DataFrame = {
    val storeFilesTransformation = getStoreFilesTransformation.persist
    val storeFilesIds = storeFilesTransformation.select(StoreIdCol)
    val currentStoreIds = hiveContext.table(s"$goldDB.$dimensionTableName").filter("status = 'current'").select(StoreIdCol)
    val stubStoreIdDF = getTlogStoreIds
      .join(storeFilesIds.unionAll(currentStoreIds).distinct, $"$TlogStoreIdCol" === $"$StoreIdCol", "left")
      .filter(s"$StoreIdCol is null").persist
    logger.info("Unmatched store ids found in tlog files: {}", stubStoreIdDF.count.toString)
    generateDefaultStoreRecords(stubStoreIdDF, storeFilesTransformation.columns)
      .select(storeFilesTransformation.getColumns: _*)
      .unionAll(storeFilesTransformation)
  }

  def main(args: Array[String]): Unit = {
    val batchId = args(0)
    load(batchId)
  }


}
