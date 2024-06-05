package com.express.processing.dim

import com.express.cdw.spark.DataFrameUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by akshay.rochwani on 7/12/2017.
  */
object DimProduct extends DimensionLoad with LazyLogging {

  import hiveContext.implicits._

  private val TlogDiscountWorkTable = s"$workDB.work_tlog_discount_dedup"
  private val TlogSaleWorkTable = s"$workDB.work_tlog_sale_dedup"
  private val TLogTenderWorkTable = s"$workDB.work_tlog_tender_dedup"
  private val TLogTransactionWorkTable = s"$workDB.work_tlog_transaction_dedup"
  private val TlogAdminWorkTable = s"$workDB.work_tlog_admin_dedup"
  private val TlogItemNbrCol = "item_number"
  private val ItemNbrCol = "item_nbr"

  override def dimensionTableName: String = "dim_product"

  override def surrogateKeyColumn: String = "product_key"

  override def naturalKeys: Seq[String] = Seq("item_nbr", "price_group_code")

  override def derivedColumns: Seq[String] = Nil

  /**
    * Get the transformed product dataframe from product reference files
    *
    * @return [[DataFrame]]
    */
  private def getProductFilesTransformation: DataFrame = {
    import hiveContext.implicits._

    /* Fetching work_proditem_dataquality dataset */

    val prodItemWorkTable = s"$workDB.work_proditem_dataquality"
    logger.info("Reading Prod Item Work Table: {}", prodItemWorkTable)


    val prodItemDF = hiveContext.table(prodItemWorkTable)
      .select("item_number", "price_group", "class_style_number", "class_code_1", "from_item_number", "from_class_style_number",
        "from_class", "from_style", "from_color", "from_size", "item_description", "item_att_01_season", "item_att_02_exit_date",
        "item_att_03_floorset", "item_att_04_silhouette", "item_att_05_dissection", "item_att_06_length", "item_att_07_base_fabric",
        "item_att_08_quota", "item_att_09_sloper_fit", "item_att_10_end_use", "on_order_qty_sign", "on_order_quantity",
        "next_receipt_qty_sign", "next_receipt_quantity", "next_receipt_date", "file_retail_sign", "file_retail_price",
        "valued_cost_sign", "valued_cost", "item_status", "division", "department", "po_cost_sign", "po_cost", "item_last_digits",
        "ticket_price_sign", "ticket_price", "item_create_date", "division", "color", "size", "style", "item_att_01_season", "batch_id")

    /* Fetching work_prodclass_dataquality dataset */

    val prodClassWorkTable = s"$workDB.work_prodclass_dataquality"
    logger.info("Reading Prod Class Work Table: {}", prodClassWorkTable)
    val prodClassDF = hiveContext.table(prodClassWorkTable)
      .select("class_code", "class_status", "class_name", "last_season_reset_date", "shrinkage_factor", "last_season_reset_iso_date")

    /* Fetching work_prodcolor_dataquality dataset */

    val prodColorWorkTable = s"$workDB.work_prodcolor_dataquality"
    logger.info("Reading Prod Color Work Table: {}", prodColorWorkTable)
    val prodColorDF = hiveContext.table(prodColorWorkTable)
      .select("color_code", "color_name")

    /* Fetching work_prodhier_dataquality dataset */

    val prodHierWorkTable = s"$workDB.work_prodhier_dataquality"
    logger.info("Reading Prod Hier Work Table: {}", prodHierWorkTable)
    val prodHierDF = hiveContext.table(prodHierWorkTable)
      .select("class_number", "division_name", "division_stat_code", "department_name", "department_stat_code")

    /* Fetching work_prodsize_dataquality dataset */

    val prodSizeWorkTable = s"$workDB.work_prodsize_dataquality"
    logger.info("Reading Prod Size Work Table: {}", prodSizeWorkTable)
    val prodSizeDF = hiveContext.table(prodSizeWorkTable)
      .select("size", "size_name")

    /* Fetching work_prodstyle_dataquality dataset */

    val prodStyleWorkTable = s"$workDB.work_prodstyle_dataquality"
    logger.info("Reading Prod Style Work Table: {}", prodStyleWorkTable)
    val prodStyleDF = hiveContext.table(prodStyleWorkTable)
      .select("class", "vendor", "style", "batch_id")

    /* Fetching work_item_uda_dataquality dataset */

    val itemUdaWorkTable = s"$workDB.work_item_uda_dataquality"
    logger.info("Reading Item Uda Work Table: {}", itemUdaWorkTable)
    val itemUdaDF = hiveContext.table(itemUdaWorkTable)
      .filter("uda_id = 1")

    /*Dataset formed after joining ProdItem and ProdClass over class_code_1 and class_code*/

    val joinPiPcDF = prodItemDF.join(prodClassDF,
      prodItemDF.col("class_code_1") === prodClassDF.col("class_code"), "left")
      .withColumnRenamed("item_number", "item_number")
      .withColumnRenamed("style", "style_cd")
      .withColumnRenamed("price_group", "price_group_code")
      .withColumnRenamed("class_style_number", "class_style_nbr")
      .withColumnRenamed("from_item_number", "from_item_nbr")
      .withColumnRenamed("from_class_style_number", "from_class_style_nbr")
      .withColumn("season", prodItemDF.col("item_att_01_season"))
      .withColumnRenamed("item_att_02_exit_date", "distro_method")
      .withColumnRenamed("item_att_03_floorset", "floorset")
      .withColumnRenamed("item_att_04_silhouette", "trend")
      .withColumnRenamed("item_att_05_dissection", "dissection")
      .withColumnRenamed("item_att_06_length", "pricing_tier")
      .withColumnRenamed("item_att_07_base_fabric", "fashion_pyramid")
      .withColumnRenamed("item_att_08_quota", "fabric")
      .withColumnRenamed("item_att_09_sloper_fit", "quota")
      .withColumnRenamed("item_att_10_end_use", "lifestyle")
      .withColumnRenamed("department", "department_cd")
      .withColumnRenamed("class_code", "class_cd")
      .withColumn("division_cd", prodItemDF.col("division"))
      .withColumn("season_cd", prodItemDF.col("item_att_01_season"))
      .withColumn("key_item_nbr", lit(null))
      .withColumn("key_item_descr", lit(null))

    /*Dataset formed after joining the above dataframe and ProdStyle on class,style and batch_id*/

    val joinPiPtPsDF = joinPiPcDF.join(prodStyleDF,
      joinPiPcDF.col("class_code_1") === prodStyleDF.col("class") &&
        joinPiPcDF.col("style_cd") === prodStyleDF.col("style") &&
        joinPiPcDF.col("batch_id") === prodStyleDF.col("batch_id")
      , "left")
      .withColumnRenamed("vendor", "vendor_cd")

    /*Dataset formed after joining the above dataframe and ProdColor on color_code*/

    val joinPiPcPtPoDF = joinPiPtPsDF.join(prodColorDF,
      joinPiPtPsDF.col("color") === prodColorDF.col("color_code"), "left")
      .withColumnRenamed("color_code", "color_cd")

    /*Dataset formed after joining the above dataframe and ProdSize on size*/

    val joinPiPcPtPoPsDF = joinPiPcPtPoDF.join(prodSizeDF,
      Seq("size"), "left")
      .withColumnRenamed("size", "size_cd")


    /*Dataset formed after joining the above dataframe and ProdHier on class_code_1*/

    val joinPiPcPtPoPsPhDF = joinPiPcPtPoPsDF.join(prodHierDF,
      joinPiPcPtPoPsDF.col("class_code_1") === prodHierDF.col("class_number"), "left")
      .withColumnRenamed("division_stat_code", "division_stat_cd")
      .withColumnRenamed("department_name", "department_descr")
      .withColumnRenamed("department_stat_code", "department_stat_cd")
      .withColumnRenamed("item_number", "item_nbr")

    /*Dataset formed after joining the above dataframe and Item_Uda on item_number
    and forming the final dataframe with all required columns*/

    joinPiPcPtPoPsPhDF.join(itemUdaDF,
      joinPiPcPtPoPsPhDF.col("item_nbr") === itemUdaDF.col("item_number"), "left")
      .withColumn("gender", when(not(isnull($"uda_value_desc")) and ($"uda_value_desc" !== ""), substring($"uda_value_desc", 0, 1)).otherwise(trim($"department_stat_cd")))
      .selectExpr("(case when item_nbr is null or item_nbr = '' then NULL  else cast(item_nbr as bigint) end) as item_nbr",
        "(case when price_group_code is null or price_group_code = '' then NULL else price_group_code end) as price_group_code",
        "cast(class_style_nbr as bigint)", "cast(from_item_nbr as bigint)", "cast(from_class_style_nbr as bigint)", "cast(from_class as bigint)",
        "cast(from_style as bigint)", "cast(from_color as bigint)", "from_size", "item_description", "season", "distro_method", "floorset",
        "trend", "dissection", "pricing_tier", "fashion_pyramid", "fabric", "quota", "lifestyle", "on_order_qty_sign", "cast(on_order_quantity as bigint)",
        "next_receipt_qty_sign", "cast(next_receipt_quantity as bigint)", "next_receipt_date", "file_retail_sign", "cast(file_retail_price as decimal(10,2))",
        "valued_cost_sign", "cast(valued_cost as decimal(10,2))", "item_status", "cast(division as long)", "department_cd", "po_cost_sign",
        "cast(po_cost as decimal(10,2))", "cast(item_last_digits as bigint)", "ticket_price_sign", "cast(ticket_price as decimal(10,2))",
        "item_create_date", "cast(class_cd as long)", "cast(division_cd as long)", "division_name", "division_stat_cd", "class_status", "class_name",
        "cast(last_season_reset_date as date)", "cast(shrinkage_factor as decimal(10,2))", "season_cd", "cast(last_season_reset_iso_date as date)",
        "cast(color_cd as long)", "color_name", "cast(size_cd as long)", "size_name", "cast(style_cd as bigint)",
        "cast(vendor_cd as bigint)", "department_descr", "gender", "department_stat_cd", "cast(key_item_nbr as bigint)", "cast(key_item_descr as string)")

  }

  /**
    * Get Product Ids from Transaction Log Files
    *
    * @return [[DataFrame]]
    */
  private def getTlogProductIds: DataFrame = {
    val tlogSaleWork = hiveContext.table(TlogSaleWorkTable)
    val tlogNonMerchandise = hiveContext.table(TLogTransactionWorkTable)
    val tlogDiscount = hiveContext.table(TlogDiscountWorkTable)
    val tlogTenderWork = hiveContext.table(TLogTenderWorkTable)
    val tlogAdminWorkTable = hiveContext.table(TlogAdminWorkTable)
    Seq(tlogSaleWork, tlogNonMerchandise, tlogDiscount, tlogDiscount, tlogTenderWork, tlogAdminWorkTable)
      .map(_.select(TlogItemNbrCol))
      .reduce(_ unionAll _)
      .distinct
  }

  /**
    * Create default data rows for new product ids
    *
    * @param productKeys New product keys from Tlog files
    * @param columns     columns to be populated
    * @return [[DataFrame]]
    */
  private def generateDefaultProductRecords(productKeys: DataFrame, columns: Seq[String]): DataFrame = {
    val defaultValuesMap = Map("department_cd" -> "-1", "class_cd" -> "-1", "division_cd" -> "-1", "division_name" -> "'UNKNOWN'",
      "color_cd" -> "-1", "color_name" -> "'UNKNOWN'", "size_cd" -> "-1", "size_name" -> "'UNKNOWN'",
      "style_cd" -> "-1", "department_descr" -> "'UNKNOWN'")
    val notNullCols = defaultValuesMap.keys.toSeq :+ ItemNbrCol
    productKeys
      .selectExpr(s"$TlogItemNbrCol as $ItemNbrCol")
      .applyExpressions(defaultValuesMap)
      .applyExpressions(columns.filterNot(column => notNullCols.contains(column)).map(column => column -> "null").toMap)
  }


  override def transform: DataFrame = {
    val productFilesTransformation = getProductFilesTransformation.persist
    val productFilesIds = productFilesTransformation.select(ItemNbrCol)
    val currentProductIds = hiveContext.table(s"$goldDB.$dimensionTableName").filter("status = 'current'").select(ItemNbrCol)
    val stubProductIdDF = getTlogProductIds
      .join(productFilesIds.unionAll(currentProductIds).distinct, $"$TlogItemNbrCol" === $"$ItemNbrCol", "left")
      .filter(s"$ItemNbrCol is null").persist
    logger.info("Unmatched products found in tlog file: {}", stubProductIdDF.count.toString)
    generateDefaultProductRecords(stubProductIdDF, productFilesTransformation.columns)
      .select(productFilesTransformation.getColumns: _*)
      .unionAll(productFilesTransformation)
  }

  def main(args: Array[String]): Unit = {
    val batchId = args(0)
    load(batchId)
  }

}