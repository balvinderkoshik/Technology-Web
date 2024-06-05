package com.express.processing.dim

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame

/**
  * Created by akshay.rochwani on 7/12/2017.
  */
object DimCurrency extends DimensionLoad with LazyLogging {

  override def dimensionTableName: String = "dim_currency"

  override def surrogateKeyColumn: String = "currency_key"

  override def naturalKeys: Seq[String] = Seq("currency_code")

  override def derivedColumns: Seq[String] = Nil

  override def transform: DataFrame = {
    val currencyWorkTable = s"$workDB.work_currency_dataquality"
    logger.info("Reading Currency Work Table: {}", currencyWorkTable)
    hiveContext.table(currencyWorkTable)
      .filter("base = 'USD'")
      .withColumnRenamed("quote", "currency_code")
      .withColumnRenamed("effective_date", "currency_date")
      .withColumnRenamed("bid", "usd_rate")
  }

  def main(args: Array[String]): Unit = {
    val batchId = args(0)
    load(batchId)
  }

}
