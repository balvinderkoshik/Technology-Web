package com.express.processing.fact

import org.apache.spark.sql.{DataFrame, SaveMode}
import com.express.cdw.CDWContext
import com.express.cdw.spark.DataFrameUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
  * Fact Load Trait.
  * Implement for Fact tables.
  *
  * @author mbadgujar
  */
trait FactLoad extends CDWContext with LazyLogging {

  import hiveContext.implicits._

  var batchId: String = _

  /**
    * Get Fact Table name
    *
    * @return [[String]]
    */
  def factTableName: String


  /**
    * Get Surrogate Key Column
    *
    * @return [[String]]
    */
  def surrogateKeyColumn: String

  /**
    * Transformation for the Fact data
    *
    * @return Transformed [[DataFrame]]
    */
  def transform: DataFrame

  /**
    * Get Temporary fact table to be loaded
    *
    * @return Temp Table name
    */
  def tempFactTable: String = s"$workDB.${factTableName}_temp"

  /**
    * Get Back-Up Fact Table
    *
    * @return [[DataFrame]]
    */
  def backUpFactTableDF: DataFrame = hiveContext.table(s"$goldDB.$factTableName")

  def partitionBy : Option[String] = None

  /**
    * Load the Fact Data into Temp Fact table
    */
  def load(): Unit = {
    val factMaxSurrogateKey = backUpFactTableDF.maxKeyValue($"$surrogateKeyColumn")
    logger.info("Max surrogate key obtained for {} :- {} ", factTableName, factMaxSurrogateKey.toString)
    val finalFactDataset = transform.generateSequence(factMaxSurrogateKey, Some(surrogateKeyColumn))
    val targetFactColumns = hiveContext.table(tempFactTable).getColumns
    finalFactDataset.select(targetFactColumns: _*).insertIntoHive(SaveMode.Overwrite, transform, tempFactTable, partitionBy)
  }

}
