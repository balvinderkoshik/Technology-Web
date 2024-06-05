package com.express.utils


import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.JavaConversions._

object Settings extends LazyLogging {

  private val conf = ConfigFactory.load("customer-matching")

  //Get Customer matching config
  private val cmConfig = conf.getConfig("customer_matching")

  // Get LookUp table map
  def getLookUpTableMap: Map[String, (String, List[String], String)] = {
    cmConfig.getConfigList("lookup_tables").map(config => {
      val table = config.getString("table")
      val columns = config.getStringList("columns").toList
      val filter = Option(config.getString("filter")) match {
        case None => null
        case Some(str) => str.asInstanceOf[String]
      }
      (table, (table, columns, filter))
    }).toMap
  }

 // Get location for storing temporary customer matching results
  def getCMTemporaryArea : String = cmConfig.getString("temp_result_area")
}