package com.express.dedup.utils
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.collection.JavaConversions._
/**
  * Created by aman.jain on 7/12/2017.
  */


object Settings extends LazyLogging {

  private val conf = ConfigFactory.load("customer-dedup")

  //Get Customer matching config
  private val dedupConfig = conf.getConfig("customer_deduplication")

  private val updateDimConfig = conf.getConfig("update_dimensions")

  case class TableConfig( partition_columns:Seq[String],order_columns:Seq[String])

 // Get location for storing temporary customer matching results
  def getDeDupTemporaryArea : String = dedupConfig.getString("temp_result_area")

  def getFuzzyThreshold : Int = dedupConfig.getInt("fuzzy_threshold")

  def getTableConfig(tablename:String): TableConfig = {
      val dimConfig=updateDimConfig.getConfig(tablename)
      val partition_columns=dimConfig.getList("partition_columns").unwrapped.map(_.toString)
      val order_columns=dimConfig.getList("order_columns").unwrapped.map(_.toString)
      TableConfig(partition_columns,order_columns)
  }
}