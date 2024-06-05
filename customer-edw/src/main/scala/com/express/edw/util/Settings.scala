package com.express.edw.util

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.JavaConversions._



object Settings extends LazyLogging {

  private val conf = ConfigFactory.load("customer-edw")

  //Get HistoryLoad transform config
  private val CustomerEdw = conf.getConfig("CustomerEdw")
  //Get customer-edw-load transform config
  private val CustomerEdwLoad = conf.getConfig("customer-edw-load")

  private val CustomerLookUpLoad = conf.getConfig("CustomerEdwLookup")
  case class FeedConfig(joinColumnConfig: Map[String, Seq[String]], finalColumnConfig: Seq[String], tableMainNameConfig: String, tableLookupNameConfig: String, regexReplaceColumnsConfig: Seq[String],validationConfig: String)

  def getMap(config: Config, orderedFlag: Boolean = false): Map[String, Seq[String]] = {
    config.entrySet.map(entry => entry.getKey -> Seq(entry.getValue.unwrapped.toString)).toMap
    //config.entrySet.map(entry => entry.getKey -> entry.getValue.unwrapped().asInstanceOf[java.util.ArrayList[String]].toSeq).toMap
  }

  def getIncMap(config: Config ): Map[String, String] = {
    config.entrySet.map(entry => entry.getKey -> entry.getValue.unwrapped.toString).toMap
  }

  def getFeedMapping(file: String ): FeedConfig = {

    val feedTableMapping = CustomerEdw.getConfig(file)
    val joinColumnConfig = getMap(feedTableMapping.getConfig("joinColumn"))
    val finalColumnConfig = feedTableMapping.getList("finalColumns").unwrapped.toSeq.map(_.toString)
    val tableMainNameConfig = feedTableMapping.getString("tableMainSmith")
    val tableLookupNameConfig = feedTableMapping.getString("tableLookupSmith")
    val regexReplaceColumnsConfig = feedTableMapping.getList("regexReplaceColumns").unwrapped.map(_.toString)
    val validationConfig = feedTableMapping.getString("validation")

    FeedConfig(joinColumnConfig, finalColumnConfig,tableMainNameConfig,tableLookupNameConfig,regexReplaceColumnsConfig,validationConfig)
  }

  case class EdwConfig(renameConfig: Map[String, String], transformConfig: Map[String, String])

  def getEDWConfigMapping(file: String): EdwConfig = {
    val tableMapping = CustomerEdwLoad.getConfig(file)

    val renameConfig = getIncMap(tableMapping.getConfig("rename"))
    val transformConfig = getIncMap(tableMapping.getConfig("transform"))

    EdwConfig(renameConfig, transformConfig)
  }

  case class LookUPConfig (targetTableName: String, sourceTableName: String, filterCondition: String, sourceColumnList: Seq[String] )

  def getLookUoConfigMapping(file: String): LookUPConfig = {
    val lookMapping = CustomerLookUpLoad.getConfig(file)

    val targetTableName = lookMapping.getString("targetTableName")
    val sourceTableName = lookMapping.getString("sourceTableName")
    val filterCondition = lookMapping.getString("filterCondition")
    val sourceColumnList = lookMapping.getList("sourceColumnList").unwrapped.map(_.toString)

    LookUPConfig(targetTableName, sourceTableName, filterCondition, sourceColumnList)

  }

}

