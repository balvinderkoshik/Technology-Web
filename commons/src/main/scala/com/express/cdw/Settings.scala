package com.express.cdw

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkConf

import scala.collection.JavaConversions._

/**
  * Common Settings to be used across all Customer Data warehouse processing modules
  *
  * @author mbadgujar
  */
object Settings extends LazyLogging {

  private val conf = ConfigFactory.load()

  //Get Spark Configs
  private val sparkConfig = conf.getConfig("spark")
  private val sparkAdditionalConfig = sparkConfig.getConfig("additional_configs")
  private val sparkMaster = sparkConfig.getString("master")
  private val sparkAppName = sparkConfig.getString("app_name")

  //Get Customer Data warehouse config
  private val cdwConfig = conf.getConfig("cdw_config")

  // Get Ecat config
  private val ecatConfig = conf.getConfig("ecat")

  /**
    * Get Spark Configuration
    *
    * @return [[SparkConf]]
    */
  def sparkConf: SparkConf = {
    def stripQuotes(s: String) = s.replaceAllLiterally("\"", "")

    val sparkConf = new SparkConf()
    if (!sparkConf.contains("spark.master")) {
      logger.info("Spark master not set, defaulting to local mode")
      sparkConf.setMaster(sparkMaster)
    }

    if (!sparkConf.contains("spark.app.name"))
      sparkConf.setAppName(sparkAppName)
    logger.debug(s"Spark Master is : ${sparkConf.get("spark.master")}")
    logger.debug(s"Spark App Name is : ${sparkConf.get("spark.app.name")}")
    sparkAdditionalConfig.entrySet.foreach(e => sparkConf.set(stripQuotes(e.getKey), e.getValue.unwrapped.asInstanceOf[String]))
    sparkConf
  }

  /**
    * Get Spark Coalesce partitions
    *
    * @return [[Int]]
    */
  def getCoalescePartitions: Int = sparkConfig.getString("coalesce_partitions").toInt

  /**
    * Get the Gold database
    *
    * @return [[String]]
    */
  def getGoldDB: String = cdwConfig.getString("gold_db") match {
    case "internal" =>
      logger.warn("Spark temporary catalog will be used to find gold/lookup tables")
      ""
    case other =>
      logger.info("Gold Database: {}", other)
      other
  }

  /**
    * Get the Work Database
    *
    * @return [[String]]
    */
  def getWorkDB: String = {
    val workDB = cdwConfig.getString("work_db")
    logger.info("Work Database: {}", workDB)
    workDB
  }

  /**
    * Get the Backup Database
    *
    * @return [[String]]
    */
  def getBackupDB: String = {
    val backupDB = cdwConfig.getString("backup_db")
    logger.info("Backup Database: {}", backupDB)
    backupDB
  }

  /**
    * Get the Smith Database
    *
    * @return [[String]]
    */
  def getSmithDB: String = {
    val smithDB = cdwConfig.getString("smith_db")
    logger.info("Smith Database: {}", smithDB)
    smithDB
  }


  /**
    * Get Ecat configuration Information
    *
    * @return Tuple2[EcatCredentials, Ecat Datastore]
    */

  val EcatHostUrl: String = ecatConfig.getString("url")

  val EcatAuthInfo: String = {
    val user = ecatConfig.getString("username")
    val password = ecatConfig.getString("password")
    s"username=$user&password=$password&grant_type=password&client_id=ecat&client_secret=ecat"
  }

  val EcatDatastore : String = ecatConfig.getString("datastore")

}