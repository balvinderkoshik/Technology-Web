package com.express.cdw.test.utils

import com.express.cdw.Settings
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.test.TestHiveContext

/**
  * Spark Utility for Testing Purpose
  *
  * @author mbadgujar
  */
object SparkUtil {

  private val sparkConf = Settings.sparkConf
  private val sparkContext = new SparkContext(sparkConf)
  private val hadoopUtilDir = this.getClass.getClassLoader.getResource("hadoop-util")
  System.setProperty("hadoop.home.dir", hadoopUtilDir.getPath)
  System.setProperty("hive.exec.scratchdir", "/tmp/cdw-test/hive")


  /**
    * Get Test SQL Context
    *
    * @return [[SQLContext]]
    */
  def getSQLContext: SQLContext = new SQLContext(sparkContext)

  /**
    * Get Test Hive Context
    *
    * @return [[HiveContext]]
    */
  def getHiveContext: HiveContext = {
    new TestHiveContext(sparkContext)
  }
}
