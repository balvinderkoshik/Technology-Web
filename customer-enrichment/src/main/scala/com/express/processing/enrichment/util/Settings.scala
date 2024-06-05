package com.express.processing.enrichment.util

import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
  * Created by Monashree.Sanil on 19-11-2018.
  */

object Settings extends LazyLogging{

  private val conf = ConfigFactory.load("ojdbc_config")

  private val OJDBCConfig = conf.getConfig("OJDBC")

  private val TEMPDIRConfig = conf.getConfig("TempDirs")

  def getConnectionProperties() : Properties = {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("driver", OJDBCConfig.getString("driverClass"))
    connectionProperties.put("user", getUser())
    connectionProperties.put("password", getPassword())
    connectionProperties
  }

  def getJDBCUrl() : String ={
    s"jdbc:oracle:thin:@${OJDBCConfig.getString("jdbcHostname")}:${OJDBCConfig.getString("jdbcPort")}/${OJDBCConfig.getString("jdbcDatabase")}"
  }

  def getUser() : String = {
     OJDBCConfig.getString("user")
  }

  def getPassword() : String ={
    OJDBCConfig.getString("password")
  }

  def getfileNamePartitionedDir() : String={
    TEMPDIRConfig.getString("fileNamePartitionedDir")
  }

  def getfinalPartFilesDir() : String={
    TEMPDIRConfig.getString("finalPartFilesDir")
  }

  def getworkTempTableDir() : String={
    TEMPDIRConfig.getString("workTempTableDir")
  }

  def getgoldDimMemberHistDir() : String={
    TEMPDIRConfig.getString("goldDimMemberHistDir")
  }

}
