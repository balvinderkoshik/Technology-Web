package com.express.util

  import java.util.Properties

  import com.typesafe.config.{ConfigFactory, ConfigValue}
  import com.typesafe.scalalogging.slf4j.LazyLogging


  object Settings extends LazyLogging {

    // jdbc connection

    private val ojdbc_conf = ConfigFactory.load("ojdbc")

    private val OJDBCConfig = ojdbc_conf.getConfig("OJDBC")

    def getConnectionProperties : Properties = {
      val connectionProperties = new Properties()
      connectionProperties.setProperty("driver", OJDBCConfig.getString("driverClass"))
      connectionProperties.put("user", getUser)
      connectionProperties.put("password", getPassword)
      connectionProperties
    }

    def getJDBCUrl : String ={
      s"jdbc:oracle:thin:@${OJDBCConfig.getString("jdbcHostname")}:${OJDBCConfig.getString("jdbcPort")}/${OJDBCConfig.getString("jdbcDatabase")}"
    }

    def getUser : String = {
      OJDBCConfig.getString("user")
    }

    def getPassword : String ={
      OJDBCConfig.getString("password")
    }

  }

