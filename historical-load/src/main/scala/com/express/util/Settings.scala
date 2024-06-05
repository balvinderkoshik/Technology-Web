package com.express.util

import java.util

import javax.xml.crypto.dsig.keyinfo.KeyValue
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.Column

import scala.collection.JavaConversions._
import scala.collection.immutable.ListMap

import scala.collection.JavaConverters


/**
  * Created by Mahendranadh.Dasari on 26-10-2017.
  */
object Settings extends LazyLogging {


  private val conf = ConfigFactory.load("historical-load")

  //Get HistoryLoad transform config
  private val HistoryLoad = conf.getConfig("HistoryLoad")

  case class TableConfig(renameConfig: Map[String, String], transformConfig: Map[String, String], surrogateKey: String, generateSK: Boolean, doNotIncrSK: Boolean,
                         grpColumns: Seq[String], ordColumns: Seq[String])


  /**
    * Get Column mapping for dimension and fact transform based on input history files
    *
    * @param file History input file
    * @return [[Map[String, String]]
    */

  def getHistoryMapping(file: String): TableConfig = {
    val tableMapping = HistoryLoad.getConfig(file)

    def getMap(config: Config, orderedFlag: Boolean = false) = {
      if (orderedFlag) {
        val transformKeys = config.root().keys
        val sorted = transformKeys.map(key => {
          val transformConfig = config.getConfig(key)
          (key, transformConfig.getInt("pos"), transformConfig.getString("value"))
        }).toList.sortBy(_._2)
        ListMap(sorted.map{case(key, _, value) => (key, value)}:_*)
      }
      else
        config.entrySet.map(entry => entry.getKey -> entry.getValue.unwrapped.toString).toMap
    }

    val orderedFlag = tableMapping.hasPath("ordered")
    val renameConfig = getMap(tableMapping.getConfig("rename"))
    val transformConfig = getMap(tableMapping.getConfig("transform"), orderedFlag)
    val surrogateKey = tableMapping.getString("surrogateKey")
    val generateSK = tableMapping.getBoolean("generateSK")
    val doNotIncrSK = tableMapping.getBoolean("doNotIncrSK")
    val grpColumns = tableMapping.getList("grpColumns").unwrapped.map(_.toString)
    val ordColumns = tableMapping.getList("ordColumns").unwrapped.map(_.toString)


    TableConfig(renameConfig, transformConfig, surrogateKey, generateSK, doNotIncrSK, grpColumns, ordColumns)
  }


}







