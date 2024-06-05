package com.datametica

import com.express.cdw.Settings
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.SparkContext
import com.datametica.ecat.HttpUtil._
import com.datametica.ecat.EcatRequests._
import com.datametica.ecat.model._

/**
  * object for Ecat utility methods
  *
  * @author mbadgujar
  */
package object ecat extends LazyLogging {

  private val (dataStore) = Settings.EcatDatastore

  private object CDWJobTypes {
    val DimensionLoad = "dimension_load"
    val FactLoad = "fact_load"
    val CustomerMatching = "customer_matching"
    val Deduplication = "customer_dedup"
    val Enrichment = "enrichment"
    val Transformation = "transform"
    val Default = "cdw_processing"
  }

  private def getJobType(appName: String) = {
    if (appName.contains("dim"))
      CDWJobTypes.DimensionLoad
    else if (appName.contains("fact"))
      CDWJobTypes.FactLoad
    else if (appName.contains("cm"))
      CDWJobTypes.CustomerMatching
    else if (appName.contains("transform"))
      CDWJobTypes.Transformation
    else if (appName.contains("enrich"))
      CDWJobTypes.Enrichment
    else if (appName.contains("dedup"))
      CDWJobTypes.Deduplication
    else
      CDWJobTypes.Default
  }


  /**
    * Logs the Spark lineage in ECAT using the Ecat cliet library
    *
    * @param context          Spark Context
    * @param sourceList       Input tables involved in transformations
    * @param destinationTable Output table
    * @param batchId          Batch Id
    */

  def logEcatLineage(context: SparkContext, sourceList: Seq[String], destinationTable: String, batchId: String): Unit =
  {
    logger.info("ECAT Starting Spark Lineage logging process")
    var httpUtil: HttpUtil = null
    try {
      if (sourceList.isEmpty) {
        logger.warn("ECAT No Source Hive tables found, lineage will not be logged")
        return
      }
      logger.info("ECAT Spark Lineage Sources:- {}", sourceList.mkString(", "))
      logger.info("ECAT Spark Lineage Destination:- {}", destinationTable)
      val httpUtil = new HttpUtil(DefaultClient)
      val authResponse = httpUtil.connectAndConvert(AuthRequest,classOf[Token])
      val token = Option(authResponse.access_token)
      val lineageOperations = sourceList.map(source => {
        LineageOperation(LineageSourceDetails(source), LineageSourceDetails(destinationTable),
          context.sparkUser, context.applicationId, context.appName)
      })
      val lineageData = LineageDetails(getJobType(context.appName.toLowerCase), "Spark", "4", dataStore, batchId, lineageOperations.toList)
      val lineagedataString = httpUtil.getJsonFromEntity(lineageData)
      val response = httpUtil.connect(LogLineageRequest(lineagedataString,token.getOrElse(throw new Exception("Authentication failed"))))
      logger.info("ECAT Lineage Logged Successfully")
    }catch {
      case e: Exception =>
        logger.error("ECAT Error while logging lineage:{}", e.getMessage)
        Option(httpUtil) match {
          case None =>
          case Some(_) => httpUtil.client.close()
        }
    }
  }
}
