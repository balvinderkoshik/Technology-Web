package com.datametica.ecat

import org.apache.http.client.methods.{HttpPost, HttpRequestBase}
import org.apache.http.entity.{ContentType, StringEntity}
import com.express.cdw.Settings
import org.apache.http.client.config.RequestConfig

/**
  * Ecat Rest API requests
  *
  * @author mbadgujar
  */
object EcatRequests {

  private val EcatAuthenticationAPI = "oauth/token"
  private val EcatLineageAPI = "api/rest/v1/lineage/data"

  private val settings = Settings

  //Time out settings
  private val defaultRequestConfig = RequestConfig.copy(RequestConfig.DEFAULT)
    .setSocketTimeout(10000)
    .setConnectTimeout(10000)
    .setConnectionRequestTimeout(10000)
    .build()

  sealed trait Request[T <: HttpRequestBase] {
    def getRequest: T
  }

  // Authentication Request
  object AuthRequest extends Request[HttpPost] {
    var client:HttpPost  = null
    override def getRequest: HttpPost = {
      val authenticateCall = new HttpPost(settings.EcatHostUrl + "/" + EcatAuthenticationAPI)
      val entity = new StringEntity(settings.EcatAuthInfo, ContentType.APPLICATION_FORM_URLENCODED)
      authenticateCall.setEntity(entity)
      authenticateCall.setConfig(defaultRequestConfig)
      authenticateCall
    }
  }

  //Log Lineage Request
  case class LogLineageRequest(lineageJson: String, accessToken: String) extends Request[HttpPost] {
    override def getRequest: HttpPost = {

      val lineageCall = new HttpPost(settings.EcatHostUrl + "/" + EcatLineageAPI)
      lineageCall.setHeader("Accept", ContentType.APPLICATION_JSON.toString)
      lineageCall.setHeader("Content-Type", ContentType.APPLICATION_JSON.toString)
      lineageCall.setHeader("Authorization", "Bearer " + accessToken)
      val entity = new StringEntity(lineageJson, ContentType.APPLICATION_JSON)
      lineageCall.setEntity(entity)
      lineageCall.setConfig(defaultRequestConfig)
      lineageCall

    }
  }

}
