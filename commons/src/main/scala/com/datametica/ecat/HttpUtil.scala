package com.datametica.ecat

import com.datametica.ecat.HttpUtil.Client
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/**
  * Http Utility Methods
  *
  * @author mbadgujar
  */
case class HttpUtil(client: Client) {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)


  def getJsonFromEntity(entity: Any) : String = mapper.writeValueAsString(entity)

  /**
    * Connect to Rest Endpoint and convert the json response to provided Type
    *
    * @param request  [[HttpRequestBase]] Http request
    * @tparam B object type required
    * @return [[B]]
    */
  def connectAndConvert[B <: Any](request: EcatRequests.Request[_ <: HttpRequestBase], clz: Class[B]): B = {
    val content = client.getResponseAsStringFunc(request.getRequest)
    mapper.readValue(content, clz)
  }


  /**
    * Connect to rest endpoint and return the response
    *
    * @param request http request
    * @return [[HttpResponse]]
    */
  def connect(request: EcatRequests.Request[_ <: HttpRequestBase]): HttpResponse = client.getResponseFunc(request.getRequest)

}


object HttpUtil {

  /**
    * HTTP Client Trait
    */
  trait Client {
    /**
      * Get Response as String
      *
      * @tparam T Http Request
      * @return String
      */
    def getResponseAsStringFunc[T <: HttpRequestBase]: T => String

    /**
      * Get Response Object
      *
      * @tparam T Http Request
      * @return Http Response object
      */
    def getResponseFunc[T <: HttpRequestBase]: T => HttpResponse
    def close(): Unit

  }

  // Default client implementation using Apache HTTP  default Client
  object DefaultClient extends Client {
    private val client = HttpClients.createDefault()

    override def getResponseAsStringFunc[T <: HttpRequestBase]: T => String = {
      (request: T) => {
        val response = client.execute(request)
        val content = EntityUtils.toString(response.getEntity)
        response.close()
        content
      }
    }
    override def getResponseFunc[T <: HttpRequestBase]: T => HttpResponse = (request: T) => client.execute(request)

    override def close(): Unit = client.close()
  }

}
