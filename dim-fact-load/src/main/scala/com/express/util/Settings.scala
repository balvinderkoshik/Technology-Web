package com.express.util

import com.typesafe.config.{ConfigFactory, ConfigValue}
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.JavaConversions._

object Settings extends LazyLogging {


  private val conf = ConfigFactory.load("dim-fact-load")

  //Get Member transform config
  private val memberTransformMapping = conf.getConfig("member-transform")
  //Get MemberMultiEmail transform config
  private val memberMultiEmailTransformMapping = conf.getConfig("member-multi-email-transform")
  //Get MemberMultiPhone transform config
  private val memberMultiPhoneTransformMapping = conf.getConfig("member-multi-phone-transform")
  //Get MemberConsent transform config
  private val memberConsentTransformMapping = conf.getConfig("dim-member-consent-transform")


  /**
    * Get Column mapping for member transform based on input customer matching file
    *
    * @param file Customer Matching input file
    * @return [[Map[String, String]]
    */
  def getMemberTransformColumnMapping(file: String): (Map[String, String], Map[String, String]) = {
    val configMapFunc = (entry: java.util.Map.Entry[String, ConfigValue]) => entry.getKey -> entry.getValue.unwrapped.toString
    val columnMapping = memberTransformMapping.getConfig(file)
    val (deriveSet, transformSet) = columnMapping.entrySet().partition(_.getKey.startsWith("derived."))
    val derivedConfig = if (deriveSet.nonEmpty)
      columnMapping.getConfig("derived").entrySet().map(configMapFunc).toMap
    else
      Map[String, String]()
    (transformSet.map(configMapFunc).toMap, derivedConfig)
  }


  /**
    * Get Column mapping for member multi email transform based on input customer matching file
    *
    * @param file Customer Matching input file
    * @return [[Map[String, String]]
    */
  def getMemberMultiEmailTransformColumnMapping(file: String): Map[String, String] = {
    val columnMapping = memberMultiEmailTransformMapping.getConfig(file)
    columnMapping.entrySet.map(entry => entry.getKey -> entry.getValue.unwrapped.toString).toMap
  }

  /**
    * Get Column mapping for member multi phone transform based on input customer matching file
    *
    * @param file Customer Matching input file
    * @return [[Map[String, String]]
    */
  def getMemberMultiPhoneTransformColumnMapping(file: String): Map[String, String] = {
    val columnMapping = memberMultiPhoneTransformMapping.getConfig(file)
    columnMapping.entrySet.map(entry => entry.getKey -> entry.getValue.unwrapped.toString).toMap
  }

  /**
    * Get Column mapping for member consent transform based on input customer matching file
    *
    * @param file Customer Matching input file
    * @return [[Map[String, String]]
    */
  def getMemberConsentTransformColumnMapping(file: String): Map[String, String] = {
    val columnMapping = memberConsentTransformMapping.getConfig(file)
    columnMapping.entrySet.map(entry => entry.getKey -> entry.getValue.unwrapped.toString).toMap
  }
}
