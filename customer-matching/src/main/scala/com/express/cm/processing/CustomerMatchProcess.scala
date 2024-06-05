package com.express.cm.processing

import com.express.cdw.CDWContext
import com.express.cm.lookup.LookUpTableUtil
import com.express.cm._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SQLContext, UserDefinedFunction}

/**
  * Trait used by all input files for Customer Matching
  *
  * @author mbadgujar
  */
trait CustomerMatchProcess extends CDWContext with LazyLogging {

  // Source key UDF based on Gender
  val sourceKeyGenderFunc: (Int, Int) => UserDefinedFunction = (sourceKeyMale: Int, sourceKeyFemale: Int) =>
    udf((gender: String) => if (gender.toLowerCase.startsWith("m")) sourceKeyMale else sourceKeyFemale)


  /**
    * Initialize Customer Matching spark context
    *
    * @return [[SQLContext]]
    */
  def initCMContext: SQLContext = {
    logger.info("Initializing LookUp Tables")
    // Register UDFs to be used in CM Config
    hiveContext.udf.register[Boolean, String]("check_phone_validity", (phone: String) => checkPhoneValidity(formatPhoneNumberUSCan(phone)))
    // init Lookup tables
    LookUpTableUtil.init(hiveContext)
    hiveContext
  }

  /**
    * Get the Source key for Input file
    *
    * @return [[Column]]
    */
  def getSourceKey: Column

}
