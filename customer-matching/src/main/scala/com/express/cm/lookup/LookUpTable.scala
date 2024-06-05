package com.express.cm.lookup

import com.express.cdw.Settings
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
  * The class represents a Look Up table, The lookup table will be persisted across the
  * Customer matching process. Optional filter condition can be provided
  *
  * @author mbadgujar
  */
class LookUpTable(tableName: String, columnList: List[String] = List(),
                  filterCondition: String = "") extends LazyLogging {

  import LookUpTable._

  /**
    * Get the Table name
    *
    * @return [[String]] table name
    */
  def getQualifiedTableName: String = database match {
    case "" => tableName
    case _ => database + "." + tableName
  }

  /**
    * Get the [[DataFrame]]for the Lookup table
    *
    * @param sqlContext SQL / Hive context
    * @return [[DataFrame]]
    */
  def getDF(sqlContext: SQLContext): DataFrame = {
    logger.info("Registering Lookup Table: {} with filter condition: {}", getQualifiedTableName, filterCondition)
    val df = sqlContext.table(getQualifiedTableName)

    // apply filter
    val dfWithFilter = filterCondition.trim match {
      case "" => df
      case filter => df.filter(filter)
    }

    val finalDF = if (tableName == "fact_transaction_detail") {
      val dimTime = TimeDimensionLookup.getDF(sqlContext)
      val joinedDfWithFilter = dfWithFilter.join(broadcast(dimTime), col("trxn_time_key") === col("time_key"), "left").drop("time_key")
      joinedDfWithFilter
        .groupBy("member_key")
        .agg(max("trxn_date").alias("trxn_date"), max("time_in_24hr_day").alias("time_in_24hr_day"))
    } else {
      dfWithFilter
    }

    // apply select if applicable
    (if (columnList.nonEmpty)
      finalDF.select(columnList.head, columnList.tail: _*)
    else
      finalDF
      ).persist()
  }

}

object LookUpTable {

  // Get the lookup database
  private val database = Settings.getGoldDB
  private val lookupTableMap = com.express.utils.Settings.getLookUpTableMap.map {
    case (tableName, (table, columns, filter)) => (tableName, new LookUpTable(table, columns, filter))
  }

  // Employee Dimension Lookup table
  val EmployeeDimensionLookup: LookUpTable = lookupTableMap("dim_employee")

  //Member Dimension Lookup
  val MemberDimensionLookup: LookUpTable = lookupTableMap("dim_member")

  //TenderType Dimension Lookup
  val TenderTypeDimensionLookup: LookUpTable = lookupTableMap("dim_tender_type")

  // Card Type Dimension Lookup
  val CardTypeDimensionLookup: LookUpTable = lookupTableMap("dim_card_type")

  //MemberMultiEmailFactLookup
  val MemberMultiEmailDimensionLookup: LookUpTable = lookupTableMap("dim_member_multi_email")

  //MemberMultiPhoneFactLookup
  val MemberMultiPhoneDimensionLookup: LookUpTable = lookupTableMap("dim_member_multi_phone")

  //Card History Fact Lookup
  val CardHistoryFactLookup: LookUpTable = lookupTableMap("fact_card_history")

  //Transaction History Fact Lookup
  val TransactionDetailFactLookup: LookUpTable = lookupTableMap("fact_transaction_detail")

  //TenderHistory Fact Lookup
  val TenderHistoryFactLookup: LookUpTable = lookupTableMap("fact_tender_history")

  val TimeDimensionLookup: LookUpTable = lookupTableMap("dim_time")


  /*
    Add the LookUpTable objects that are to be initialized in this list
   */
  val LookUpTableList: List[LookUpTable] = List(EmployeeDimensionLookup, MemberDimensionLookup, TenderHistoryFactLookup,
    TenderTypeDimensionLookup, CardTypeDimensionLookup, CardHistoryFactLookup, TransactionDetailFactLookup, MemberMultiEmailDimensionLookup, TimeDimensionLookup, MemberMultiPhoneDimensionLookup)

}
