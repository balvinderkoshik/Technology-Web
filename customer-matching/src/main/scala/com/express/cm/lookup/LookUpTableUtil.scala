package com.express.cm.lookup

import com.express.cdw.spark.udfs._
import com.express.cm.criteria._
import com.express.cm.lookup.LookUpTable._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions.{lit, not}
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

/**
  * Look Up Tables Utility.
  * Used to initialize the Lookup tables and get their [[DataFrame]] object when required
  *
  * @author mbadgujar
  */
class LookUpTableUtil(sqlContext: SQLContext) {

  private val lookupTableMap = LookUpTableList.map(lookupTable =>
    (lookupTable, lookupTable.getDF(sqlContext))).toMap

  /**
    * Get the Lookup Dataframe
    *
    * @param lookUpTable [[LookUpTable]] object
    * @return [[DataFrame]]
    */
  private def getLookupTable(lookUpTable: LookUpTable): DataFrame = lookupTableMap(lookUpTable)
}

object LookUpTableUtil extends LazyLogging {

  private var lookUpUtilInstance: LookUpTableUtil = _

  /**
    * Initialize the look up tables based on the [[LookUpTable.LookUpTableList]]
    *
    * @param sqlContext Spark Sql context
    */
  def init(sqlContext: SQLContext): Unit = {
    lookUpUtilInstance = new LookUpTableUtil(sqlContext)
  }

  /**
    * Get the Lookup table dataframe
    *
    * @param  lookUpTable [[LookUpTable]] object
    * @return [[DataFrame]]
    */
  def getLookupTable(lookUpTable: LookUpTable): DataFrame = lookUpUtilInstance.getLookupTable(lookUpTable)

  /**
    * Get the Lookup table dataframe with filtered non-empty columns
    *
    * @param  lookUpTable [[LookUpTable]] object
    * @return [[DataFrame]]
    */
  def getLookupTable(lookUpTable: LookUpTable, matchTrait: MatchTrait): DataFrame = {

    val lookUpDF = lookUpUtilInstance.getLookupTable(lookUpTable)
    import lookUpDF.sqlContext.implicits._

    // Not Empty/ Not Null UDF for String column type
    def ne(column: Column) = not(CheckEmptyUDF(column))

    // TODO : Check filter conditions for all Lookup table
    val filterExpr = (lookUpTable, matchTrait) match {
      case (MemberDimensionLookup, _: BankCardNameMatch) => ne($"first_name") and ne($"last_name")
      case (MemberDimensionLookup, _: EmailNameMatch) => ne($"first_name") and ne($"last_name")
      case (MemberDimensionLookup, LoyaltyIDMatch) => ne($"loyalty_id")
      case (MemberDimensionLookup, _: NameAddressMatch) => ne($"first_name") and ne($"last_name")
      case (MemberDimensionLookup, _: PhoneEmailNameMatch) => ne($"phone_nbr") or (ne($"first_name") and ne($"last_name"))
      //case (MemberDimensionLookup, _: PhoneNameMatch) => ne($"phone_nbr") or (ne($"first_name") and ne($"last_name"))
      case (MemberDimensionLookup, _: PhoneNameMatch) => ne($"first_name") and ne($"last_name")
      case (MemberDimensionLookup, _: PLCCNameMatch) => ne($"first_name") and ne($"last_name")
      case (_, _) => lit(true)
    }
    logger.info("Applying filter expression for LookupTable:{} :- {}", lookUpTable.getQualifiedTableName,
      filterExpr.toString.replace("UDF", "CheckEmptyUDF"))
    lookUpDF.filter(filterExpr)
  }

}