package com.express

import java.math.BigDecimal
import java.sql.Date

import com.express.cdw.model.Store
import org.apache.commons.cli.{BasicParser, OptionBuilder, Options}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


/**
  * Global Case classes, Constants used by all Customer Data warehouse processing modules
  *
  * @author mbadgujar
  */
package object cdw {

  // Customer Matching Meta columns
  val MatchStatusColumn = "match_status"
  val MemberKeyColumn = "member_key"
  val MemberLoyaltyIDColumn = "loyalty_id"
  val MatchTypeKeyColumn = "match_key"
  val MCDCriteriaColumn = "mcd_criteria"
  val SourceKeyColumn = "source_key"
  val RecordInfoKeyColumn = "record_info_key"
  val BatchIDColumn = "batch_id"
  val ETLUniqueIDColumn = "etl_unique_id"
  val LastUpdatedDateColumn = "last_updated_date"

  /* Customer Matching Match Type Keys */
  object MatchTypeKeys {
    val EmployeeId = 20
    val LoyaltyId = 4
    val EanRid = 8
    val EmailName = 13
    val PhoneName = 11
    val NameAddress = 7
    val Phone = 12
    val EmailReverseAppend = 16
    val PlccLookupQueue = 90
    val Plcc = 2
    val BankCard = 6
    val PhoneEmail = 10
    val PhoneReverseAppend = 15
    val EanMember = 18
    val PlccName = 1
    val BankCardName = 5
    val PhoneEmailName = 9
    val Email = 14
    val EmailReverseAppendQueue = 91
    val PlccPrimary = 19
    val LwAlternateId = 21
    val IpCode = 22
    val PhoneEmailLoyalty = 24
    val PlccNameLoyalty = 25
    val PlccLoyalty = 26
    val PlccEmailPhone = 27
    val PlccEmail = 28
    val PlccPhone = 29
    val EanProspect = 30
    val NoMatch = 92
  }

  /* Input file source keys */
  object SourceKeys {
    val TlogWeb = 5
    val TlogRetail = 6
    val CustomerWeb = 47
    val CustomerRetail = 7
    val PeopleSoft = 144
    val ADS600 = 8
    val ADS400 = 9
    val RentalListExperianMale = 1561
    val RentalListExperianFemale = 1560
    val RentalListVenus = 2112
    val RentalListJcrew = 264
    val RentalListAnthemFile=2199
    val MobileConsent: Int = -1
    val MemberEmailUpdate: Int = -1
    val LoyaltyWarePOS = 256
    val LoyaltyWareEcom = 257
  }

  /* Customer matching file types */
  object CustomerMatchFileType {
    val PeopleSoft = "peoplesoft"
    val TlogCustomer = "tlog_customer"
    val ADS600 = "ads_600"
    val ADS400 = "ads_400"
    val RentalListVenus = "rentallist_venus"
    val RentalListExperian = "rentallist_experian"
    val RentalListJcrew = "rentallist_jcrew"
    val RentalListAnthemFile = "rentallist_anthem"
    val MobileConsent = "ir_mobile_consent"
    val MemberEmailUpdate = "member_email_update"
    val DMA = "dma"
    val LoyaltyWare = "bp_member"
  }

  object ThirdPartyType {
    val ExperianRea = "rea_experian"
    val AcxiomRea = "rea_acxiom"
    val ComprehensiveHygiene = "chygiene"
    val Rpa_experian = "rpa_experian"
    val Rpa_acxiom = "rpa_acxiom"
  }

  object EDW {
    val CustomerFeed = "edw_customer"
    val HouseHoldFeed = "edw_household"
    val transactionDetailFeed = "edw_transaction_detail"
    val TransactionSummaryFeed = "edw_transaction_summary"
    val ConstantsFeed = "edw_constants"
    val LookupLoyaltyFeed = "edw_lkp_source"
    val LookupScoringModelFeed = "edw_lkp_scoring_model"
    val LookupModelSegmentFeed = "edw_lkp_scoring_model_segment"
    val LookupTransactionTypeFeed = "edw_lkp_transaction_type"
    val LookupDiscountTypeFeed = "edw_lkp_discount_type"
    val CustomerDedupe = "edw_customer_dedup_mtx"
    val CustomerDedupeTrnx = "edw_customer_dedup_txn_mtx"
  }
  /* Common column aliases to be used for customer matching process */
  object MatchColumnAliases {
    val EmailAddressAlias = "email_address"
    val PhoneAlias = "phone"
    val TransactionIdAlias = "transaction_id"
    val CapturedLoyaltyIdAlias = "captured_loyalty_id"
    val ImpliedLoyaltyIdAlias = "implied_loyalty_id"
    val CardAlias = "tender_number"
    val CardTypeAlias = "tender_type"
    val EmployeeIdAlias = "employee_id"
    val NamePrefixAlias = "name_prefix"
    val FirstNameAlias = "first_name"
    val MiddleInitialAlias = "middle_initial"
    val LastNameAlias = "last_name"
    val NameSuffixAlias = "name_suffix"
    val ApartmentNumberAlias = "apartment_number"
    val AddressLineOneAlias = "address_line_1"
    val AddressLineTwoAlias = "address_line_2"
    val CityAlias = "city"
    val CountryAlias = "country"
    val StateCodeAlias = "state_code"
    val StateNameAlias = "state"
    val ZipCodeAlias = "zip_code"
    val ZipPlusFourAlias = "zip_plus_4"
    val NonUSAPostalCodeAlias = "non_usa_postal_code"
    val CompanyNameAlias = "company_name"
    val MobileAlias = "mobile"
  }

  // Minimum Customer Definition Columns
  trait MCDTrait {

    import MatchColumnAliases._

    val NameColumns = Seq(FirstNameAlias, LastNameAlias)
    val USAddressColumns = Seq(AddressLineOneAlias, ZipCodeAlias)
    val CanadaAddressColumns = Seq(AddressLineOneAlias, NonUSAPostalCodeAlias)
    val PhoneColumn: String = PhoneAlias
    val CardColumn: String = CardAlias
  }

  object MCDColumns extends MCDTrait

  object RecordInfoColumns extends MCDTrait {

    import MatchColumnAliases._

    val CardTypeColumn: String = CardTypeAlias
    val EmailColumn: String = EmailAddressAlias
    val MobileColumn: String = MobileAlias
    val LoyaltyColumn: String = ImpliedLoyaltyIdAlias
  }


  // Express Spark-hive context, extend to use
  trait CDWContext {
    private val sc = new SparkContext(Settings.sparkConf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    hiveContext.setConf("spark.sql.hive.convertMetastoreOrc", "false")
    val goldDB: String = Settings.getGoldDB
    val workDB: String = Settings.getWorkDB
    val backUpDB: String = Settings.getBackupDB
    val smithDB: String = Settings.getSmithDB
  }

  // Express CDW required input arguments
  trait CDWOptions {

    private val options = new Options
    val BatchIdOption = "batch_id"

    /**
      * Add program Option
      *
      * @param option Option name
      */
    def addOption(option: String, required: Boolean = true): Unit = {
      val newOption = OptionBuilder.create(option)
      newOption.setRequired(required)
      newOption.setArgs(1)
      options.addOption(newOption)
    }

    // Add default 'batch_id' argument
    addOption(BatchIdOption)

    /**
      * Parses the command line options and returns a Map of arguments and value
      *
      * @param args input arguments
      * @return [[Map]]
      */
    def parse(args: Array[String]): Map[String, String] = {
      val cmdLine = new BasicParser().parse(options, args, true)
      cmdLine.getOptions.map(option => option.getOpt.trim -> option.getValue.trim).toMap
    }
  }


  /*
   * Common Functions
   */

  /**
    * File Date Function to extract file data from ETLUniqueIDColumn
    *
    * @param uniqueId ETL Unique ID
    * @return File Date
    */
  def fileDateFunc(uniqueId: String): Date = {
    val DateRegex = """.*-(\d{4})(\d{2})(\d{2})""".r
    val fileDate = uniqueId match {
      case DateRegex(y, m, d) => s"$y-$m-$d"
      case _ => "1970-01-01" /* default date if not present in ETLUniqueIDColumn, will rank randomly */
    }
    Date.valueOf(fileDate)
  }

  /*
   * Distance Travelled
   */

  /**
    * File Date Function to extract file data from ETLUniqueIDColumn
    *
    * @param lat1
    * @return Distance
    */
  def distanceTravelledFunc(lat1: Double, lat2: Double, lon1: Double, lon2: Double, el1: Double = 0, el2: Double = 0): Double = {
    val R = 6371
    val latDistance = Math.toRadians(lat2 - lat1)
    val lonDistance = Math.toRadians(lon2 - lon1)
    val a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    val distance_temp = R * c * 1000
    val height = el1 - el2
    val distance = Math.pow(distance_temp, 2) + Math.pow(height, 2)
    /*Converting meters to miles*/
    Math.sqrt(distance) * 0.000621371
  }

  //case class Store(store_key:Long,latitude: BigDecimal ,longitude: BigDecimal)

  /**
    * Find closest and second shortest
    *
    * @param lat1 member latitude
    * @param lon1  member longitute
    * @param storeEle
    * @return
    */
  def closestStoreKey(lat1: BigDecimal, lon1: BigDecimal, storeEle: Seq[Store]): Row = {

    if (lat1 == null || lon1 == null)
      return Row.fromSeq(Seq(null, null, null, null))


    var minStoreKey: Long = storeEle.head.store_key
    var secondMinStoreKey: Long = storeEle(1).store_key
    var shortestDist = distanceTravelledFunc(lat1.doubleValue(), storeEle.head.latitude.doubleValue(), lon1.doubleValue(), storeEle.head.longitude.doubleValue())
    var secondShortestDist = distanceTravelledFunc(lat1.doubleValue(), storeEle(1).latitude.doubleValue(), lon1.doubleValue(), storeEle(1).longitude.doubleValue())

    if (shortestDist > secondShortestDist) {
      minStoreKey = storeEle(1).store_key
      secondMinStoreKey = storeEle.head.store_key
    }

    for (ele <- 2 until  storeEle.length) {
      var current = distanceTravelledFunc(lat1.doubleValue(), storeEle(ele).latitude.doubleValue(), lon1.doubleValue(), storeEle(ele).longitude.doubleValue())
      if (current < shortestDist) {
        secondMinStoreKey = minStoreKey
        secondShortestDist = shortestDist
        minStoreKey = storeEle(ele).store_key
        shortestDist = current

      }
      else if (current < secondShortestDist) {
        secondMinStoreKey = storeEle(ele).store_key
        secondShortestDist = current
      }
    }
    Row.fromSeq(Seq[Any](minStoreKey, secondMinStoreKey, shortestDist, secondShortestDist))
  }

  /**
    * Check if the column value is not empty
    *
    * @param value column value
    * @return [[Boolean]]
    */
  def isNotEmpty(value: Any): Boolean = {
    Option(value) match {
      case None => false
      case Some(str: String) => str.trim.nonEmpty
      case Some(_) => true
    }
  }

  /**
    * Check if the email contains invalid characters
    *
    * @param email column value
    * @return [[Boolean]]
    */

  def validEmailFlag(email: String): Boolean = {
    val invalid_chars = Seq("!", "#", "$", "%", "&", "'", "*", "+", "-", "/", "=", "?", "^", "`")
    invalid_chars.exists(email.contains)
  }

  /**
    * Converts String to Long
    *
    * @param str Input string
    * @return Long
    */
  def strToLong(str: String): Long = Option(str) match {
    case None => 0L
    case Some("") => 0L
    case Some(id) => id.toLong
  }

  /**
    * Remove Square Brackets from the member_key_tuple
    *
    * @param member_key_tuple Input string
    * @return String
    */
  def remove_brackets(member_key_tuple: String): String = {
    if (member_key_tuple == null)
      null
    else
      member_key_tuple.replaceAll("[\\[\\]]", "").trim
  }


  def strToDate(str: String, dateFormat: String): Date = {
    Option(str) match {
      case None => null
      case Some(date) =>
        val javaDate = DateTime.parse(date, DateTimeFormat.forPattern(dateFormat)).getMillis
        new Date(javaDate)
    }
  }

    def latitudeLongitudeConv(str: String): Double = {
      if (str.takeRight(1) == "N" || str.takeRight(1) == "E")
      {
        val value1 = str.dropRight(1).toDouble
        value1/1000000
      }
      else
        {
          val value2 = str.dropRight(1).toDouble
          -1 * value2/1000000

        }
      }

  def sortMKsString (str : String) : String ={
    str.split(",").map(_.toLong).sorted.mkString(",")
  }

}
