package com.express

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.util.Try

/**
  * Constants, Methods, Case classes and UDFs for Customer Matching process
  *
  * @author mbadgujar
  */
package object cm {

  /*
   *  Constants
   */
  // Minimum customer definition Statuses
  object MCDStatuses {
    val MCDLoyalty = "mcd_loyaltyid"
    val MCDEmail = "mcd_email"
    val MCDNameAddressUS = "mcd_name_address_us"
    val MCDNameAddressCanada = "mcd_name_address_canada"
    val MCDPhone = "mcd_phone"
    val MCDBankCard = "mcd_bankcard"
    val MCDFailed = "mcd_failed"
  }

  /*
   *  Case Classes
   */
  // UDF wrapper
  case class UDF(udf: UserDefinedFunction, columns: Seq[String]) {
    def apply: Column = udf.apply(columns.map(new Column(_)): _*)
  }

  // Standard name format used for matching purpose
  case class NameFormatClass(prefix: String, firstName: String, middleName: String, lastName: String, suffix: String) {
    def this(name: Seq[String]) = this(name.head, name(1), name(2), name(3), name(4))
  }

  // Standard Address format used for matching
  case class AddressFormatClass(address1: String, address2: String, city: String, state: String, zipCode: String, extendedZipCode: String,
                                fullZipCode: String, nonUSPostalCode: String, countryCode: String) {
    def this(address: Seq[String]) = this(address.head, address(1), address(2), address(3), address(4), address(5),
      address(6), address(7), address(8))
  }

  // Check if not null and convert to lower case
  def toLower(string: String): String = Option(string)
    .fold(string)(_.trim.toLowerCase) match {
    case "" => null
    case str => str
  }

  val NameFormat: (String, String, String, String, String) => NameFormatClass =
    (prefix: String, firstName: String, middleName: String, lastName: String, suffix: String) =>
      NameFormatClass(toLower(prefix), toLower(firstName), toLower(middleName), toLower(lastName), toLower(suffix))

  val AddressFormat: (String, String, String, String, String, String, String, String, String) => AddressFormatClass =
    (address1: String, address2: String, city: String, state: String, zipCode: String, extendedZipCode: String,
     fullZipCode: String, nonUSPostalCode: String, countryCode: String) =>
      AddressFormatClass(toLower(address1), toLower(address2), toLower(city), toLower(state), toLower(zipCode),
        toLower(extendedZipCode), toLower(fullZipCode), toLower(nonUSPostalCode), toLower(countryCode))


  /*
   *  Methods
   */

  def formatPhoneNumberUSCan(phoneNumber: String): Long = {
    if (phoneNumber == null) return 0
    val phoneNoWithoutSymbols = phoneNumber.replaceAll("[^0-9]", "")
    Try(phoneNoWithoutSymbols.toLong).getOrElse(0)
  }

  def checkPhoneValidity(phoneNumber: Long): Boolean = {
    val invalidPhone = Seq("0000000001", "1234567890", "1231231234", "1000000000", "0000000123", "2000000000", "0000000002", "8000000000")
    val phoneNumberString = phoneNumber.toString
    (phoneNumberString.length == 10 || (phoneNumberString.length == 11 && phoneNumberString.startsWith("1"))) &&
      phoneNumberString.distinct.length != 1 && !invalidPhone.contains(phoneNumberString)
  }

  /*
   *  User Defined Functions
   */
  // Member Name format UDF
  val MemberNameFormatUDF = UDF(udf(NameFormat(_: String, _: String, "", _: String, _: String)),
    Seq("name_prefix", "first_name", "last_name", "name_suffix"))

  // Member Address Format UDF
  val MemberAddressFormatUDF = UDF(udf(AddressFormat(_: String, _: String, _: String, _: String, _: String, _: String,
    "", _: String, _: String)),
    Seq("address1", "address2", "city", "state", "zip_code", "zip4",
      "non_us_postal_code", "country_code"))


  // Format Email UDF
  //val formatEmailUDF: UserDefinedFunction = udf[String, String]((str: String) => str.toLowerCase)
  val formatEmailUDF: UserDefinedFunction = udf((str: String) => Option(str) match {
    case None => ""
    case Some(string) => string.toLowerCase.trim
  })
  // Format Phone UDF
  val formatPhoneNumber: UserDefinedFunction = udf[Long, String](formatPhoneNumberUSCan)


}