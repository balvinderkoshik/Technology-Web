package com.express.cm

import com.express.cdw.{MatchStatusColumn, MatchTypeKeyColumn, MemberKeyColumn}
import com.express.cdw.test.utils.DataUtil._
import com.express.cdw.test.utils.SparkUtil
import com.express.cm.lookup.LookUpTable._
import com.express.cm.lookup.LookUpTableUtil
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Test Spark Context to be used for Customer Matching unit tests
  *
  * @author mbadgujar
  */
object TestSparkContext {

  System.setProperty("cdw_config.gold_db", "internal")
  private val sqlContext = SparkUtil.getHiveContext

  def getSQLContext: SQLContext = sqlContext
  sqlContext.udf.register[Boolean, String]("check_phone_validity", (phone: String) => checkPhoneValidity(formatPhoneNumberUSCan(phone)))

  val CMColumns: DataFrame => DataFrame = (df: DataFrame) => {
    df.withColumn(MatchStatusColumn, lit(false))
      .withColumn(MatchTypeKeyColumn, lit(""))
      .withColumn(MemberKeyColumn, lit(null))
  }

  private def file(name: String) = this.getClass.getClassLoader.getResource(s"./data/$name").getPath

  sqlContext
    .getDfFromCSV(file("employee.tsv"), "dim_employee", Seq("employee_id", "member_key", "status"))
    .registerTempTable(EmployeeDimensionLookup.getQualifiedTableName)

  sqlContext
    .getDfFromCSV(file("member.tsv"), "dim_member", Seq("member_key", "first_name", "last_name", "loyalty_id", "address1",
      "address2", "zip_code", "status"))
    .registerTempTable(MemberDimensionLookup.getQualifiedTableName)

  sqlContext
    .getDfFromCSV(file("card_type.tsv"), "dim_card_type", Seq("card_type_key", "is_express_plcc", "status"))
    .registerTempTable(CardTypeDimensionLookup.getQualifiedTableName)

  sqlContext
    .getDfFromCSV(file("member_email.tsv"), "dim_member_multi_email", Seq("email_address", "member_key", "valid_email", "status", "is_loyalty_email"))
    .registerTempTable(MemberMultiEmailDimensionLookup.getQualifiedTableName)

  sqlContext
    .getDfFromCSV(file("member_phone.tsv"), "dim_member_multi_phone", Seq("phone_number", "member_key", "status", "is_loyalty_flag"))
    .registerTempTable(MemberMultiPhoneDimensionLookup.getQualifiedTableName)

  sqlContext
    .getDfFromCSV(file("tender_type.tsv"), "dim_tender_type", Seq("tender_type_key", "tender_type_code", "is_bank_card", "status"))
    .registerTempTable(TenderTypeDimensionLookup.getQualifiedTableName)

  sqlContext
    .getDfFromCSV(file("time.tsv"), "dim_time", Seq("time_key", "time_in_24hr_day", "status"))
    .registerTempTable(TimeDimensionLookup.getQualifiedTableName)

  sqlContext
    .getDfFromCSV(file("transaction_detail.tsv"), "fact_transaction_detail", Seq("trxn_date", "time_in_24hr_day", "member_key"))
    .registerTempTable(TransactionDetailFactLookup.getQualifiedTableName)

  sqlContext
    .getDfFromCSV(file("tender_hist.tsv"), "fact_tender_history", Seq("tokenized_cc_nbr", "tender_type_key", "member_key"))
    .registerTempTable(TenderHistoryFactLookup.getQualifiedTableName)

  sqlContext
    .getDfFromCSV(file("card_hist.tsv"), "fact_card_history", Seq("tokenized_cc_nbr", "card_type_key", "member_key",
      "is_primary_account_holder"))
    .registerTempTable(CardHistoryFactLookup.getQualifiedTableName)


  // Init the Lookup Util for test
  LookUpTableUtil.init(sqlContext)


}
