package com.express.cm.criteria

import com.express.cdw.RecordInfoColumns._
import com.express.cdw._
import com.express.cdw.spark.DataFrameUtils._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}


/**
  * Contains logic for assigning the record info key to the Customer matched input records.
  *
  * @author mbadgujar
  */
object RecordInfo extends LazyLogging {

  private val Separator = "::"
  private val IsPLCCColumn = "is_express_plcc"
  private val IsBankCardColumn = "is_credit_card"
  private val RecordInfoStrColumn = "record_info_str"
  private val RecordInfoKeyColumn = "record_info_key"

  // Columns used for checking record information
  private val RecordInfoColumns = NameColumns ++ (USAddressColumns ++ CanadaAddressColumns).distinct :+ EmailColumn :+
    PhoneColumn :+ MobileColumn :+ CardColumn :+ CardTypeColumn :+ LoyaltyColumn :+ MemberKeyColumn


  /**
    * Check if the column value is not empty
    *
    * @param value column value
    * @return [[Boolean]]
    */
  private def isNotEmpty(value: Any) = {
    Option(value) match {
      case None => false
      case Some(str) => str.toString.trim.nonEmpty
    }
  }

  /**
    * Get the column value from provided [[Row]] and row columns
    *
    * @param row        Input [[Row]]
    * @param rowColumns Column [[Seq]]
    * @param column     column value to extract
    * @tparam T Column type
    * @return Column value
    */
  private def getAs[T](row: Row, rowColumns: Seq[String])(column: String): T = row.getAs[T](rowColumns.indexOf(column))


  /**
    * Get the info function for "Card" type columns
    *
    * @param cardType card type column
    * @return Func for checking column info
    */
  private def cardCheck(cardType: String): (Row, Seq[String]) => Boolean = (row: Row, rowColumns: Seq[String]) => {
    val cardColumns = Seq(CardColumn, CardTypeColumn)
    cardColumns.forall(rowColumns.contains) &&
      cardColumns.map(getAs[String](row, rowColumns)).forall(isNotEmpty) &&
      getAs[String](row, rowColumns)(cardType) == "YES"
  }

  /**
    * Get the record info function for single columns
    *
    * @param column Column to check
    * @return Func for checking column info
    */
  private def singleColumnCheck(column: String) = (row: Row, rowColumns: Seq[String]) => {
    rowColumns.contains(column) && isNotEmpty(getAs[String](row, rowColumns)(column))
  }


  /*
     Individual Column functions for checking record information
   */
  private val emailCheck = singleColumnCheck(EmailColumn)
  private val phoneCheck = singleColumnCheck(PhoneColumn)
  private val mobileCheck = singleColumnCheck(MobileColumn)
  private val loyaltyCheck = singleColumnCheck(LoyaltyColumn)

  private val nameCheck = (row: Row, rowColumns: Seq[String]) => {
    NameColumns.forall(rowColumns.contains(_)) &&
      NameColumns.map(getAs[String](row, rowColumns)).exists(isNotEmpty)
  }

  private val addressCheck = (row: Row, rowColumns: Seq[String]) => {
    (USAddressColumns.forall(rowColumns.contains(_)) && USAddressColumns.map(getAs[Any](row, rowColumns)).exists(isNotEmpty)) ||
      (CanadaAddressColumns.forall(rowColumns.contains(_)) && CanadaAddressColumns.map(getAs[Any](row, rowColumns)).exists(isNotEmpty))
  }

  private val mcdCheck = (row: Row, rowColumns: Seq[String]) => getAs[Long](row, rowColumns)(MemberKeyColumn) != -1


  /*
   *
   *  UDF for checking record info, Concatenates "YES" if info is present & "NO" if not
   */
  private val recordInfoUDF = udf((row: Row, rowColumnsStr: String) => {
    val rowColumns = rowColumnsStr.split(Separator)
    val recordInfoChecks = Seq(nameCheck, addressCheck, emailCheck, phoneCheck, mobileCheck, cardCheck(IsPLCCColumn),
      cardCheck(IsBankCardColumn), loyaltyCheck, mcdCheck)
    recordInfoChecks.foldLeft("") {
      case (recordInfoStr, recordCheckFunc) =>
        recordInfoStr.concat(if (recordCheckFunc(row, rowColumns)) "YES" else "NO")
    }
  })


  /**
    * Assigns Record info key to Customer matched Input records
    *
    * @param cmOutput [[DataFrame]] containing Customer matching output
    * @return [[DataFrame]] with record info key assigned
    */
  def assignRecordInfoKey(cmOutput: DataFrame): DataFrame = {

    val recordInfoColumns = cmOutput.columns.filter(RecordInfoColumns.contains(_))

    val sqlContext = cmOutput.sqlContext
    import sqlContext.implicits._
    val goldDB = Settings.getGoldDB

    val cardTypeDf = sqlContext.table(s"$goldDB.dim_card_type").filter("status = 'current'")
      .select($"tender_type_code".as(CardTypeColumn), $"$IsBankCardColumn", $"$IsPLCCColumn")

    // Get the card info, if card columns are present
    val (recordInfoInputDF, recordInfoColumnsInData) = if (recordInfoColumns.contains(CardColumn) &&
      recordInfoColumns.contains(CardTypeColumn))
      (cmOutput.join(cardTypeDf, Seq(CardTypeColumn), "left"), recordInfoColumns :+ IsBankCardColumn :+ IsPLCCColumn)
    else
      (cmOutput, recordInfoColumns)

    val recordInfoColumnsInDataStr = recordInfoColumnsInData.mkString(Separator)
    logger.info("Recod info columns in input source file: {}", recordInfoColumnsInDataStr)

    // Get the record info
    val recordInfoAssignedDF = recordInfoInputDF.withColumn(RecordInfoStrColumn, recordInfoUDF(struct(recordInfoColumnsInData.head,
      recordInfoColumnsInData.tail: _*), lit(recordInfoColumnsInDataStr)))

    // Join with record info dim to get record info key
    val recordInfoDim = sqlContext.table(s"$goldDB.dim_record_info").filter("status = 'current'")
      .dropColumns(Seq("status", "batch_id", "last_updated_date"))
    val recordInfoDimWithKey = recordInfoDim
      .withColumn(RecordInfoStrColumn, concat(recordInfoDim.columns.tail.map(col => upper(trim($"$col"))): _*))
      .select(RecordInfoKeyColumn, RecordInfoStrColumn)

    recordInfoAssignedDF.join(recordInfoDimWithKey, Seq(RecordInfoStrColumn), "left").na.fill(-1, Seq(RecordInfoKeyColumn))
  }


}
