package com.express.cm.criteria

import com.express.cdw._
import org.apache.spark.sql.DataFrame

/**
  * Base Trait that should be implemented by all Customer Matching Functions
  *
  * @author mbadgujar
  */
trait MatchTrait {

  val CMMetaColumns = List(MatchStatusColumn, MatchTypeKeyColumn, MemberKeyColumn)

  // Alias used during matching
  val SourceAlias = "source"
  val LookUpAlias = "lookup"

  // Formatted Columns
  val FormattedNameColumn = "formatted_name"
  val FormattedAddressColumn = "formatted_address"
  val FormattedEmailColumn = "formatted_email"
  val FormattedPhoneColumn = "formatted_phone_number"
  /**
    * Customer Matching process function.
    * The function applies the match logic, and updates the [[MatchStatusColumn]] to true if match if found
    * on the data and also updates the [[MemberKeyColumn]] in the dataframe.
    *
    * @param source The source [[DataFrame]] on which matching process is to ran
    * @return Updated [[DataFrame]]
    */
  def matchFunction(source: DataFrame): DataFrame

}
