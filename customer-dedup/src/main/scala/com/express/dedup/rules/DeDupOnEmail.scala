package com.express.dedup.rules

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
/**
  * Created by aman.jain on 6/28/2017.
  */
object DeDupOnEmail extends DeDupResolutionTrait {

  override def groupingCriteriaId: Int = 5

  override def groupingIdentificationRule: Column = expr("(first_name is null or empty(first_name) or first_name='UNKNOWN' )" +
    " and (last_name is null or empty(last_name) or last_name='UNKNOWN') " +
    "and (address1_scrubbed is null or empty(address1_scrubbed) or address1_scrubbed='UNKNOWN') " +
    "and (zip_code_scrubbed is null or empty(zip_code_scrubbed) or zip_code_scrubbed='UNKNOWN') " +
    "and (size(bank_card) = 0 or bank_card is null ) " +
    "and (size(dedup_phone_number) = 0 or dedup_phone_number is null )" +
    "and size(dedup_email_address) > 0")

  override def groupingColumns: Seq[String] = Seq("dedup_email_address")
}
