package com.express.processing.fact

/**
  * Created by amruta.chalakh on 6/14/2017.
  */

import com.express.cdw.spark.DataFrameUtils._
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.{DataFrame, SaveMode}

object FactWaterfallSummary extends FactLoad {

  override def factTableName = "fact_waterfall_summary"

  override def surrogateKeyColumn = ""

  override def transform: DataFrame  = {
    val customerMatchingADS400 = hiveContext.sql(s"select source_key,record_info_key,match_key as match_type_key,mcd_criteria,count(*) as number_matches " +
      s"from $workDB.work_ads_400_cm  where batch_id = '$batchId' group by source_key,record_info_key,match_key,mcd_criteria")

    val customerMatchingADS600 = hiveContext.sql(s"select source_key,record_info_key,match_key as match_type_key,mcd_criteria,count(*) as number_matches " +
      s"from $workDB.work_ads_600_cm  where batch_id = '$batchId'   group by source_key,record_info_key,match_key,mcd_criteria")

    val customerMatchingEmSubUnsub = hiveContext.sql(s"select source_key,record_info_key,match_key as match_type_key,mcd_criteria,count(*) as number_matches " +
      s"from $workDB.work_member_email_update_cm  where batch_id = '$batchId'  group by source_key,record_info_key,match_key,mcd_criteria")

    val customerMatchingPeoplesoft = hiveContext.sql(s"select source_key,record_info_key,match_key as match_type_key,mcd_criteria,count(*) as number_matches " +
      s"from $workDB.work_peoplesoft_cm  where batch_id = '$batchId'  group by source_key,record_info_key,match_key,mcd_criteria")

    val customerMatchingPersioMb = hiveContext.sql(s"select source_key,record_info_key,match_key as match_type_key,mcd_criteria,count(*) as number_matches " +
      s"from $workDB.work_ir_mobile_consent_cm  where batch_id = '$batchId'  group by source_key,record_info_key,match_key,mcd_criteria")

    val customerMatchingRentalExp = hiveContext.sql(s"select source_key,record_info_key,match_key as match_type_key,mcd_criteria,count(*) as number_matches " +
      s"from $workDB.work_rentallist_experian_cm  where batch_id = '$batchId'  group by source_key,record_info_key,match_key,mcd_criteria")

    val customerMatchingRentalJcrew = hiveContext.sql(s"select source_key,record_info_key,match_key as match_type_key,mcd_criteria,count(*) as number_matches " +
      s"from $workDB.work_rentallist_jcrew_cm  where batch_id = '$batchId'  group by source_key,record_info_key,match_key,mcd_criteria")

    val customerMatchingRentalVenus = hiveContext.sql(s"select source_key,record_info_key,match_key as match_type_key,mcd_criteria,count(*) as number_matches " +
      s"from $workDB.work_rentallist_venus_cm  where batch_id = '$batchId'  group by source_key,record_info_key,match_key,mcd_criteria")

    val customerMatchingTlogCustomer = hiveContext.sql(s"""select source_key,record_info_key,match_type_key,mcd_criteria,count(*) as number_matches from
                                                            (select source_key,record_info_key,match_key as match_type_key,mcd_criteria,
                                                                    row_number() over(partition by trxn_id order by match_key asc) as row_num
                                                                    from $workDB.work_tlog_customer_cm
                                                                    where batch_id = '$batchId') tlog_temp
                                                             where tlog_temp.row_num =1
                                                             group by source_key,record_info_key,match_type_key,mcd_criteria""")

    val finalSummary = customerMatchingADS400.unionAll(customerMatchingADS600)
      .unionAll(customerMatchingEmSubUnsub)
      .unionAll(customerMatchingPeoplesoft)
      .unionAll(customerMatchingPersioMb)
      .unionAll(customerMatchingRentalExp)
      .unionAll(customerMatchingRentalJcrew)
      .unionAll(customerMatchingRentalVenus)
      .unionAll(customerMatchingTlogCustomer)

    finalSummary
      .withColumn("last_updated_date", lit(current_timestamp()))
      .withColumn("batch_id", lit(batchId))
  }


  override def load(): Unit = {
    val targetFactColumns = hiveContext.table(tempFactTable).getColumns
    transform.select(targetFactColumns: _*).insertIntoHive(SaveMode.Overwrite, transform, tempFactTable)
  }

  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }
}
