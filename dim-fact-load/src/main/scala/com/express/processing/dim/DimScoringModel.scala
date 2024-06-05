package com.express.processing.dim

import com.express.cdw.CDWContext
import com.express.cdw.spark.DataFrameUtils._
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{current_timestamp, lit, _}
import org.apache.spark.sql.types.{DateType, LongType, TimestampType}

/**
  * Created by aditi.chauhan on 10/10/2017.
  */
object DimScoringModel extends CDWContext {

  def main(args: Array[String]): Unit = {

    val batchId = args(0)
    val outputTable = s"$goldDB.dim_scoring_model"


    /*Fetching date fields in required format*/

    val date = Calendar.getInstance.getTime

    val year_month_format = new SimpleDateFormat("yyyy-MM")
    val year_month = year_month_format.format(date)

    val current_dt_format = new SimpleDateFormat("yyyy-MM-dd")
    val current_dt = current_dt_format.format(date)


    /*Fetching the first fiscal Wednesday on which monthly scoring would be done*/

    /*val firstFiscalWed_Control = hiveContext.sql(s"""select a.sdate from
                                                (select cast(sdate as string) as sdate,row_number() over(partition by day_of_week_name order by sdate) as row_num
                                                 from $goldDB.dim_date where status='current'
                                                 and day_of_week_name = 'TUESDAY'
                                                 and sdate like '$year_month%')a where a.row_num =2""")
                                                 .rdd.map(r => r(0).asInstanceOf[String])
                                                 .collect.head*/

    val firstFiscalWed = hiveContext.sql(s"""select a.sdate from
                                             (select cast(sdate as string) as sdate,row_number() over(partition by day_of_week_name order by sdate) as row_num
                                              from $goldDB.dim_date where status='current'
                                              and day_of_week_name = 'WEDNESDAY'
                                              and fiscal_month_begin_date like '$year_month%'
                                              )a where a.row_num =1""")
      .rdd.map(r => r(0).asInstanceOf[String])
      .collect.head

    /****************************************Checking if the current date is equal to the first Wednesday of the month*****************************************/

    if (firstFiscalWed == current_dt) {

      val lastFiscalSat = hiveContext.sql(s"""select cast(date_sub(to_date(fiscal_month_begin_date),1) as string) from $goldDB.dim_date
                                              where sdate = '$current_dt' and status='current'""")
        .rdd.map(r => r(0).asInstanceOf[String])
        .collect.head

      /*Fetching the data for dim_scoring_model */


      val dimScoringModelDF = hiveContext.sql(s"select * from $goldDB.dim_scoring_model")


      /********************************************Fetching the maximumm surrogate key for Indivisual and Household Model****************************************/

      val maxSurrogateKey_Ind = hiveContext.sql(s"""select (max(scoring_model_key)+1) as scoring_model_key
                                                     from $goldDB.dim_scoring_model where status='current' """)
        .withColumn("model_id", lit(104)).persist()


      val maxSurrogateKey_HH = hiveContext.sql(s"""select (max(scoring_model_key)+2) as scoring_model_key
                                                    from $goldDB.dim_scoring_model where status='current' """)
        .withColumn("model_id", lit(108)).persist()

      val dimScoringModel = maxSurrogateKey_Ind.unionAll(maxSurrogateKey_HH)

      /***********************************************Fetching the count for Indivisual and Household model from fact_scoring_history***************************************/

      val count_factScoringHistory_Ind = hiveContext.sql(s"""select count(*) from $goldDB.fact_scoring_history where batch_id = $batchId
                                                             and member_key is not null and segment_rank != 0 and segment_rank is not null """)
        .rdd.map(r => r(0).asInstanceOf[Long])
        .collect.head



      val count_factScoringHistory_HH = hiveContext.sql(s"""select count(*) from $goldDB.fact_scoring_history where batch_id = $batchId
                                                             and member_key is null and segment_rank != 0 and segment_rank is not null""")
        .rdd.map(r => r(0).asInstanceOf[Long])
        .collect.head

      /**********************************Creating the Current Partition DF for model_id in (104,108)*********************************************************************/

      val insertCurrentDF = dimScoringModelDF
        .filter("status = 'current' and (model_id = 104 or model_id = 108) and upper(trim(current_model_ind)) = 'YES'")
        .selectExpr("model_id", "model_version_id + 1 as model_version_id ", "model_source",
          "marketing_brand_model_code", "model_description", "model_level_code",
          "model_level", "lbi_segment_processed_flag", "current_model_ind")
        .withColumn("effective_date", lit(firstFiscalWed).cast(TimestampType))
        .withColumn("expiration_date", to_date(lit("2099-12-31")).cast(TimestampType))
        .withColumn("scored_record_count", when(col("model_id") === 104,count_factScoringHistory_Ind).otherwise(count_factScoringHistory_HH))
        .withColumn("data_through_date", lit(lastFiscalSat).cast(DateType))
        .join(dimScoringModel,Seq("model_id"),"left")
        .withColumn("last_updated_date", current_timestamp)
        .withColumn("batch_id", lit(batchId))
        .withColumn("status", lit("current"))
        .select(dimScoringModelDF.getColumns: _*)

      /*Removing the previous values from current partition and inserting new ones*/

      val currentPartitionDF = dimScoringModelDF
        .filter("status = 'current' and model_id not in (108,104)")
        .unionAll(insertCurrentDF)
        .withColumn("batch_id", lit(batchId))


      /*********************************************************Creating the History Partition DF*******************************************************************/

      val InsertHistoryDF = dimScoringModelDF
        .filter("status = 'history' and (model_id = 104 or model_id = 108) and upper(trim(current_model_ind)) ='YES'")
        .drop(col("current_model_ind"))
        .drop(col("expiration_date"))
        .withColumn("current_model_ind", lit("NO"))
        .withColumn("expiration_date", date_sub(lit(firstFiscalWed).cast(DateType), 1).cast(TimestampType))
        .select(dimScoringModelDF.getColumns: _*)

      val historyPartitionDF = InsertHistoryDF
        .unionAll(dimScoringModelDF.filter("status='history'")
          .except(dimScoringModelDF.filter("status = 'history' and (model_id = 104 or model_id = 108) and upper(trim(current_model_ind)) ='YES'")))
        .unionAll(insertCurrentDF.drop(col("status")).withColumn("status", lit("history")))
        .withColumn("batch_id", lit(batchId))

      /********************************************Overwriting the data in current and history partitions**********************************************************/


      historyPartitionDF.insertIntoHive(SaveMode.Overwrite, historyPartitionDF, outputTable, Some("status"))
      currentPartitionDF.insertIntoHive(SaveMode.Overwrite, currentPartitionDF, outputTable, Some("status"))


    }
    else {
      println("Exiting the spark script as the process should occur on the first Wednesday of each month")
      sys.exit(2)

    }
  }
}