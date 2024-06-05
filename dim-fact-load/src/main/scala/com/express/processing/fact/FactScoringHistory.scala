package com.express.processing.fact

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.express.cdw.spark.DataFrameUtils._
import org.apache.spark.sql.types._

/**
  * Created by Mahendranadh.Dasari/Aditi Chauhan on 20-07-2017.
  */
object FactScoringHistory extends FactLoad {

  override def factTableName = "fact_scoring_history"

  override def surrogateKeyColumn = "scoring_history_id"

  override def transform: DataFrame = {

    /*Fetching date fields in required format*/
    val date = Calendar.getInstance.getTime

    val year_month_format = new SimpleDateFormat("yyyy-MM")
    val year_month = year_month_format.format(date)

    val current_dt_format = new SimpleDateFormat("yyyy-MM-dd")
    val current_dt = current_dt_format.format(date)

    val targetFactColumns = hiveContext.table(tempFactTable)
      .getColumns
      .filter(_!=col("scoring_history_id"))

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



    /*Creating the Dataframe for inserting new customer records daily into gold.fact_scoring_history*/

    /*Identifying new customer records by joining dim_member and fact_scoring_history table*/

    val dimMemberDataset = hiveContext.sql(s"""select distinct member_key from $goldDB.dim_member where status='current'""")

    val factScoringHistoryDataset = hiveContext.sql(s"""select distinct member_key as member_key_scoring from $goldDB.fact_scoring_history""")

    val sourceDataset = dimMemberDataset.join(factScoringHistoryDataset,col("member_key") === col("member_key_scoring"),"left")
                                        .filter(isnull(col("member_key_scoring")))
                                        .select("member_key")

    /* Fetching the dataset for dim_scoring_model_segment for the active records against model_id=104*/

    val DimScoringSegmentDataset = hiveContext.sql(s"""select cast(segment_rank as string),scoring_model_segment_key,model_id,segment_type,var_channel,var_plcc_holder
                                                   from $goldDB.dim_scoring_model_segment
                                                   where status='current'
                                                   and model_id in (104,108)
                                                   and segment_in_use = 'YES'
                                                   and segment_grouping_rank is not null""")

    /*Fetching the dataset for dim_household*/

    val dimHousehold = hiveContext.sql(s"""select household_key,associated_member_key
                                       from $goldDB.dim_household where status='current'""")
                                  .withColumn("member_key", explode(split(col("associated_member_key"), ",")))

    /*Fetching the dataset for dim_scoring_model for the active records against model_id = 104.
    The if condition is added as the scoring_model_key will be incremented by 1 on the first fiscal Wednesday every month for model_id =104 and by 2 for model_id =108 */

    val dimScoringModel = if(firstFiscalWed == current_dt )
    {
      hiveContext.sql(
        s"""select (max(scoring_model_key)+1) as scoring_model_key,(max(scoring_model_key)+2) as scoring_model_key_hh
                                  from $goldDB.dim_scoring_model where status='current' """)
        .withColumn("model_id", lit(104))
        .withColumn("model_id_hh",lit(108))
    }
    else
    {
      hiveContext.sql(s"""select scoring_model_key,model_id
                                          from $goldDB.dim_scoring_model
                                          where model_id in (104,108) and trim(upper(current_model_ind))='YES'
                                          and status='current'""")

    }


    /*Creating the final transformed dataset for inserting new customers*/

    val joinedTransform_Daily = sourceDataset.join(dimHousehold, Seq("member_key"), "left")
      .select("household_key", sourceDataset.columns: _*)
      .withColumn("primary_customer_rid", lit(null))
      .withColumn("rid", lit(null))
      .withColumn("raw_score", lit(null))
      .withColumn("segment_rank", lit(15))
      .withColumn("segmented_score", lit(null))
      .join(broadcast(DimScoringSegmentDataset).filter("model_id =104"),Seq("segment_rank"),"left")
      .withColumn("segment_type", lit("New30+Days1Trip"))
      .join(broadcast(dimScoringModel).filter("model_id =104"),Seq("model_id"),"left")
      .withColumnRenamed("var_channel", "channel")
      .withColumnRenamed("var_plcc_holder", "plcc_holder")
      .withColumn("scoring_date", current_date)
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_updated_date", current_timestamp)
      .select(targetFactColumns: _*)

    joinedTransform_Daily

    /*Checking if the current date is the first fiscal wednesday of the month*/

    if(firstFiscalWed == current_dt)

    {

      import hiveContext.implicits._

      val sourceDataset = SegmentRank.SegmentRankCalculator(hiveContext)

      val joinedsrcWithScoringModel = sourceDataset
        .join(dimScoringModel, Seq("model_id"), "left")
        .select("scoring_model_key", sourceDataset.columns: _*)

      logger.debug("joinedsrcWithScoringModel:" + joinedsrcWithScoringModel.count().toString)

      /*Creating the monthly+daily dataframe for records to be inserted on the first fiscal Wednesday of each month*/

      val joinedTransform_Monthly = joinedsrcWithScoringModel.join(dimHousehold, Seq("member_key"), "left")
        .select("household_key", joinedsrcWithScoringModel.columns: _*)
        .withColumnRenamed("var_channel", "channel")
        .withColumnRenamed("var_plcc_holder", "plcc_holder")
        .withColumn("primary_customer_rid", lit(null))
        .withColumn("rid", lit(null))
        .withColumn("raw_score", lit(null))
        .withColumn("segmented_score", lit(null))
        .withColumn("scoring_date", lit(firstFiscalWed).cast(DateType))
        .withColumn("batch_id", lit(batchId))
        .withColumn("last_updated_date", current_timestamp)
        .select(targetFactColumns: _*)

      joinedTransform_Monthly.registerTempTable("joinedTransform_Monthly")

      /*Creating Dataframe for Inserting records in fact_scoring_history for Household Model -Frequency Monthly*/


     val Household_Scoring_MonthlyDF = hiveContext.sql(s"""select household_key,min(segment_rank) as segment_rank
                                                          from joinedTransform_Monthly as temp
                                                          where segment_rank is not null and member_key is not null and household_key is not null
                                                          group by household_key""")



      val Household_Scoring_Monthly = Household_Scoring_MonthlyDF
                                      .withColumn("member_key",lit(null))
                                      .withColumn("primary_customer_rid",lit(null))
                                      .withColumn("rid",lit(null))
                                      .withColumn("raw_score", lit(null))
                                      .withColumn("segmented_score", lit(null))
                                      .join(broadcast(DimScoringSegmentDataset).filter("model_id = 108"),Seq("segment_rank"),"left")
                                      .join(broadcast(dimScoringModel)
                                        .select("scoring_model_key_hh","model_id_hh"),col("model_id") === col("model_id_hh"),"left")
                                      .withColumnRenamed("scoring_model_key_hh","scoring_model_key")
                                      .withColumnRenamed("var_channel", "channel")
                                      .withColumnRenamed("var_plcc_holder", "plcc_holder")
                                      .withColumn("scoring_date", lit(firstFiscalWed).cast(DateType))
                                      .withColumn("batch_id", lit(batchId))
                                      .withColumn("last_updated_date", current_timestamp)
                                      .select(targetFactColumns: _*)

      val Monthly_Scoring_Dataset = Household_Scoring_Monthly.unionAll(joinedTransform_Monthly)

      joinedTransform_Daily.unionAll(Monthly_Scoring_Dataset)

    }
    else

    {

      joinedTransform_Daily

    }

  }

  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }

}