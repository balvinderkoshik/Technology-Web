package com.express.processing.fact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by aditi.chauhan on 10/10/2017.
  */
object FactScoringHistoryDaily extends FactLoad {

  override def factTableName = "fact_scoring_history"

  override def surrogateKeyColumn = "scoring_history_id"

  override def transform: DataFrame = {
    import hiveContext.implicits._



    /*Fetching the dataset for the new members created daily*/
    val sourceDataset = hiveContext.sql(s"select member_key from $goldDB.dim_member where trim(match_type_key) = 92 and status='current' and batch_id = '$batchId'")

    /* Fetching the dataset for dim_scoring_model_segment for the active records against model_id=104*/

    val DimScoringSegmentDataset = hiveContext.sql(
      s"""select cast(segment_rank as string),scoring_model_segment_key,model_id,var_channel,var_plcc_holder from $goldDB.dim_scoring_model_segment
          where status='current' and model_id = 104 and segment_in_use = 'YES' and segment_grouping_rank is not null""")

    /*Fetching the dataset for dim_household*/

    val dimHousehold = hiveContext.sql(s"select household_key,associated_member_key from $goldDB.dim_household where status='current'")
      .withColumn("member_key", explode(split($"associated_member_key", ",")))

    /*Fetching the dataset for dim_scoring_model for the active records against model_id = 104 */

    val dimScoringModel = hiveContext.sql(s"select scoring_model_key,model_id from $goldDB.dim_scoring_model where model_id = 104 and status='current'")

    val joinedTransform = sourceDataset.join(dimHousehold, Seq("member_key"), "left")
      .select("household_key", sourceDataset.columns: _*)
      .withColumn("primary_customer_rid", lit(null))
      .withColumn("rid", lit(null))
      .withColumn("raw_score", lit(null))
      .withColumn("segment_rank", lit(15))
      .withColumn("segmented_score", lit(null))
      .withColumn("segment_type", lit("New30+Days1Trip"))
      .join(DimScoringSegmentDataset,Seq("segment_rank"),"left")
      .join(dimScoringModel,Seq("model_id"),"left")
      .withColumnRenamed("var_channel", "channel")
      .withColumnRenamed("var_plcc_holder", "plcc_holder")
      .withColumn("scoring_date", lit(null))
      .withColumn("batch_id", lit(batchId))
      .withColumn("last_updated_date", current_timestamp)

    joinedTransform
  }

  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }
}