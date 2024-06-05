package com.express.processing.fact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, lit, _}

/**
  * Created by amruta.chalakh on 7/11/2017.
  */
object FactEngagementHist extends FactLoad{

  override def factTableName = "fact_engagement_history"

  override def surrogateKeyColumn = "engagement_history_id"

  override def transform: DataFrame = {
    val WorkEngagement = hiveContext.table(s"$workDB.work_lw_engagement_dataquality")
      .select("a_rowkey","a_vckey","a_overrideamount","a_engagementtype","a_adddate","a_parentrowkey","a_storecode")
      .withColumnRenamed("a_rowkey","id")
      .withColumnRenamed("a_vckey","vc_key")
      .withColumnRenamed("a_overrideamount","override_amount")
      .withColumnRenamed("a_engagementtype","engagement_def_code")
      .withColumnRenamed("a_adddate","engagement_date")
      .withColumn("member_key",lit(null))

    val DimstoreDF = hiveContext.sql(s"select store_id,nvl(store_key,-1) as store_key from $goldDB.dim_store_master where status='current'")
    val DimDateDF = hiveContext.sql(s"select sdate,nvl(date_key,-1) as date_key from $goldDB.dim_date where status ='current'")
    val factEngagementDF = hiveContext.sql(s"select engagement_history_id,nvl(id,-1) as id from $goldDB.fact_engagement_history")
      .withColumnRenamed("engagement_history_id","engagement_history_id_parent")
      .withColumnRenamed("id","id_parent")

    val factEngagementHistStore = WorkEngagement.join(DimstoreDF, col("a_storecode") === col("store_id"),"left")
      .select("store_key", WorkEngagement.columns: _*)
      .withColumnRenamed("store_key","engagement_store_key")
      .drop("a_storecode")

    val factEngagementHistDate = factEngagementHistStore.join(DimDateDF,col("engagement_date") === col("sdate"),"left")
      .select("date_key",factEngagementHistStore.columns: _*)
      .withColumnRenamed("date_key","engagement_date_key")

    val factEngagementHistID = factEngagementHistDate.join(factEngagementDF, col("a_parentrowkey") === col("id_parent"),"left")
      .select("engagement_history_id_parent",factEngagementHistDate.columns: _*)
      .withColumn("parent_engagement_history_id", when(col("engagement_history_id_parent").isNull, lit(-1)).otherwise(col("engagement_history_id_parent")))
      //.withColumnRenamed("engagement_history_id_parent","parent_engagement_history_id")
      .drop("a_parentrowkey").drop("engagement_history_id_parent")

    factEngagementHistID
      .withColumn("batch_id",lit(batchId))
      .withColumn("last_updated_date",current_timestamp)
  }

  def main(args: Array[String]): Unit = {
    batchId = args(0)
    load()
  }
}
