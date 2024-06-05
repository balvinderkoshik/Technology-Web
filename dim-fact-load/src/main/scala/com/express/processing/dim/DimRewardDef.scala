package com.express.processing.dim

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by poonam.mishra on 7/12/2017.
  */
object DimRewardDef extends DimensionLoad with LazyLogging{
  override def dimensionTableName: String = "dim_reward_def"

  override def surrogateKeyColumn: String = "reward_def_key"

  override def naturalKeys: Seq[String] = Seq("reward_def_id")

  override def derivedColumns: Seq[String] = Nil

  override def transform: DataFrame = {
    val dimTier=hiveContext.table(s"$goldDB.dim_tier")
      .filter("status='current'")
      .select("tier_key","tier_id")
      .withColumnRenamed("tier_id","dim_tier_id")
      .withColumnRenamed("tier_key","dim_tier_key")

    val rewardDefWorkTable = s"$workDB.work_bp_rewards_dataquality"
    logger.info("Reading - Reward Def Work Table: {}", rewardDefWorkTable)
    hiveContext.table(rewardDefWorkTable)
      .withColumnRenamed("id","reward_def_id")
      .withColumnRenamed("name","reward_name")
      .withColumn("reward_long_description",lit(null))
      .withColumn("reward_short_description",lit(null))
      .withColumnRenamed("how_many_points_to_earn","points_required_to_earn")
      .withColumnRenamed("thresh_hold","threshhold")
      .withColumn("expiration_period",lit(null))
      .withColumn("expiration_period_unit",lit(null))
      .withColumnRenamed("active","active_flag")
      .withColumn("is_appeasement",when(upper(trim(col("certificate_type_code"))) === "APPEASEMENT",lit("YES")).otherwise(lit("NO")))
      .join(dimTier,dimTier.col("dim_tier_id")===col("tier_id"),"left")
      .withColumn("tier_key",col("dim_tier_key"))
      .withColumn("tier_key",when(col("tier_key").isNull,lit(-1)).otherwise(col("tier_key")))
      .selectExpr("reward_def_id","certificate_type_code","reward_name","cast(reward_long_description as string)","cast(reward_short_description as string)","points_required_to_earn","point_type","point_event","cast(tier_key as int)","threshhold","cast(expiration_period as int)","cast(expiration_period_unit as int)","cast(catalog_start_date as date)","cast(catalog_end_date as date)","cast(active_flag as string)","cast(is_appeasement as string)")

  }
  def main(args: Array[String]): Unit = {
    val batchId = args(0)
    load(batchId)
  }

}
