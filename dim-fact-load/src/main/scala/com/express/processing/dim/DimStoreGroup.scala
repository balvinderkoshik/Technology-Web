package com.express.processing.dim
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.LongType

/**
  * Created by neha.mahajan on 7/17/2017.
  */
object DimStoreGroup extends DimensionLoad with LazyLogging{


  override def dimensionTableName: String = "dim_store_group"

  override def surrogateKeyColumn: String = "store_group_key"
  override def naturalKeys: Seq[String] = Seq("record_key","active_store_number")
  override def derivedColumns: Seq[String] = Nil

  override def transform: DataFrame = {
    val storegroupWorkTable = s"$workDB.work_storegroup_dataquality"
    logger.info("Reading Storegroup Work Table: {}", storegroupWorkTable)
    /*val storemasterBackupTable = s"$backUpDB.bkp_dim_store_master"
    logger.info("Reading StoreterBackup Table: {}", storemasterBackupTable)*/
    val storeGroupDF = hiveContext.table(storegroupWorkTable).
      select("active_store_number","record_key","selection_name")

    /*val smDimensionDF = hiveContext.sql(s"select * from $backUpDB.bkp_dim_store_master where status='current'")
    val maxBatchIdBackUpSM: String = smDimensionDF.select(col("batch_id").cast(LongType))
      .distinct
      .sort(desc("batch_id"))
      .head(1).toList match {
      case Nil => "0"
      case record :: Nil => record.getAs[Long]("batch_id").toString
    }*/
    val storeMasterDF = hiveContext.sql(s"select distinct store_id,store_key from $goldDB.dim_store_master where status='current'")

    storeGroupDF.join(storeMasterDF, storeMasterDF.col("store_id") === storeGroupDF.col("active_store_number"),"left")
      .withColumn("store_key",when(isnull(col("store_key")), -1).otherwise(col("store_key")))
      .withColumnRenamed("selection_name","group_selection_name")
      .selectExpr("(case when record_key is null or record_key = '' then NULL  else cast(record_key as int) end) as record_key","group_selection_name","(case when active_store_number is null or active_store_number = '' then NULL  else cast(active_store_number as int) end) as active_store_number","store_key")
  }
  def main(args: Array[String]): Unit = {
    val batchId = args {0}
    load(batchId)
  }
}
