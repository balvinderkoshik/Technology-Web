package com.express.processing.dim

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * DimCSAgent Dimension Load
  *
  * @author aditi chauhan
  *
  *
  */

object DimCSAgent extends DimensionLoad with LazyLogging {


  override def dimensionTableName: String = "dim_cs_agent"

  override def surrogateKeyColumn: String = "cs_agent_key"

  override def naturalKeys: Seq[String] = Seq("cs_agent_id")

  override def derivedColumns: Seq[String] = Nil

  override def transform: DataFrame = {
    val CSAgentWorkTable = s"$workDB.work_bp_csagent_dataquality"
    logger.info("Reading CS Agent Work Table: {}", CSAgentWorkTable)
    hiveContext.table(CSAgentWorkTable)
      .withColumn("cs_agent_id",col("id"))
      .withColumn("cs_agent_nbr", col("agentnumber"))
      .withColumn("cs_agent_group_id", col("groupid"))
      .withColumn("cs_agent_first_name", col("firstname"))
      .withColumn("cs_agent_last_name", col("lastname"))
      .withColumn("cs_agent_email_addr", col("emailaddress"))
      .withColumn("cs_agent_phone_nbr", col("phonenumber"))
      .withColumn("cs_agent_extension", col("extension"))
      .withColumn("cs_agent_username", col("username"))
      .withColumn("cs_agent_status_cd",col("status"))
      .withColumn("cs_agent_role_id", col("roleid"))

  }

  def main(args: Array[String]): Unit = {
    val batchId = args(0)
    load(batchId)
  }

}
