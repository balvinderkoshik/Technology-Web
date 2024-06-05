package com.express.edw.lookup

import com.express.edw.util.Settings
import com.express.cdw.{CDWContext, CDWOptions}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object LookUp extends CDWContext with CDWOptions{

  addOption("moduleName")

  def main(args: Array[String]): Unit = {

    //printing input parameters
    println("ARGS:" + args.mkString(","))

    //importing input parameters
    val options = parse(args)
    val moduleName = options("moduleName")


    val tableConfig = Settings.getLookUoConfigMapping(moduleName)
    val targetTableName = tableConfig.targetTableName
    val sourceTable = tableConfig.sourceTableName
    val filterCondition = tableConfig.filterCondition
    val sourceColumnList = tableConfig.sourceColumnList.seq
    val sourceColumnListString = sourceColumnList.mkString(",")


    val sourceData = hiveContext.sql(s"select $sourceColumnListString from $goldDB.$sourceTable where $filterCondition")

    val finalDF = sourceData.withColumn("run_date",current_date)

    finalDF.write.partitionBy("run_date").mode(SaveMode.Append).insertInto(s"$smithDB.$targetTableName")

  }

}
