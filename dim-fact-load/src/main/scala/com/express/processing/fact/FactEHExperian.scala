package com.express.processing.fact

//import org.apache.spark.sql.functions.{current_timestamp, lit, udf, _}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by nidhi chechani on 31/10/17.
  */
object FactEHExperian extends FactLoad{
  /**
    * Get Fact Table name
    *
    * @return [[String]]
    */
  override def factTableName: String = "fact_eh_experian"

  /**
    * Get Surrogate Key Column
    *
    * @return [[String]]
    */
  override def surrogateKeyColumn: String = "fact_eh_experian_id"

  /**
    * Transformation for the Fact data
    *
    * @return Transformed [[DataFrame]]
    */

  override def transform: DataFrame = {

    val valid_invalid = udf((Result : String,Verbose_Result : String) => {
      (Result.trim.toLowerCase, Verbose_Result.trim.toLowerCase)
      match {
        case ("verified", "verified") => "valid"
        case ("undeliverable", "mailboxfull") => "valid"
        case ("undeliverable", "syntaxfailure") => "valid"
        case ("unreachable", "unreachable") => "valid"
        case ("unreachable", "unresolvable") => "valid"
        case ("illegitimate", "illegitimate") => "valid"
        case ("illegitimate", "roleaccount") => "valid"
        case ("illegitimate", "localpartspamtrap") => "valid"
        case ("illegitimate", "profanity") => "valid"
        case ("unknown", "unknown") => "valid"
        case ("unknown", "timeout") => "valid"
        case ("unknown", "acceptall") => "valid"
        case ("unknown", "relaydenied") => "valid"
        case (_,_) => "invalid"

      }
    })

    hiveContext.table(s"$workDB.work_experian_eh_dataquality")
      .withColumn("category", valid_invalid(col("result"),col("verboseresult")))
      .withColumn("last_updated_date",current_timestamp)
      .withColumn("batch_id",lit(batchId))
  }

  def main(args: Array[String]): Unit =  {
    batchId = args(0)
    load()
  }
}
