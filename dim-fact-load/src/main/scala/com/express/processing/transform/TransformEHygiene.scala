package com.express.processing.transform

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.express.cdw.CDWContext
import com.express.cdw.spark.DataFrameUtils._

/**
  * Created by aditi.chauhan on 11/5/2017.
  */
object TransformEHygiene extends CDWContext with LazyLogging {

  val tempDimMmeTable = s"$workDB.dim_member_multi_email"
  val tempDimMmeDF = hiveContext.table(s"$workDB.dim_member_multi_email")

  /**
    * Transform E Hygiene entry point
    *
    */
  def main(args: Array[String]): Unit = {

    /*Fetching data from gold.dim_member_multi_email table*/

    val mmeDF = hiveContext.table(s"$goldDB.dim_member_multi_email").filter("status='current'")
                                                                    .drop(col("last_updated_date"))
                                                                    .drop(col("status"))

    /*The list of columns to be modified/used as a part of Ehygiene transform*/

    val eh_transform_colList = Seq(col("member_key"),col("email_address"),col("valid_email"),col("batch_id"))


    /*Fetching the incoming data from EHygiene Experian inbound file*/
    val factEHExperian_DF= hiveContext.sql(s"""select trim(lower(email)) as email_address_eh,
                                                      case when trim(lower(category))='valid' then 'YES' when trim(lower(category))='invalid' then 'NO'
                                                      end as valid_invalid_flag,
                                                      trim(lower(correction)) as correction,
                                                      batch_id as batch_id_eh
                                                      from $workDB.fact_eh_experian_temp""")
                                     .withColumn("valid_email_eh",col("valid_invalid_flag"))
                                     .distinct()


    /*Joining the factEHExperian_DF with dim_member_multi_email to find the matching records for valid_email flag update*/

    val joinSourceDF =  factEHExperian_DF
                        .join(mmeDF,col("email_address_eh") === trim(lower(col("email_address"))),"left" )
                        .filter(not(isnull(col("email_address"))) && not(isnull(col("member_key"))))
                        .drop(col("valid_email"))
                        .withColumn("valid_email",col("valid_email_eh"))

    /*Process started for identifying corrected emails to be inserted in dim_member_multi_email table*/
    import hiveContext.implicits._

    /*Here we are identifying the emails that ehygiene has sent us as a part of the correction.
     These emails along with their associated member keys will be inserted into the system if they do not already exist.
     Here,the is_loyalty_email will be explicitly set to 'N0' and valid_email is set to 'YES'.
     All the other attributes are carry forwarded for the email_address for which this correction came in.
     -which defines the unmatched and InsertDF dataframe

     However,if the email_address and associated member_key that came in as a part of correction already existed into our system
     then only the valid_email flag will be updated
     while all the other attributes will be carry forwarded.
     -which defines the matched and Insert_UpdateDF
     */

    val (unmatched,matched) = joinSourceDF.filter("correction is not null and length(trim(correction)) > 0")
                                          .drop(col("email_address"))
                                          .drop(col("valid_email"))
                                          .withColumn("valid_email",lit("YES"))
                                          .withColumnRenamed("correction","email_address")
                                          .join(mmeDF
                                          .selectExpr("member_key as dim_member_key","email_address as dim_email_address","is_loyalty_email as dim_is_loyalty_email")
                                                      ,col("member_key") === col("dim_member_key") and lower(trim(col("email_address"))) === lower(trim(col("dim_email_address"))),"left")
                                          .partition(isnull(col("dim_member_key")))

    /*InsertDF: This dataframe will be used to insert those member_key and email_address in dim_member_multi_email table
                which did not exist in our system and came as a part of correction through EHygiene*/

    val InsertDF =     unmatched.withColumn("is_loyalty_email",lit("NO"))
                                .withColumn("original_source_key",lit(2228))
                                .select(mmeDF.getColumns:_*)
                                .withColumn("action_flag",lit("I"))

    val Insert_UpdateDF = matched.select("dim_member_key","dim_email_address")
                                 .join(mmeDF,lower(trim(col("email_address"))) === lower(trim(col("dim_email_address"))) and col("member_key") === col("dim_member_key"),"left")
                                 .withColumn("valid_email",lit("YES"))
                                 .select(mmeDF.getColumns:_*)

    /*UpdateDF: This dataframe will be used to update those member_key and email_address in dim_member_multi_email table
                which already existed into our system.The columns that will be updated would be : valid_email and source_key*/

    val UpdateDF = joinSourceDF.select(mmeDF.getColumns:_*)
                               .unionAll(Insert_UpdateDF)
                               .withColumn("action_flag",lit("U"))

    /*Final Dataframe:-Insert+Update*/

    val FinalDF = InsertDF.unionAll(UpdateDF)
                          .withColumn("source_key",lit(2228))
                          .withColumn("last_updated_date",current_timestamp())
                          .withColumn("process",lit("ehygiene_experian"))
                          .select(tempDimMmeDF.getColumns:_*)

    // Write output to temp dim_member_multi_email table
    FinalDF.write.mode(SaveMode.Overwrite).partitionBy("process").insertInto(tempDimMmeTable)

  }

}
