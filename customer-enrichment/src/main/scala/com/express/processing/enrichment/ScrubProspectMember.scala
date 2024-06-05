package com.express.processing.enrichment


import java.io.{File, FilenameFilter}
import java.sql.{Connection, Date, DriverManager, SQLException, Statement}
import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw.{CDWContext, CDWOptions}
import com.express.processing.enrichment.HDFSRenameUtil._
import com.express.processing.enrichment.util.Settings
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.joda.time.DateTime

/**
  * Created by Monashree.Sanil on 31/08/2018.
  */
object ScrubProspectMember extends CDWContext with LazyLogging with CDWOptions {


  private val PurgeTransformations = {
    Map(
      "direct_mail_consent" -> lit("UNKNOWN"),
      "gender" -> lit("UNKNOWN"),
      "deceased" -> lit("UNKNOWN"),
      "address_is_prison" -> lit("UNKNOWN"),
      "current_source_key" -> lit(776),
      "overlay_rank_key" -> lit(0),
      "gender_scrubbed" -> lit("UNKNOWN"),
      "last_updated_date" -> to_utc_timestamp(concat_ws(" ", date_add(current_date(), 1), lit("00:01:00")), "YYYY-MM-dd HH:mm:ss")
    )
  }

  /**
    * Function for applying transformations
    *
    * @param inputDF Input [[DataFrame]]
    * @return [[DataFrame]]
    */
  private def applyTransformations(inputDF: DataFrame): DataFrame = {

    val ExceptionColList = List("valid_email", "email_consent", "customer_add_date", "customer_introduction_date", "record_info_key", "original_record_info_key",
      "match_type_key", "original_source_key", "is_express_plcc", "record_type", "customer_add_date_key", "customer_introduction_date_key",
      "is_dm_marketable", "is_em_marketable", "is_sms_marketable", "closest_store_state", "is_loyalty_member", "preferred_store_state", "second_closest_store_state", "best_household_member",
      "purge_flag", "status", "member_key", "file_name", "member_id","batch_id")
    inputDF.columns.foldLeft(inputDF) {
      (df, column) =>
        df.withColumn(column, when(col("purge_flag") === true && !ExceptionColList.contains(column),
          PurgeTransformations.getOrElse(column, if (column matches ".*_key") -1 else null)).otherwise(col(column)))
    }
  }

  private def update(r: Row, con: Connection, st: Statement, table: String, batch_id: String): Unit = {
    val purge_completion_date = r.getAs[Date]("purge_completion_date").toString
    val purge_completed_flag = r.getAs[String]("purge_completed_flag")
    val purge_record_count = r.getAs[Long]("purge_record_count")
    val last_updated_date = r.getAs[String]("last_updated_date").toString
    val rental_list_id = r.getAs[java.math.BigDecimal]("RENTAL_LIST_ID")
    val sql = s"UPDATE $table SET purge_completion_date=to_date('$purge_completion_date','YYYY-MM-DD'),purge_record_count=$purge_record_count," +
      s"purge_completed_flag='$purge_completed_flag',last_updated_date=to_date('$last_updated_date','YYYY-MM-DD HH24:MI:SS'),batch_id='$batch_id' where rental_list_id=$rental_list_id"
    try {
      st.executeUpdate(sql)
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
  }

  private def checkCondition(check: Boolean, errorMsg: String): Unit = {
    if (!check) {
      logger.info(errorMsg)
      throw new IllegalStateException(errorMsg)
    }
  }

  // function to purge members from current partition
  private def purge_current(members: DataFrame, lowerBoundFilterDate: Date, batch_id: String): Unit = {

    val dimMember = s"$goldDB.dim_member"

    val members_without_chygiene = members
      .withColumn("purge_flag", when(col("current_source_key") === 776, lit(false)).otherwise(col("purge_flag")))

    val members_purged_count = members_without_chygiene.filter("purge_flag=true").count()

    //purging columns and preparing final dataframe
    val purgedProspectMembersResult = applyTransformations(members_without_chygiene)

    logger.info("<----------------------------------Purge Process for Current Partition-------------------------------------->")
    //Insert into target table
    purgedProspectMembersResult
      .drop(col("purge_flag"))
      .withColumn("batch_id",lit(batch_id))
      .insertIntoHive(SaveMode.Overwrite, dimMember, Some("status"), batch_id)
    logger.info("<----------------------------------Purge Process for Current Completed Successfully-------------------------------------->")
    logger.info(s"<---------------------------------$members_purged_count members are purged in current partition--------------------->")
  }

  // function to purge members from history partition

  private def purge_history(memberKeys: DataFrame, workTempTableDir: String, fileNamePartitionedDir: String, finalPartFilesDir: String, goldDimMemberHistDir: String): Unit = {

    logger.info("<----------------------------------Purge Process for History Partition-------------------------------------->")

    val HistPurgePartFileTable = s"$workDB.dim_member_scrub_history"

    val historyPurgePartsFiles = hiveContext.table(s"$goldDB.dim_member")
      .filter("status = 'history'")
      .select("member_key")
      .withColumn("file_name", input_file_name())
      .join(broadcast(memberKeys), Seq("member_key"), "inner")

    val historyPurgePartsList = historyPurgePartsFiles.select("file_name").distinct().collect().map(_.getAs[String](0))
    val partFilesCount = historyPurgePartsList.length
    logger.info(s"<---------------------Part file list of length $partFilesCount retrieved----------------------------->")

    deleteHDFS(workTempTableDir)
    checkCondition(makeDirHDFS(workTempTableDir), s"Failure in creation of $workTempTableDir.")

    logger.info("<--------------------------Part Files copying to temp table------------------>")
    case class copyFile (src: String ,dest: String)
    val copy_rdd=hiveContext.sparkContext.parallelize(historyPurgePartsList.map(a => copyFile(a,workTempTableDir)))
    val successFailure=copy_rdd.map(cp  => copyHDFS(cp.src,cp.dest))
    val successFailureDistinct=successFailure.distinct()
    checkCondition(successFailureDistinct.count() == 1 && successFailureDistinct.first(),"sException in copy of Part files in $workTempTableDir")

    val tempTablefilelist = listFilesHDFS(workTempTableDir, new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = true
    }, None)
    val tempTableFileCount = tempTablefilelist.length
    checkCondition(tempTableFileCount == partFilesCount, s"$tempTableFileCount copied instead of $partFilesCount")
    logger.info(s"<----------$tempTableFileCount Part files copied to temp table successfully!!--------->")

    logger.info("<-------------------------Applying Purge transformations-------------------->")
    val historyPurgeDF = hiveContext.table(HistPurgePartFileTable)
      .filter("status='history'")
      .withColumn("file_name", udf((path: String) => path.split("/").last).apply(input_file_name()))

    val historyPurgedPartPurgeFlagJoin = historyPurgeDF
      .join(broadcast(memberKeys), historyPurgeDF.col("member_key") === memberKeys.col("member_key"), "left")
      .withColumn("purge_flag", when(memberKeys.col("member_key").isNotNull, lit(true)).otherwise(lit(false)))
      .drop(memberKeys.col("member_key"))

    val historyPurgedParts = applyTransformations(historyPurgedPartPurgeFlagJoin)

    deleteHDFS(fileNamePartitionedDir)
    checkCondition(makeDirHDFS(fileNamePartitionedDir), s"Failure in creation of $fileNamePartitionedDir.")
    logger.info(s"$fileNamePartitionedDir created succcessfully!!")
    logger.info(s"Processed files being written to $fileNamePartitionedDir!!")
    historyPurgedParts
      .dropColumns(Seq("status", "purge_flag"))
      .repartition(historyPurgePartsList.length, col("file_name"))
      .write.partitionBy("file_name")
      .format("orc").mode("overwrite").save(fileNamePartitionedDir)

    val fileList = listFilesHDFS(fileNamePartitionedDir, new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = dir.getPath.contains(name)
    }, Some("file_name"))
    val fileListCount = fileList.length
    checkCondition(fileListCount == partFilesCount, s"$fileListCount copied instead of $partFilesCount")
    logger.info(s"$fileListCount Processed Orc files written to $fileNamePartitionedDir successfully!!")

    deleteHDFS(finalPartFilesDir)
    checkCondition(makeDirHDFS(finalPartFilesDir), s"Failure in creation of $finalPartFilesDir.")
    logger.info(s"$finalPartFilesDir created succcessfully!!")

    val srcTodestMap = fileList.map(path => (path, path.split("/").takeRight(2).head.split("=").last)).toMap
    checkCondition(srcTodestMap.forall { case (src, dest) => moveHDFS(src, s"$finalPartFilesDir$dest") }, s"Exception in renaming and moving processed Part files in $finalPartFilesDir")
    val finalPartFiles = listFilesHDFS(finalPartFilesDir, new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = true
    }, None)

    val finalPartFileCount = finalPartFiles.length
    checkCondition(finalPartFileCount == partFilesCount, s"$finalPartFileCount copied instead of $partFilesCount")
    logger.info(s"$finalPartFileCount Orc files renamed and copied to $finalPartFilesDir")

    val finalcopy_rdd=hiveContext.sparkContext.parallelize(finalPartFiles.map( filename =>copyFile( filename , goldDimMemberHistDir)))
    val successFail=finalcopy_rdd.map(cp => copyHDFS(cp.src,cp.dest))
    val successFaildistinct=successFail.distinct()
    checkCondition(successFaildistinct.count()==1 & successFaildistinct.first(),s"Exception in copying files to $goldDimMemberHistDir")
    logger.info(s"$finalPartFileCount Processed part files overwritten in $goldDimMemberHistDir successfully")

    closeHDFS()

  }

  def main(args: Array[String]): Unit = {

    val arguments = parse(args)
    val batch_id = arguments.getOrElse("batch_id", null)

    val audit_table = "nifi_tracking.prospect_audit"
    val jdbcUrl = Settings.getJDBCUrl()
    val connectionProperties = Settings.getConnectionProperties()

    val AuditRental = hiveContext.read.jdbc(jdbcUrl, audit_table, connectionProperties)

    //Get Purge status and counts from Audit table
    val current_date_time = DateTime.now()
    val lowerBoundFilterDate = AuditRental.filter("purge_completed_flag='N'").selectExpr("to_date(process_date) as process_date").agg(min("process_date")).head().getDate(0)
    val purge_period=if(lowerBoundFilterDate != null)
      AuditRental.filter(s"purge_completed_flag='N' and to_date(process_date)=to_date('$lowerBoundFilterDate')").select("purge_period").head().getDecimal(0).intValueExact()
    else
      0

    val message = if (lowerBoundFilterDate != null && lowerBoundFilterDate.before(current_date_time.minusDays(purge_period - 1).toDate)) {

      // Member Dataframe
      val dimMemberDF = hiveContext.table(s"$goldDB.dim_member").filter("status = 'current'")

      //labeling members to be purged
      val members = dimMemberDF
        .withColumn("purge_flag", when(upper(trim(col("record_type"))) === "PROSPECT" and col("customer_add_date") >= lowerBoundFilterDate and datediff(current_date(), col("customer_add_date")) >= purge_period, lit(true))
          .otherwise(lit(false)))

      //purging members in current partition
      purge_current(members, lowerBoundFilterDate, batch_id)

      //list of purged member keys
      val purgedMembers = members.filter("purge_flag=true")
      val memberKeys = purgedMembers.select("member_key").persist
      val members_purged_count = memberKeys.count


      //purging members in history partition
      purge_history(memberKeys, Settings.getworkTempTableDir(), Settings.getfileNamePartitionedDir(), Settings.getfinalPartFilesDir(), Settings.getgoldDimMemberHistDir())
      logger.info(s"<-------------------------------------Purged all prospect members from history partition----------------------------->")

      logger.info("<----------------------------------Audit Table Updation-------------------------------------->")
      val memberCounts = purgedMembers.select("member_key", "customer_add_date").groupBy("customer_add_date").count().withColumnRenamed("count", "countbydate")

      val AuditRental_withDates = AuditRental.withColumn("p_date", to_date(col("process_date")))
      val updatedAuditRental = AuditRental_withDates
        .join(memberCounts, AuditRental_withDates.col("p_date") === memberCounts.col("customer_add_date"), "left")
        .filter(col("customer_add_date").isNotNull)
        .withColumn("purge_record_count", col("countbydate"))
        .withColumn("purge_completed_flag", lit("Y"))
        .withColumn("purge_completion_date", lit(current_date()))
        .withColumn("last_updated_date", lit(from_unixtime(unix_timestamp())))
        .drop("customer_add_date")
        .drop("countbydate")

      try {
        val conn = DriverManager.getConnection(jdbcUrl, Settings.getUser(), Settings.getPassword())
        val stmt = conn.createStatement
        updatedAuditRental.collect().foreach(row => update(row, conn, stmt, audit_table, batch_id))
        stmt.close()
        conn.close()
      } catch {
        case se: SQLException =>
          logger.info("Audit Table updation failed!! Caught SQL Exception")
          throw se
        case e: Exception =>
          logger.info("Audit Table updation failed!! Caught Connection Exception")
          throw e
      }

      members_purged_count + " members were successfully purged!!"

    } else "No members found to purge"

    logger.info(message)
  }
}
