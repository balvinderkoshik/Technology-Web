package com.express.cm.spark

import com.express.cdw._
import com.express.cm.criteria.MatchTrait
import com.express.utils.Settings
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Dataframe Utility Functions for Customer matching process
  *
  * @author mbadgujar
  */
object DataFrameUtils {

  implicit class CMDataFrameImplicits(dataframe: DataFrame) extends LazyLogging {

    /**
      * Add Customer Matching meta columns to the source Dataframe
      *
      * @return [[DataFrame]]
      */
    def addCMMetaColumns(): DataFrame = {
      dataframe.withColumn(MatchStatusColumn, lit(false)) /* */
        .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.NoMatch))
        .withColumn(MemberKeyColumn, lit(-1L))
        .withColumn(MCDCriteriaColumn,lit("matched"))
    }

    /**
      * Transform the source dataframe by applying the provided [[MatchTrait]] functions
      *
      * @param cmFunctions The CM matching functions in order they should be applied
      * @return Tuple of Optional dataframe of Matched records and dataframe of unmatched records
      */
    def applyCustomerMatch(cmFunctions: Seq[MatchTrait]): (Option[DataFrame], DataFrame) = {
      // Get Matched records
      def getMatched(dataframe: DataFrame) = dataframe.filter(s"$MatchStatusColumn = true")

      // Get unmatched records
      def getUnMatched(dataframe: DataFrame) = dataframe.filter(s"$MatchStatusColumn = false")

      /*
         Note: *Temporary Workaround* Saving intermediate matched and unmatched results to HDFS to break the lineage
         Currently, the Spark Driver hangs/overloads if provided with all of the Customer matching dataframe transformations chaining,
         resulting in long delays for submission of jobs
         TODO: Simplify the DF transformations and/or Check with latest Spark version
       */
      def saveResults(previousMatched: Option[DataFrame], matchApplied: DataFrame, cmFunction: String): ((Option[DataFrame], DataFrame), Boolean) = {
        val temporaryArea = Settings.getCMTemporaryArea
        val coalescePartitionNum = com.express.cdw.Settings.getCoalescePartitions
        val tempMatchedDir = temporaryArea + "matched/" + cmFunction
        val tempUnmatchedDir = temporaryArea + "unmatched/" + cmFunction
        val sqlContext = matchApplied.sqlContext
        val matchedRecords = getMatched(matchApplied)
        val unmatchedRecords = getUnMatched(matchApplied)
        val matchedCount = matchedRecords.count
        logger.info("Matched records: {}", matchedCount.toString)
        val unmatchedCount = unmatchedRecords.count
        logger.info("Unmatched records: {}", unmatchedCount.toString)
        val nextMatchNotRequired = unmatchedCount == 0
        // Write Intermediate Output to HDFS
        (previousMatched match {
          case None => matchedRecords
          case Some(matched) => matched.unionAll(matchedRecords)
        }).coalesce(coalescePartitionNum).write.mode(SaveMode.Overwrite).parquet(tempMatchedDir)
        unmatchedRecords.coalesce(coalescePartitionNum).write.mode(SaveMode.Overwrite).parquet(tempUnmatchedDir)
        matchApplied.unpersist
        ((Some(sqlContext.read.parquet(tempMatchedDir)), sqlContext.read.parquet(tempUnmatchedDir)), nextMatchNotRequired)
      }

      // apply Customer Matching transformation functions
      cmFunctions.foldLeft[(Option[DataFrame], DataFrame)]((None, dataframe)) {
        case ((None, _), matchTrait) =>
          dataframe.persist()
          logger.info("Applying {} customer matching function", matchTrait.getClass.getName)
          logger.info("Input DF records: {}", dataframe.count.toString)
          val matchApplied = addCMMetaColumns().transform(matchTrait.matchFunction).persist()
          logger.info("Output DF records: {}", matchApplied.count.toString)
          dataframe.unpersist()
          val ((matched, unmatched), nextMatchNotRequired) = saveResults(None, matchApplied, matchTrait.getClass.getName)
          if (nextMatchNotRequired) return (matched, unmatched) else (matched, unmatched)
        case ((previousMatched, df), matchTrait) =>
          df.persist()
          logger.info("Applying {} customer matching function", matchTrait.getClass.getName)
          logger.info("Input records: {}", df.count.toString)
          val matchApplied = df.transform(matchTrait.matchFunction).persist()
          logger.info("Output DF records: {}", matchApplied.count.toString)
          df.unpersist()
          val ((matched, unmatched), nextMatchNotRequired) = saveResults(previousMatched, matchApplied, matchTrait.getClass.getName)
          if (nextMatchNotRequired) return (matched, unmatched) else (matched, unmatched)
      }
    }
  }

}
