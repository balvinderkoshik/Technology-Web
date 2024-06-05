package com.express.cm.criteria

import com.express.cdw.spark.DataFrameUtils._
import com.express.cdw._
import com.express.cm.lookup.LookUpTable.EmployeeDimensionLookup
import com.express.cm.lookup.LookUpTableUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Employee ID match Function
  * Match Table: DIM_EMPLOYEE
  *
  * @author mbadgujar
  */
object EmployeeIDMatch extends MatchTrait {

  override def matchFunction(source: DataFrame): DataFrame = {

    val sqlContext = source.sqlContext
    import sqlContext.implicits._

    // filter out  unmatched
    val unmatched = source.alias(SourceAlias)
    // load the look-up
    val lookUpDF = getLookupTable(EmployeeDimensionLookup).alias(LookUpAlias)
    // join
    val joinedDF = unmatched.join(lookUpDF, lpad($"$SourceAlias.employee_id",11,"0") === lpad($"$LookUpAlias.employee_id",11,"0"), "left")
    // unmatched employee ids
    val employeeUnmatched = joinedDF.filter(isnull($"$LookUpAlias.$MemberKeyColumn"))
      .select(source.getColumns(SourceAlias): _*)

    // Matched employee id, set the member id from matched records
    val employeeMatched = joinedDF.filter(not(isnull($"$LookUpAlias.$MemberKeyColumn")))
      .select(source.getColumns(SourceAlias, CMMetaColumns) :+ $"$LookUpAlias.$MemberKeyColumn": _*)
      .withColumn(MatchStatusColumn, lit(true))
      .withColumn(MatchTypeKeyColumn, lit(MatchTypeKeys.EmployeeId))
      .select(source.getColumns: _*)

    // create union and return the result
    employeeMatched.unionAll(employeeUnmatched)
  }
}
