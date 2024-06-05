package com.express.cm.criteria

import com.express.cdw._
import com.express.cm.TestSparkContext._
import com.express.cm.{NameFormat, UDF}
import org.apache.spark.sql.functions.udf
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by aman.jain on 4/13/2017.
  * Test class for PLCCNameMatch
  */
class PLCCNameImpliedLoyaltyMatchTest extends FlatSpec with Matchers {

  case class PLCCNameSource(tender_number: Int, first_name: String, last_name: String)


  "PLCC Match Function" should "generate the Match results correctly" in {

    val sqlContext = getSQLContext

    val dummyPLCCSource = Seq(
      PLCCNameSource(50001, "Sherlock", "Holmes"),
      PLCCNameSource(50002, "Sherlockk", "Holmes"),
      PLCCNameSource(50004, "John", "Watson"),
      PLCCNameSource(50006, "Sherlock", "Holmes")
    )

    val source = sqlContext.createDataFrame[PLCCNameSource](dummyPLCCSource).transform(CMColumns)
    val sourceNameFormatUDF = UDF(udf(NameFormat("", _: String, "", _: String, "")), Seq("first_name", "last_name"))

    // run match function

    val plccNameMatch = new PLCCNameImpliedLoyaltyMatch(sourceNameFormatUDF)

    val matchResults = source.transform(plccNameMatch.matchFunction)
    matchResults.show()
    matchResults.filter(s"$MatchStatusColumn = true").count should be(1)
    matchResults.filter(s"$MatchStatusColumn = false").count should be(3)
  }
}