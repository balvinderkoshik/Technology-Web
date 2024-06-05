package com.express.cm.criteria

import com.express.cdw._
import com.express.cm.TestSparkContext._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by aman.jain on 4/10/2017.
  * Test class for PLCCMatch
  */
class PLCCMatchTest extends  FlatSpec with Matchers {

  case class PLCCSource(tender_number: Int)

  "PLCC Match Function" should "generate the Match results correctly" in {

    val sqlContext = getSQLContext

    val dummyPLCCSource = Seq(
      PLCCSource(50001),
      PLCCSource(50002),
      PLCCSource(50004),
      PLCCSource(50006)
    )

    val source = sqlContext.createDataFrame[PLCCSource](dummyPLCCSource).transform(CMColumns)

    val plccMatch = PLCCMatch
    // run match function
    val matchResults = source.transform(plccMatch.matchFunction)
    matchResults.filter(s"$MatchStatusColumn = true").count should be(2)
    matchResults.filter(s"$MatchStatusColumn = false").count should be(2)
    matchResults.show()

  }
}
