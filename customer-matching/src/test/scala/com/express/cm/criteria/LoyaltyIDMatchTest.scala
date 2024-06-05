package com.express.cm.criteria

/**
  * Created by bhautik.patel on 12/04/17.
  */

import com.express.cm.TestSparkContext._
import com.express.cdw._
import com.express.cm.criteria.LoyaltyIDMatch._
import org.scalatest.{FlatSpec, Matchers}

class LoyaltyIDMatchTest extends FlatSpec with Matchers {

  case class EmpLoyaltySource(implied_loyalty_id: String)

  "LoyaltyID Match Function" should "generate the Match results correctly" in {

    val sqlContext = getSQLContext
    val dummySource = Seq(EmpLoyaltySource("100001"), EmpLoyaltySource("100004"),EmpLoyaltySource("100005"),
      EmpLoyaltySource("100007"))

    val source = sqlContext.createDataFrame[EmpLoyaltySource](dummySource).transform(CMColumns)

    // run match function
    val matchResults = source.transform(matchFunction)
    matchResults.filter(s"$MatchStatusColumn = true").count should be(2)
    matchResults.filter(s"$MatchStatusColumn = false").count should be(2)
    matchResults.filter(s"implied_loyalty_id = 100004").head().getAs[Int](MemberKeyColumn) should be(1004)
  }

}