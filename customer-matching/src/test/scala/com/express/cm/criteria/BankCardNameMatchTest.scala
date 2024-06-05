package com.express.cm.criteria

import com.express.cdw._
import com.express.cm.TestSparkContext._
import com.express.cm.{NameFormat, UDF}
import org.apache.spark.sql.functions.udf
import org.scalatest.{FlatSpec, Matchers}


/**
  * Bank Card match criteria test
  *
  * @author pmishra
  */
class BankCardNameMatchTest extends FlatSpec with Matchers{

  case class TlogSource(tender_number: Long, tender_type: Long, first_name: String, last_name: String)

  "BankCardAndName Match Function" should "generate the Match results correctly" in
  {
    //import BankCardOnlyMatch._
    val sqlContext = getSQLContext
    val dummySource = Seq(TlogSource(50001, 1, "s", "h"), TlogSource(50002, 2, "", ""),
      TlogSource(50003, 3, "sherlock", "holmes"), TlogSource(50003, 3, "", ""))
    val sourceNameFormatUDF = UDF(udf(NameFormat("", _: String, "", _: String, "")),
      Seq("first_name", "last_name"))

    val source = sqlContext.createDataFrame[TlogSource](dummySource).transform(CMColumns)
    val bankcardnamematch = new BankCardNameMatch(sourceNameFormatUDF)

    // run match function
    val matchResults = source.transform(bankcardnamematch.matchFunction)
    matchResults.filter(s"$MatchStatusColumn = true").count should be(1)
    matchResults.show
  }

}