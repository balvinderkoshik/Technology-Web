package com.express.cm.criteria

import com.express.cdw._
import com.express.cm.TestSparkContext._
import com.express.cm.{NameFormat, UDF, _}
import org.apache.spark.sql.functions.udf
import org.scalatest.{FlatSpec, Matchers}

/**
  *
  * @author mbadgujar
  */
class NameAddressMatchTest extends FlatSpec with Matchers {

  case class NameAddressSource(name: String, lastName: String, address1: String, address2: String, zipcode: String)

  "Name Address Match Function" should "generate the Match results correctly" in {

    val sqlContext = getSQLContext
    val dummySource = Seq(NameAddressSource("sherlock", "holmes", "baker Street", null, "111111"),
      NameAddressSource("sherlock", "holmes", "baker Street", "", "111111"),
      NameAddressSource("sherlock", "holmes", "baker Street", "101", "111111"))

    val source = sqlContext.createDataFrame[NameAddressSource](dummySource)
      .transform(CMColumns)

    // run match function
    val sourceNameFormatUDF = UDF(udf(NameFormat("", _: String, "", _: String, "")),
      Seq("name", "lastName"))

    val sourceAddressFormat = UDF(udf(AddressFormat(_: String, _: String, "", "", _: String, "", "", "", "")),
      Seq("address1", "address2", "zipcode"))

    val nameAddressMatch = new NameAddressMatch(sourceNameFormatUDF, sourceAddressFormat)
    val matchResults = source.transform(nameAddressMatch.matchFunction)
    matchResults.filter(s"$MatchStatusColumn = true").count should be(2)
    matchResults.filter(s"$MatchStatusColumn = false").count should be(1)
    matchResults.filter(s"$MatchStatusColumn = true").head().getAs[Int](MemberKeyColumn) should be(1001)
  }

}
