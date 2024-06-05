package com.express.cm.criteria

import com.express.cdw._
import com.express.cm.TestSparkContext._
import com.express.cm.{NameFormat, UDF}
import org.apache.spark.sql.functions.udf
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Gaurav.Maheshwari on 4/20/2017.
  */
class EmailNameMatchTest extends FlatSpec with Matchers {

  case class EmailNameSource(email_address: String, first_name: String, last_name: String)


  "Email and Name Match Function" should "generate the Match results correctly" in {

    val sqlContext = getSQLContext
    val sourceNameFormatUDF = UDF(udf(NameFormat("", _: String, "", _: String, "")),
      Seq("first_name", "last_name"))

    val dummySource = Seq(
      EmailNameSource("SHERLOCK@GMAIL.com", "", ""),
      EmailNameSource("sherlock_holmes@gmail.com", "Sherlock", "Holmes"),
      EmailNameSource("SHERLOCK@GMAIL.com", "SherLock", "HoLmes"),
      EmailNameSource("", "Sherlock", "Holmes"),
      EmailNameSource("", "", ""))

    val source = sqlContext.createDataFrame[EmailNameSource](dummySource).transform(CMColumns)
    val emailNameMatch = new EmailNameMatch(sourceNameFormatUDF)
    val matchResults = source.transform(emailNameMatch.matchFunction)
    matchResults.show()
    matchResults.filter(s"$MatchStatusColumn = true").count should be(1)
    matchResults.filter(s"$MatchStatusColumn = false").count should be(4)
  }

}