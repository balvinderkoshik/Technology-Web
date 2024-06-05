package com.express.cm.criteria

import com.express.cdw._
import com.express.cm.{NameFormat, UDF}
import com.express.cm.TestSparkContext._
import org.apache.spark.sql.functions.udf
import org.scalatest.{FlatSpec, Matchers}

/**
  * EMail and Name match criteria test
  *
  * @author gmaheshwari
  */
class EmailMatchTest extends FlatSpec with Matchers {

  case class EmailSource(email_address: String)
  case class EmailNameSource(email_address: String, first_name: String, last_name: String)


  "EmailMatch Function" should "generate the Match results correctly" in {
    val sqlContext = getSQLContext
    val dummySource = Seq(EmailSource("SHERLOCK@GMAIL.com"), EmailSource("sherlock_holmes@gmail.com"))
    val source = sqlContext.createDataFrame[EmailSource](dummySource).transform(CMColumns)
    // run match function
    val emailMatch = new EmailMatch()
    val matchResults = source.transform(emailMatch.matchFunction)
    matchResults.show()
    matchResults.filter(s"$MatchStatusColumn = true").count should be(1)
  }

  "EmailMatch Function with name information in dataset" should "generate the Match results correctly" in {
    val sqlContext = getSQLContext
    val sourceNameFormatUDF = UDF(udf(NameFormat("", _: String, "", _: String, "")),
      Seq("first_name", "last_name"))

    val dummySource = Seq(
      EmailNameSource("SHERLOCK@GMAIL.com", "", ""),
      EmailNameSource("sherlock_holmes@gmail.com", "Sherlock", "Holmes"),
      EmailNameSource("", "Sherlock", "Holmes"),
      EmailNameSource("", "", ""))

    val source = sqlContext.createDataFrame[EmailNameSource](dummySource).transform(CMColumns)
    // run match function
    val emailMatch = new EmailMatch(Some(sourceNameFormatUDF))
    val matchResults = source.transform(emailMatch.matchFunction)
    matchResults.show()
    matchResults.filter(s"$MatchStatusColumn = true").count should be(1)
  }

}
