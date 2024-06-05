package com.express.cm.criteria

import com.express.cdw._
import com.express.cm.TestSparkContext._
import com.express.cm.{NameFormat, UDF}
import org.apache.spark.sql.functions.udf
import org.scalatest.{FlatSpec, Matchers}

/**
  * Employee ID match criteria test
  *
  * @author akshay rochwani
  */

class PhoneEmailImpliedLoyaltyMatchTest extends FlatSpec with Matchers {

  case class PhoneEmailSource(phone: String, email_address: String, first_name: String, last_name: String)

  "Phone Email Match Function" should "generate the Match results correctly" in {

    val sqlContext = getSQLContext

    val dummySource = Seq(
      PhoneEmailSource("123456789", "Sherlock@GMAIL.com", "", ""),
      PhoneEmailSource("123456789", "Sherlock@GMAIL.com", "Sherlock", "Holmes"),
      PhoneEmailSource("", "Sherlock@GMAIL.com", "Sherlock", "Holmes"),
      PhoneEmailSource("123456789", "", "Sherlock", "Holmes"),
      PhoneEmailSource("", "", "Sherlock", "Holmes"),
      PhoneEmailSource("11111111", "Sherlock@GMAIL.com", "", ""),
      PhoneEmailSource("981234567", "WatsoN@gmail.com", "", "")
    )

    val sourceNameFormatUDF = UDF(udf(NameFormat("", _: String, "", _: String, "")),
      Seq("first_name", "last_name"))
    val phoneEmailMatch = new PhoneEmailImpliedLoyaltyMatch(sourceNameFormatUDF)
    import phoneEmailMatch._

    val source = sqlContext.createDataFrame[PhoneEmailSource](dummySource).transform(CMColumns)

    // run match function
    val matchResults = source.transform(matchFunction)
    matchResults.filter(s"$MatchStatusColumn = true").count should be(0)
    matchResults.filter(s"$MatchStatusColumn = false").count should be(7)
    //matchResults.filter(s"phone = '123456789'").head().getAs[Int](MemberKeyColumn) should be(1001)
    matchResults.show()
  }

}