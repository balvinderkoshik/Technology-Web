package com.express.cm.criteria

import com.express.cdw._
import com.express.cm.TestSparkContext._
import com.express.cm.{NameFormat, UDF}
import org.apache.spark.sql.functions.{lit, udf}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Phone Email Name match criteria test
  *
  * @author akshay rochwani
  */

class PhoneEmailNameMatchTest extends FlatSpec with Matchers {


  case class PhoneEmailNameSource(phone: String, email_address: String, first_name: String, last_name: String)

  "Phone Email & Name Match Function" should "generate the Match results correctly" in {

    val sqlContext = getSQLContext

    val dummySource = Seq(
      PhoneEmailNameSource("123456789", "Sherlock@GMAIL.com", "", ""),
      PhoneEmailNameSource("123456789", "Sherlock@GMAIL.com", "Sherlock", "Holmes"),
      PhoneEmailNameSource("", "Sherlock@GMAIL.com", "Sherlock", "Holmes"),
      PhoneEmailNameSource("123456789", "", "Sherlock", "Holmes"),
      PhoneEmailNameSource("", "", "Sherlock", "Holmes"),
      PhoneEmailNameSource("11111111", "Sherlock@GMAIL.com", "", ""),
      PhoneEmailNameSource("981234567", "WatsoN@gmail.com", "", "")
    )

    val sourceNameFormatUDF = UDF(udf(NameFormat("", _: String, "", _: String, "")),
      Seq("first_name", "last_name"))

    val phoneEmailNameMatch = new PhoneEmailNameMatch(sourceNameFormatUDF)

    import phoneEmailNameMatch._
    val source = sqlContext.createDataFrame[PhoneEmailNameSource](dummySource).transform(CMColumns)
;
    // run match function
    val matchResults = source.transform(matchFunction)
    matchResults.filter(s"$MatchStatusColumn = true").count should be(0)
    matchResults.filter(s"$MatchStatusColumn = false").count should be(7)
    matchResults.show()
  }

}
