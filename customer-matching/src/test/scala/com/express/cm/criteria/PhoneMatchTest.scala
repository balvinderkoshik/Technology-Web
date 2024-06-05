package com.express.cm.criteria

import com.express.cdw._
import com.express.cm.TestSparkContext._
import com.express.cm.{NameFormat, UDF}
import org.apache.spark.sql.functions.udf
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by bhautik.patel on 11/04/17.
  */
class PhoneMatchTest extends FlatSpec with Matchers {

  case class PhoneSource(phone: Int)
  case class PhoneNameSource(phone: Int, first_name: String, last_name: String)


  "Phone Match Function" should "generate the Match results correctly" in {
    val sqlContext = getSQLContext
    val dummySource = Seq(PhoneSource(123456789), PhoneSource(981234567), PhoneSource(981234000))
    val source = sqlContext.createDataFrame[PhoneSource](dummySource).transform(CMColumns)
    val phoneMatch = new PhoneMatch()
    val matchResults = source.transform(phoneMatch.matchFunction)
    matchResults.filter(s"$MatchStatusColumn = true").count should be(0)
    matchResults.filter(s"$MatchStatusColumn = false").count should be(3)
    //matchResults.filter("phone = 123456789").head().getAs[Int](MemberKeyColumn) should be(1001)
    matchResults.show()
  }


  "Phone Match Function with name information in dataset" should "generate the Match results correctly" in {
    val sqlContext = getSQLContext
    val sourceNameFormatUDF = UDF(udf(NameFormat("", _: String, "", _: String, "")),
      Seq("first_name", "last_name"))

    val dummySource = Seq(
      PhoneNameSource(123456789, "", ""),
      PhoneNameSource(123456789, "Sherlock", "Holmes"),
      PhoneNameSource(987654323, "Sherlock", "Holmes"),
      PhoneNameSource(981234567, "", ""),
      PhoneNameSource(981234563, "", ""))

    val source = sqlContext.createDataFrame[PhoneNameSource](dummySource).transform(CMColumns)
    val phoneMatch = new PhoneMatch(Some(sourceNameFormatUDF))
    val matchResults = source.transform(phoneMatch.matchFunction)
    matchResults.filter(s"$MatchStatusColumn = true").count should be(0)
    matchResults.filter(s"$MatchStatusColumn = false").count should be(5)
    //matchResults.filter("phone = 123456789").head().getAs[Int](MemberKeyColumn) should be(1001)
    matchResults.show()
  }

}
