package com.express.cm.criteria


import com.express.cdw._
import com.express.cm.TestSparkContext._
import com.express.cm.{NameFormat, UDF}
import org.apache.spark.sql.functions.udf
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by bhautik.patel on 11/04/17.
  */
class PhoneNameMatchTest extends FlatSpec with Matchers {

  case class EmpPhoneNameSource(phone: Int, first_name: String, last_name: String)

  val dummySource = Seq(
    EmpPhoneNameSource(123456789, "Sherlock", "Holmes"),
    EmpPhoneNameSource(987654323, "SHerlock", "holmes"),
    EmpPhoneNameSource(123456789, "", "Holmes"),
    EmpPhoneNameSource(123456789, "Sherlockk", "Holmes"),
    EmpPhoneNameSource(981234567, "John", "WatSon"),
    EmpPhoneNameSource(981234500, "John", "WatSon"),
    EmpPhoneNameSource(987654300, "John", "WatSon")
  )


  "Phone Name Match  Function" should "generate the Match results correctly" in {


    val sqlContext = getSQLContext
    val source = sqlContext.createDataFrame[EmpPhoneNameSource](dummySource).transform(CMColumns)

    val sourceNameFormatUDF = UDF(udf(NameFormat("", _: String, "", _: String, "")), Seq("first_name", "last_name"))

    val phoneNoNameMatch = new PhoneNameMatch(sourceNameFormatUDF)
    // run match function
    val matchResults = source.transform(phoneNoNameMatch.matchFunction)
    matchResults.filter(s"$MatchStatusColumn = true").count should be(0)
    matchResults.filter(s"$MatchStatusColumn = false").count should be(7)
    //matchResults.filter("phone = 123456789").head().getAs[Int](MemberKeyColumn) should be(1001)
    matchResults.show()
  }
}
