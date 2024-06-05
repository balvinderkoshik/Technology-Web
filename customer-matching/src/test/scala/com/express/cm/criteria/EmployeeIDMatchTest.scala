package com.express.cm.criteria

import com.express.cdw._
import com.express.cm.TestSparkContext._
import com.express.cm.criteria.EmployeeIDMatch._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Employee ID match criteria test
  *
  * @author mbadgujar
  */
class EmployeeIDMatchTest extends FlatSpec with Matchers {

  case class EmpSource(employee_id: Int)

  case class EmpDim(employee_id: Int, emp_name: String, member_key: Int, status: String)

  "Employee ID Match Function" should "generate the Match results correctly" in {

    val sqlContext = getSQLContext
    val dummySource = Seq(EmpSource(1), EmpSource(2), EmpSource(5))

    val source = sqlContext.createDataFrame[EmpSource](dummySource).transform(CMColumns)

    // run match function
    val matchResults = source.transform(matchFunction)
    matchResults.filter(s"$MatchStatusColumn = true").count should be(2)
    matchResults.filter(s"$MatchStatusColumn = false").count should be(1)
    matchResults.filter(s"employee_id = 1").head().getAs[Int](MemberKeyColumn) should be(1001)
    matchResults.show()
  }

}
