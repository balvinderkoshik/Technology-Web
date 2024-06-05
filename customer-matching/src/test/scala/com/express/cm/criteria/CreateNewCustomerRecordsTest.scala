package com.express.cm.criteria

import com.express.cm.TestSparkContext._
import com.express.cm.criteria.CreateNewCustomerRecords._
import com.express.cm.MCDStatuses._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by poonam.mishra on 4/24/2017.
  */
class CreateNewCustomerRecordsTest extends FlatSpec with Matchers {

  case class unmatched(id:Integer,email_address: String, first_name: String, last_name: String, address_line_1: String,
                       zip_code: String, non_usa_postal_code: String, phone: String, implied_loyalty_id: String,tender_number:String)

  "Create New Customer Record" should "generate the new Member Key results correctly" in {
    val sqlContext = getSQLContext
    val dummySource = Seq(
      unmatched(1,"abc.gjdgjs@gmail.com", "John", "Snow", "E-1218,Sector-1", "5678892", "578678689", "9876786786", "123445","123213123"),
      unmatched(2,"abc.gjdgjs@yahoo.com", "Steve", "Glow", "E-1218,Sector-1", "5678892", "689789", "8989078689", null,"123123123"),
      unmatched(3,null, null, "Stark", "E-1218,Sector-1", "5678892", "689789", null, null,null),
      unmatched(4,"abc.xyz@gmail.com","Matt","Scott","E-1218,Sector-23","123123","123124324","23179127391",null,null),
      unmatched(5,"abc.xyz@gmail.com","Matt","Scott",null,null,null,"23179127391",null,null),
      unmatched(6,"abc.xyz@gmail.com","Matt","Scott",null,null,null,"23179127391",null,null),
      unmatched(7,"abc.xyz@gmail.com","Steve","Scott",null,null,null,"23179127391",null,null),
      unmatched(8,null,null,null,null,null,null,"23179127391",null,null),
      unmatched(9,null,null,null,null,null,null,null,null,"21039120312"),
      unmatched(10,"abc.xyz@gmail.com","Matt","Scott","E-1218,Sector-23","123123","123124324","23179127391",null,"21039120312"),
      unmatched(11,"abc.xyz@gmail.com",null,null,"East Street",null,null,"23179127391",null,"21039120312"),
      unmatched(12,"abc.xyz@gmail.com",null,null,"East Street",null,null,"23179127391",null,"21039120312"),
      unmatched(13,"abc.xyz@gmail.com",null,null,"East Street,Sector-3",null,null,"23179127391",null,"21039120312"),
      unmatched(14,null, null, "Stark", "E-1218,Sector-1", "5678892", "689789", "9861039661", null,null),
      unmatched(15,null, null, "Stark", "E-1218,Sector-1", "5678892", "689789", "9861039661", null,null),
      unmatched(16,null, null, "Johnson", "EastSector-1", "5678892", "689789", "9861039661", null,null),
      unmatched(17,null, null, "Stark", "EastSector-1", "5678892", "689789", "9861039661", null,null))
    val ummatched = sqlContext.createDataFrame[unmatched](dummySource)
    val matchResults = create(ummatched)
    matchResults.show()
    matchResults.filter("id = 3").collect().head.getAs[Long]("member_key") should be(-1)
    matchResults.filter("id = 1").collect().head.getAs[String](MCDCriteria) should be(MCDLoyalty)
    matchResults.filter("id = 2").collect().head.getAs[String](MCDCriteria) should be(MCDNameAddressUS)
    matchResults.filter("id = 11").collect().head.getAs[String](MCDCriteria) should be(MCDBankCard)
    matchResults.filter("id = 11 or id = 12").collect().map(_.getAs[Long]("member_key")).distinct.length should be(1)
    matchResults.filter("id = 14 or id = 15").collect().map(_.getAs[Long]("member_key")).distinct.length should be(1)
    matchResults.filter("id = 16 ").collect().head.getAs[String](MCDCriteria) should be(MCDPhone)

  }
}