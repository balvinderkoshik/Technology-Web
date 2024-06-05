package com.express.dedup.rules

import java.sql.{Date, Timestamp}

import com.express.dedup.model.Member
import org.scalatest.{FlatSpec, Matchers}

/**
  *
  * @author mbadgujar
  */
class MemberResolutionTest extends FlatSpec with Matchers {
  val nul_vl : Int  = Int.unbox(null)

  "NameAddressResolution function" should "resolve correctly on basis of overlay rank key" in {
    val memberList = Seq(
      new Member(member_key  =Some(1),  household_key  =Some(1), source_overlay_rank  =Some(1), last_updated_date  =Some(  Timestamp.valueOf("2018-01-01 00:00:00"))),
      new Member(member_key  =Some(2), household_key  =Some(2), source_overlay_rank  =Some(2), last_updated_date  =Some(Timestamp.valueOf("2018-01-01 00:00:00"))),
      new Member(member_key  =Some(3), household_key  =Some(3), source_overlay_rank  =Some(3), last_updated_date = Some(Timestamp.valueOf("2018-01-01 00:00:00")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateNameAddressFields()
    mr.collapsedMember.household_key shouldBe Some(1)
  }

  "NameAddressResolution function" should "resolve correctly on basis of null overlay rank key" in {
    val memberList = Seq(
      new Member(member_key  =Some(1),  household_key  =Some(1), source_overlay_rank  =Some(nul_vl ), last_updated_date  =Some(Timestamp.valueOf("2018-01-01 00:00:00"))),
      new Member(member_key  =Some(2), household_key  =Some(2), source_overlay_rank  =Some(-1), last_updated_date  =Some(Timestamp.valueOf("2018-01-01 00:00:00"))),
      new Member(member_key  =Some(3), household_key  =Some(3), source_overlay_rank  =Some(1), last_updated_date = Some(Timestamp.valueOf("2018-01-01 00:00:00")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateNameAddressFields()
    mr.collapsedMember.household_key shouldBe Some(3)
  }

  "NameAddressResolution function" should "resolve correctly on basis of dpv_key" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),  household_key  =Some(1), source_overlay_rank  =Some(1), dpv_key =Some(nul_vl),last_updated_date  =Some(Timestamp.valueOf("2018-01-01 00:00:00"))),
      new Member(member_key  =Some(2), household_key  =Some(2), source_overlay_rank  =Some(1), dpv_key  =Some(-1l), last_updated_date  =Some(Timestamp.valueOf("2018-02-01 00:00:00"))),
      new Member(member_key  =Some(3), household_key  =Some(3), source_overlay_rank  =Some(1), dpv_key =Some(1),last_updated_date = Some(Timestamp.valueOf("2018-01-01 00:00:00")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateNameAddressFields()
    mr.collapsedMember.household_key shouldBe Some(3)
  }

  "NameAddressResolution function" should "resolve correctly on basis null of dpv_key" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),  household_key  =Some(1), source_overlay_rank  =Some(1), dpv_key =Some(nul_vl),last_updated_date  =Some(Timestamp.valueOf("2018-01-01 00:00:00"))),
      new Member(member_key  =Some(2), household_key  =Some(2), source_overlay_rank  =Some(1), dpv_key  =Some(-1l), last_updated_date  =Some(Timestamp.valueOf("2018-02-01 00:00:00"))),
      new Member(member_key  =Some(3), household_key  =Some(3), source_overlay_rank  =Some(3), dpv_key =Some(-1l),last_updated_date = Some(Timestamp.valueOf("2018-01-01 00:00:00")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateNameAddressFields()
    mr.collapsedMember.household_key shouldBe Some(2)
  }


  "NameAddressResolution function" should "resolve correctly on basis of last_updated_date" in {
    val memberList = Seq(
      new Member(member_key  =Some(1),  household_key  =Some(1), source_overlay_rank  =Some(1), dpv_key =Some(-1l),last_updated_date  =Some(Timestamp.valueOf("2018-01-01 00:00:00"))),
      new Member(member_key  =Some(2), household_key  =Some(2), source_overlay_rank  =Some(1), dpv_key  =Some(-1l), last_updated_date  =Some(Timestamp.valueOf("2018-02-01 00:00:00"))),
      new Member(member_key  =Some(3), household_key  =Some(3), source_overlay_rank  =Some(3), dpv_key =Some(-1l),last_updated_date = Some(Timestamp.valueOf("2018-03-01 00:00:00")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateNameAddressFields()
    mr.collapsedMember.household_key shouldBe Some(2)
  }

  "NameAddressResolution function" should "resolve correctly on basis of same last_updated_date" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),  household_key  =Some(2), source_overlay_rank  =Some(1), dpv_key =Some(-1l),last_updated_date  =Some(Timestamp.valueOf("2018-02-01 00:00:00"))),
      new Member(member_key  =Some(2), household_key  =Some(1), source_overlay_rank  =Some(1), dpv_key  =Some(-1l), last_updated_date  =Some(Timestamp.valueOf("2018-02-01 00:00:00"))),
      new Member(member_key  =Some(3), household_key  =Some(3), source_overlay_rank  =Some(3), dpv_key =Some(-1l),last_updated_date = Some(Timestamp.valueOf("2018-03-01 00:00:00")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateNameAddressFields()
    mr.collapsedMember.household_key shouldBe Some(2)
  }

  "OverlayFieldResolution function" should "resolve correctly on basis of overlay rank" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),  source_overlay_rank  =Some(1), overlay_load_id  =Some(1), overlay_date  =Some(Date.valueOf("2018-02-01"))),
      new Member(member_key  =Some(2),  source_overlay_rank  =Some(2), overlay_load_id  =Some(2), overlay_date  =Some(Date.valueOf("2018-02-01"))),
      new Member(member_key  =Some(3),  source_overlay_rank  =Some(3), overlay_load_id  =Some(3 ),overlay_date =Some(Date.valueOf("2018-03-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateOverlayFields()
    mr.collapsedMember.overlay_load_id shouldBe Some(1)
  }

  "OverlayFieldResolution function" should "resolve correctly on basis of same overlay rank" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),  source_overlay_rank  =Some(1), overlay_load_id  =Some(1), overlay_date  =Some(Date.valueOf("2018-02-01"))),
      new Member(member_key  =Some(2),  source_overlay_rank  =Some(1), overlay_load_id  =Some(2), overlay_date  =Some(Date.valueOf("2018-02-01"))),
      new Member(member_key  =Some(3),  source_overlay_rank  =Some(1), overlay_load_id  =Some(3 ),overlay_date =Some(Date.valueOf("2018-03-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateOverlayFields()
    mr.collapsedMember.overlay_load_id shouldBe Some(1)
  }

  "OverlayFieldResolution function" should "resolve correctly on basis of double overlay rank" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),  source_overlay_rank  =Some(1), overlay_load_id  =Some(1), overlay_date  =Some(Date.valueOf("2018-02-01"))),
      new Member(member_key  =Some(2),  source_overlay_rank  =Some(1), overlay_load_id  =Some(2), overlay_date  =Some(Date.valueOf("2018-02-01"))),
      new Member(member_key  =Some(3),  source_overlay_rank  =Some(3), overlay_load_id  =Some(3 ),overlay_date =Some(Date.valueOf("2018-03-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateOverlayFields()
    mr.collapsedMember.overlay_load_id shouldBe Some(1)
  }

  "SourceInfoResolution function" should "resolve correctly on basis of customer_introduction_date" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),  original_source_key  =Some(1), customer_introduction_date  =Some(Date.valueOf("2018-02-01"))),
      new Member(member_key  =Some(2),  original_source_key  =Some(2), customer_introduction_date  =Some(Date.valueOf("2018-02-01"))),
      new Member(member_key  =Some(3),  original_source_key  =Some(3), customer_introduction_date =Some(Date.valueOf("2018-03-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateOriginalSourceRecordInfoFields()
    mr.collapsedMember.original_source_key shouldBe Some(1)
  }

  "SourceInfoResolution function" should "resolve correctly on basis of null customer_introduction_date" in {

    val memberList = Seq(
      new Member(member_key  =Some(1), original_source_key  =Some(1), customer_introduction_date  =None),
      new Member(member_key  =Some(2), original_source_key  =Some(2), customer_introduction_date  =None),
      new Member(member_key  =Some(3), original_source_key  =Some(3), customer_introduction_date = Some(Date.valueOf("2018-03-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateOriginalSourceRecordInfoFields()
    mr.collapsedMember.original_source_key shouldBe Some(3)
  }

  "SourceInfoResolution function" should "resolve correctly on basis of all null customer_introduction_date" in {

    val memberList = Seq(
      new Member(member_key  =Some(1), original_source_key  =Some(1), customer_introduction_date  =None),
      new Member(member_key  =Some(2), original_source_key  =Some(2), customer_introduction_date  =None),
      new Member(member_key  =Some(3), original_source_key  =Some(3), customer_introduction_date = None)
    )
    val mr = new MemberResolution(memberList)
    mr.populateOriginalSourceRecordInfoFields()
    mr.collapsedMember.original_source_key shouldBe Some(1)
  }

  "RecordInfo function" should "resolve correctly on basis of last_updated_date" in {

    val memberList = Seq(
      new Member(member_key  =Some(1), current_source_key  =Some(1), last_updated_date  =Some(Timestamp.valueOf("2018-02-01 00:00:00") )),
      new Member(member_key  =Some(2), current_source_key  =Some(2), last_updated_date  =Some(Timestamp.valueOf("2018-02-02 00:00:00") )),
      new Member(member_key  =Some(3), current_source_key  =Some(3), last_updated_date = Some(Timestamp.valueOf("2018-03-01 00:00:00")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateSourceRecordInfoFields()
    mr.collapsedMember.current_source_key shouldBe Some(3)
  }

  "RIDCustomer function" should "resolve correctly on basis of many conditions" in {

    val memberList = Seq(
      new Member(member_key  =Some(1), rid  =Some("1"), ecommerce_customer_nbr =Some("345"), legacy_customer_nbr =Some("123"),ean =Some("1"),customer_add_date  =Some(Date.valueOf("2018-02-01") )),
      new Member(member_key  =Some(2), rid  =Some("2"), ecommerce_customer_nbr =Some("123"), legacy_customer_nbr =Some("345"),ean =Some("2"), customer_add_date  =Some(Date.valueOf("2018-02-02"))),
      new Member(member_key  =Some(3), rid  =Some("3"), ecommerce_customer_nbr =Some("456"), legacy_customer_nbr =Some("456"), ean =Some("3"),customer_add_date = Some(Date.valueOf("2018-03-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateRIDCustomerFields()
    mr.collapsedMember.rid shouldBe Some("1")
    mr.collapsedMember.ecommerce_customer_nbr shouldBe Some("345")
    mr.collapsedMember.legacy_customer_nbr shouldBe Some("123")
    mr.collapsedMember.ean shouldBe Some("1")
    mr.collapsedMember.customer_add_date shouldBe Some(Date.valueOf("2018-02-01"))

  }

  "RIDCustomer function" should "resolve correctly on basis of null rids" in {

    val memberList = Seq(
      new Member(member_key  =Some(1), rid  =Some(""), ecommerce_customer_nbr =Some("345"), legacy_customer_nbr =Some("123"),ean =Some("1"),customer_add_date  =Some(Date.valueOf("2018-02-01") )),
      new Member(member_key  =Some(2), rid  =Some("2"), ecommerce_customer_nbr =Some("123"), legacy_customer_nbr =Some("345"),ean =Some("2"), customer_add_date  =Some(Date.valueOf("2018-02-02"))),
      new Member(member_key  =Some(3), rid  =Some("3"), ecommerce_customer_nbr =Some("456"), legacy_customer_nbr =Some("456"), ean =Some("3"),customer_add_date = Some(Date.valueOf("2018-03-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateRIDCustomerFields()
    mr.collapsedMember.rid shouldBe Some("")
    mr.collapsedMember.ecommerce_customer_nbr shouldBe Some("345")
    mr.collapsedMember.legacy_customer_nbr shouldBe Some("123")
    mr.collapsedMember.ean shouldBe Some("1")
    mr.collapsedMember.customer_add_date shouldBe Some(Date.valueOf("2018-02-01"))

  }

  "RIDCustomer function" should "resolve correctly on basis of all null rids" in {

    val memberList = Seq(
      new Member(member_key  =Some(1), rid  =None, ecommerce_customer_nbr =Some("345"), legacy_customer_nbr =Some("123"),ean =Some("1"),customer_add_date  =Some(Date.valueOf("2018-02-01") )),
      new Member(member_key  =Some(2), rid  =None, ecommerce_customer_nbr =Some("123"), legacy_customer_nbr =Some("345"),ean =Some("2"), customer_add_date  =Some(Date.valueOf("2018-02-02"))),
      new Member(member_key  =Some(3), rid  =None, ecommerce_customer_nbr =Some("456"), legacy_customer_nbr =Some("456"), ean =Some("3"),customer_add_date = Some(Date.valueOf("2018-03-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateRIDCustomerFields()
    mr.collapsedMember.rid shouldBe None
    mr.collapsedMember.ecommerce_customer_nbr shouldBe Some("345")
    mr.collapsedMember.legacy_customer_nbr shouldBe Some("123")
    mr.collapsedMember.ean shouldBe Some("1")
    mr.collapsedMember.customer_add_date shouldBe Some(Date.valueOf("2018-02-01"))

  }

  "RIDCustomer function" should "resolve correctly on basis of null customer nbrs" in {

    val memberList = Seq(
      new Member(member_key  =Some(1), rid  =Some("1"), ecommerce_customer_nbr =None, legacy_customer_nbr =Some("123"),ean =Some("1"),customer_add_date  =Some(Date.valueOf("2018-02-01") )),
      new Member(member_key  =Some(0), rid  =Some("2"), ecommerce_customer_nbr =Some("123"), legacy_customer_nbr =None,ean =Some("2"), customer_add_date  =Some(Date.valueOf("2018-02-02"))),
      new Member(member_key  =Some(3), rid  =Some("3"), ecommerce_customer_nbr =None, legacy_customer_nbr =None, ean =Some("3"),customer_add_date = Some(Date.valueOf("2018-03-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateRIDCustomerFields()
    mr.collapsedMember.rid shouldBe Some("2")
    mr.collapsedMember.ecommerce_customer_nbr shouldBe Some("123")
    mr.collapsedMember.legacy_customer_nbr shouldBe None
    mr.collapsedMember.ean shouldBe Some("2")
    mr.collapsedMember.customer_add_date shouldBe Some(Date.valueOf("2018-02-02"))

  }

  "RIDCustomer function" should "resolve correctly on basis of all null customer nbrs" in {

    val memberList = Seq(
      new Member(member_key  =Some(1), rid  =Some("1"), ecommerce_customer_nbr =None, legacy_customer_nbr =None,ean =Some("1"),customer_add_date  =Some(Date.valueOf("2018-02-01") )),
      new Member(member_key  =Some(2), rid  =Some("2"), ecommerce_customer_nbr =None, legacy_customer_nbr =None,ean =Some("2"), customer_add_date  =Some(Date.valueOf("2018-02-02"))),
      new Member(member_key  =Some(3), rid  =Some("3"), ecommerce_customer_nbr =None, legacy_customer_nbr =None, ean =Some("3"),customer_add_date = Some(Date.valueOf("2018-03-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateRIDCustomerFields()
    mr.collapsedMember.rid shouldBe Some("1")
    mr.collapsedMember.ecommerce_customer_nbr shouldBe None
    mr.collapsedMember.legacy_customer_nbr shouldBe None
    mr.collapsedMember.ean shouldBe Some("1")
    mr.collapsedMember.customer_add_date shouldBe Some(Date.valueOf("2018-02-01"))

  }

  "RIDCustomer function" should "resolve correctly on basis of null ean" in {

    val memberList = Seq(
      new Member(member_key  =Some(1), rid  =Some("1"), ecommerce_customer_nbr =Some("345"), legacy_customer_nbr =Some("123"),ean =None,customer_add_date  =Some(Date.valueOf("2018-02-01") )),
      new Member(member_key  =Some(2), rid  =Some("2"), ecommerce_customer_nbr =Some("123"), legacy_customer_nbr =Some("345"),ean =None, customer_add_date  =Some(Date.valueOf("2018-02-02"))),
      new Member(member_key  =Some(3), rid  =Some("3"), ecommerce_customer_nbr =Some("456"), legacy_customer_nbr =Some("456"), ean =Some("3"),customer_add_date = Some(Date.valueOf("2018-03-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateRIDCustomerFields()
    mr.collapsedMember.rid shouldBe Some("1")
    mr.collapsedMember.ecommerce_customer_nbr shouldBe Some("345")
    mr.collapsedMember.legacy_customer_nbr shouldBe Some("123")
    mr.collapsedMember.ean shouldBe None
    mr.collapsedMember.customer_add_date shouldBe Some(Date.valueOf("2018-02-01"))

  }

  "RIDCustomer function" should "resolve correctly on basis of blank ean" in {

    val memberList = Seq(
      new Member(member_key  =Some(1), rid  =Some("1"), ecommerce_customer_nbr =Some("345"), legacy_customer_nbr =Some("123"),ean =Some(""),customer_add_date   =Some(Date.valueOf("2018-02-01"))),
      new Member(member_key  =Some(2), rid  =Some("2"), ecommerce_customer_nbr =Some("123"), legacy_customer_nbr =Some("345"),ean =Some(""), customer_add_date  =Some(Date.valueOf("2018-02-02"))),
      new Member(member_key  =Some(3), rid  =Some("3"), ecommerce_customer_nbr =Some("456"), legacy_customer_nbr =Some("456"), ean =Some("3"),customer_add_date = Some(Date.valueOf("2018-03-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateRIDCustomerFields()
    mr.collapsedMember.rid shouldBe Some("1")
    mr.collapsedMember.ecommerce_customer_nbr shouldBe Some("345")
    mr.collapsedMember.legacy_customer_nbr shouldBe Some("123")
    mr.collapsedMember.ean shouldBe Some("")
    mr.collapsedMember.customer_add_date shouldBe Some(Date.valueOf("2018-02-01"))

  }

  "RIDCustomer function" should "resolve correctly on basis of all null ean " in {

    val memberList = Seq(
      new Member(member_key  =Some(1), rid  =Some("1"), ecommerce_customer_nbr =Some("345"), legacy_customer_nbr =Some("123"),ean =None,customer_add_date  =Some(Date.valueOf("2018-02-01") )),
      new Member(member_key  =Some(2), rid  =Some("2"), ecommerce_customer_nbr =Some("123"), legacy_customer_nbr =Some("345"),ean =None, customer_add_date  =Some(Date.valueOf("2018-02-02"))),
      new Member(member_key  =Some(3), rid  =Some("3"), ecommerce_customer_nbr =Some("456"), legacy_customer_nbr =Some("456"), ean =None,customer_add_date = Some(Date.valueOf("2018-03-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateRIDCustomerFields()
    mr.collapsedMember.rid shouldBe Some("1")
    mr.collapsedMember.ecommerce_customer_nbr shouldBe Some("345")
    mr.collapsedMember.legacy_customer_nbr shouldBe Some("123")
    mr.collapsedMember.ean shouldBe None
    mr.collapsedMember.customer_add_date shouldBe Some(Date.valueOf("2018-02-01"))

  }

  "RIDCustomer function" should "resolve correctly on basis of all null customer_add_date" in {

    val memberList = Seq(
      new Member(member_key  =Some(1), rid  =None, ecommerce_customer_nbr =Some("345"), legacy_customer_nbr =Some("123"),ean =Some("1"),customer_add_date  =None),
      new Member(member_key  =Some(2), rid  =None, ecommerce_customer_nbr =Some("123"), legacy_customer_nbr =Some("345"),ean =Some("2"), customer_add_date  =None),
      new Member(member_key  =Some(3), rid  =None, ecommerce_customer_nbr =Some("456"), legacy_customer_nbr =Some("456"), ean =Some("3"),customer_add_date = Some(Date.valueOf("2018-03-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateRIDCustomerFields()
    mr.collapsedMember.rid shouldBe None
    mr.collapsedMember.ecommerce_customer_nbr shouldBe Some("345")
    mr.collapsedMember.legacy_customer_nbr shouldBe Some("123")
    mr.collapsedMember.ean shouldBe Some("1")
    mr.collapsedMember.customer_add_date shouldBe None

  }

  "IndicatorFields function" should "resolve correctly on basis of current source_key and open_close_ind " in {

    val memberList = Seq(
      new Member(member_key  =Some(1), open_close_ind =Some(Array("O","C")), ads_do_not_statement_insert =Some("ABC"),current_source_key =Some(8),last_updated_date  =Some(Timestamp.valueOf("2018-02-01 00:00:00"))),
      new Member(member_key  =Some(2), open_close_ind =Some(Array("C")), ads_do_not_statement_insert =Some("DEF"),current_source_key =Some(9),last_updated_date  =Some(Timestamp.valueOf("2018-02-02 00:00:00"))),
      new Member(member_key  =Some(3), open_close_ind =Some(Array("C","O")), ads_do_not_statement_insert =Some("GHI"),current_source_key =Some(6),last_updated_date = Some(Timestamp.valueOf("2018-03-01 00:00:00")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateIndicatorsFields()
    mr.collapsedMember.ads_do_not_statement_insert shouldBe Some("ABC")

  }

  "IndicatorFields function" should "resolve correctly on basis of non ads  current source_key and open_close_ind " in {

    val memberList = Seq(
      new Member(member_key  =Some(1), open_close_ind =Some(Array("O","C")), ads_do_not_statement_insert =Some("ABC"),current_source_key =Some(6),last_updated_date  =Some(Timestamp.valueOf("2018-02-01 00:00:00") )),
      new Member(member_key  =Some(2), open_close_ind =Some(Array("C")), ads_do_not_statement_insert =Some("DEF"),current_source_key =Some(300),last_updated_date  =Some(Timestamp.valueOf("2018-02-02 00:00:00"))),
      new Member(member_key  =Some(3), open_close_ind =Some(Array("C","O")), ads_do_not_statement_insert =Some("GHI"),current_source_key =Some(6),last_updated_date = Some(Timestamp.valueOf("2018-03-01 00:00:00")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateIndicatorsFields()
    mr.collapsedMember.ads_do_not_statement_insert shouldBe Some("GHI")
  }

  "DirectConsentFields function" should "resolve correctly on basis of email_consent_date " in {

    val memberList = Seq(
      new Member(member_key  =Some(1),email_consent  =Some("YES"),direct_mail_consent =Some("Yes"),email_consent_date  =Some(Date.valueOf("2018-02-01") )),
      new Member(member_key  =Some(2),email_consent  =Some("YES"),direct_mail_consent =Some("Yes"),email_consent_date  =Some(Date.valueOf("2018-02-02"))),
      new Member(member_key  =Some(3),email_consent  =Some("YES"),direct_mail_consent =Some("NO"),email_consent_date = Some(Date.valueOf("2018-03-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateDirectMailConsentField()
    mr.collapsedMember.direct_mail_consent shouldBe Some("NO")
  }

  "DirectConsentFields function" should "resolve correctly on basis of null email_consent_date " in {

    val memberList = Seq(
      new Member(member_key  =Some(1),email_consent  =Some("YES"),direct_mail_consent =Some("Yes1"),email_consent_date  =Some(Date.valueOf("2018-02-01") )),
      new Member(member_key  =Some(2),email_consent  =Some("YES"),direct_mail_consent =Some("Yes2"),email_consent_date  =Some(Date.valueOf("2018-02-02"))),
      new Member(member_key  =Some(3),email_consent  =Some("YES"),direct_mail_consent =Some("NO"),email_consent_date = None)
    )
    val mr = new MemberResolution(memberList)
    mr.populateDirectMailConsentField()
    mr.collapsedMember.direct_mail_consent shouldBe Some("NO")
  }

  "DirectConsentFields function" should "resolve correctly on basis of  email_consent " in {

    val memberList = Seq(
      new Member(member_key  =Some(1),email_consent  =Some("YES"),direct_mail_consent =Some("Yes1"),email_consent_date  =Some(Date.valueOf("2018-02-01") )),
      new Member(member_key  =Some(2),email_consent  =Some("YES"),direct_mail_consent =Some("Yes2"),email_consent_date  =Some(Date.valueOf("2018-03-02"))),
      new Member(member_key  =Some(3),email_consent  =Some("NO"),direct_mail_consent =Some("NO"),email_consent_date =Some(Date.valueOf("2018-03-02")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateDirectMailConsentField()
    mr.collapsedMember.direct_mail_consent shouldBe Some("NO")
  }

  "DirectConsentFields function" should "resolve correctly on basis of null email_consent " in {

    val memberList = Seq(
      new Member(member_key  =Some(1),email_consent  =Some("YES"),direct_mail_consent =Some("Yes1"),email_consent_date  =Some(Date.valueOf("2018-02-01") )),
      new Member(member_key  =Some(2),email_consent  =None,direct_mail_consent =Some("Yes2"),email_consent_date  =Some(Date.valueOf("2018-03-02"))),
      new Member(member_key  =Some(3),email_consent  =Some("NO"),direct_mail_consent =Some("NO"),email_consent_date =Some(Date.valueOf("2018-03-02")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateDirectMailConsentField()
    mr.collapsedMember.direct_mail_consent shouldBe Some("NO")
  }

  "PromotionConsentFields function" should "resolve correctly on basis of yes promotion_consent " in {

    val memberList = Seq(
      new Member(member_key  =Some(1),promotion_consent  =None),
      new Member(member_key  =Some(2),promotion_consent  =None),
      new Member(member_key  =Some(3),promotion_consent = Some("YES"))
    )
    val mr = new MemberResolution(memberList)
    mr.populatePromotionConsentField()
    mr.collapsedMember.promotion_consent shouldBe Some("YES")
  }

  "PromotionConsentFields function" should "resolve correctly on basis of no promotion_consent " in {

    val memberList = Seq(
      new Member(member_key  =Some(1),promotion_consent  =Some("NO" )),
      new Member(member_key  =Some(2),promotion_consent  =None),
      new Member(member_key  =Some(3),promotion_consent = Some("NO"))
    )
    val mr = new MemberResolution(memberList)
    mr.populatePromotionConsentField()
    mr.collapsedMember.promotion_consent shouldBe Some("NO")
  }

  "PromotionConsentFields function" should "resolve correctly on basis of null promotion_consent " in {

    val memberList = Seq(
      new Member(member_key  =Some(1),promotion_consent  =None),
      new Member(member_key  =Some(2),promotion_consent  =None),
      new Member(member_key  =Some(3),promotion_consent = None)
    )
    val mr = new MemberResolution(memberList)
    mr.populatePromotionConsentField()
    mr.collapsedMember.promotion_consent shouldBe Some("UNKNOWN")
  }

  "FirstTransactionFields function" should "resolve correctly on basis of first_transaction_date" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),first_trxn_date  =Some(Date.valueOf("2018-02-01")),first_trxn_store_key  =Some(3)),
      new Member(member_key  =Some(2),first_trxn_date  =Some(Date.valueOf("2018-03-01")),first_trxn_store_key  =Some(2)),
      new Member(member_key  =Some(3),first_trxn_date  =Some(Date.valueOf("2018-04-01")),first_trxn_store_key = Some(3))
    )
    val mr = new MemberResolution(memberList)
    mr.populateFirstTransactionDate()
    mr.collapsedMember.first_trxn_date shouldBe Some(Date.valueOf("2018-02-01"))
  }

  "FirstTransactionFields function" should "resolve correctly on basis of first_transaction_store_key" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),first_trxn_date  =Some(Date.valueOf("2018-02-01")),first_trxn_store_key  =Some(3)),
      new Member(member_key  =Some(2),first_trxn_date  =Some(Date.valueOf("2018-02-01")),first_trxn_store_key  =Some(2)),
      new Member(member_key  =Some(3),first_trxn_date  =Some(Date.valueOf("2018-04-01")),first_trxn_store_key = Some(3l))
    )
    val mr = new MemberResolution(memberList)
    mr.populateFirstTransactionDate()
    mr.collapsedMember.first_trxn_date shouldBe Some(Date.valueOf("2018-02-01"))
  }

  "FirstTransactionFields function" should "resolve correctly on basis of null first_transaction_date" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),first_trxn_date  =None,first_trxn_store_key  =Some(3)),
      new Member(member_key  =Some(2),first_trxn_date  =None,first_trxn_store_key  =Some(2)),
      new Member(member_key  =Some(3),first_trxn_date  =None,first_trxn_store_key = Some(3l))
    )
    val mr = new MemberResolution(memberList)
    mr.populateFirstTransactionDate()
    mr.collapsedMember.first_trxn_date shouldBe None
    mr.collapsedMember.first_trxn_store_key shouldBe Some(2)
  }

  "FirstTransactionFields function" should "resolve correctly on basis of all null first_transaction_date" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),first_trxn_date  =None,first_trxn_store_key  =Some(0)),
      new Member(member_key  =Some(2),first_trxn_date  =None,first_trxn_store_key  =Some(0)),
      new Member(member_key  =Some(3),first_trxn_date  =None,first_trxn_store_key =Some(4))
    )
    val mr = new MemberResolution(memberList)
    mr.populateFirstTransactionDate()
    mr.collapsedMember.first_trxn_date shouldBe None
    mr.collapsedMember.first_trxn_store_key shouldBe Some(4)
  }

  "lastPurchaseFields function" should "resolve correctly on basis of last_store_purch_date" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),last_store_purch_date  =Some(Date.valueOf("2018-02-01")),last_store_purch_date_key  =Some(3)),
      new Member(member_key  =Some(2),last_store_purch_date  =Some(Date.valueOf("2018-03-01")),last_store_purch_date_key  =Some(2)),
      new Member(member_key  =Some(3),last_store_purch_date  =Some(Date.valueOf("2018-04-01")),last_store_purch_date_key = Some(3l))
    )
    val mr = new MemberResolution(memberList)
    mr.populateLastPurchaseDate()
    mr.collapsedMember.last_store_purch_date_key shouldBe Some(3)
  }

  "lastPurchaseFields function" should "resolve correctly on basis of null last_store_purch_date" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),last_store_purch_date  =None,last_store_purch_date_key  =Some(3)),
      new Member(member_key  =Some(2),last_store_purch_date  =None,last_store_purch_date_key  =Some(2)),
      new Member(member_key  =Some(3),last_store_purch_date  =Some(Date.valueOf("2018-04-01")),last_store_purch_date_key = Some(1))
    )
    val mr = new MemberResolution(memberList)
    mr.populateLastPurchaseDate()
    mr.collapsedMember.last_store_purch_date_key shouldBe Some(1)
  }

  "lastPurchaseFields function" should "resolve correctly on basis of all null last_store_purch_date" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),last_store_purch_date  =None,last_store_purch_date_key  =Some(3)),
      new Member(member_key  =Some(2),last_store_purch_date  =None,last_store_purch_date_key  =Some(2)),
      new Member(member_key  =Some(3),last_store_purch_date  =None,last_store_purch_date_key = Some(3))
    )
    val mr = new MemberResolution(memberList)
    mr.populateLastPurchaseDate()
    mr.collapsedMember.last_store_purch_date_key shouldBe Some(3)
  }

  "ProspectCountFields function" should "resolve correctly on basis of all prospect counts" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),prospect_appearance_count  =Some(0l)),
      new Member(member_key  =Some(2),prospect_appearance_count  =Some(3l)),
      new Member(member_key  =Some(3),prospect_appearance_count = Some(0l))
    )
    val mr = new MemberResolution(memberList)
    mr.populateProspectCountField()
    mr.collapsedMember.prospect_appearance_count shouldBe Some(3)
  }

  "Loyaltyfields function" should "resolve correctly on basis of member_status" in {

  val memberList = Seq(
    new Member(member_key  =Some(1),member_status =Some("ACTIVE"),is_loyalty_member =Some("YES"),ip_code =Some(1234567)),
    new Member(member_key  =Some(2),member_status =Some("INACTIVE"),is_loyalty_member =Some("YES"),ip_code =Some(2345678)),
    new Member(member_key  =Some(3),member_status =None,is_loyalty_member =Some("YES"),ip_code =Some(2345677)),
    new Member(member_key  =Some(3),member_status =None,is_loyalty_member =Some("NO"),ip_code=Some(0l))
  )
  val mr = new MemberResolution(memberList)
  mr.populateMemberSocialIdAndEnrollDate()
  mr.collapsedMember.ip_code shouldBe Some(1234567)
}

  "Loyaltyfields function" should "resolve correctly on basis of trxn_date" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),member_status =Some("ACTIVE"),trxn_date =Some(Date.valueOf("2018-03-01")),member_acct_enroll_date =None,is_loyalty_member =Some("YES"),ip_code =Some(1234567)),
      new Member(member_key  =Some(2),member_status =Some("ACTIVE"),trxn_date =None,member_acct_enroll_date =None,is_loyalty_member =Some("YES"),ip_code =Some(2345678)),
      new Member(member_key  =Some(3),member_status =Some("ACTIVE"),trxn_date =Some(Date.valueOf("2018-04-01")),member_acct_enroll_date =None,is_loyalty_member =Some("YES"),ip_code =Some(2345677)),
      new Member(member_key  =Some(3),member_status =None,trxn_date =Some(Date.valueOf("2018-03-01")),member_acct_enroll_date =None,is_loyalty_member =Some("NO"),ip_code=Some(0l))
    )
    val mr = new MemberResolution(memberList)
    mr.populateMemberSocialIdAndEnrollDate()
    mr.collapsedMember.ip_code shouldBe Some(2345677)
  }

  "Loyaltyfields function" should "resolve correctly on basis of null trxn_date" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),member_status =Some("ACTIVE"),trxn_date =None,member_acct_enroll_date =None,is_loyalty_member =Some("YES"),ip_code =Some(1234567)),
      new Member(member_key  =Some(2),member_status =Some("ACTIVE"),trxn_date =None,member_acct_enroll_date =None,is_loyalty_member =Some("YES"),ip_code =Some(2345678)),
      new Member(member_key  =Some(3),member_status =Some("ACTIVE"),trxn_date =Some(Date.valueOf("2018-04-01")),member_acct_enroll_date =None,is_loyalty_member =Some("YES"),ip_code =Some(2345677)),
      new Member(member_key  =Some(3),member_status =None,trxn_date =Some(Date.valueOf("2018-03-01")),member_acct_enroll_date =None,is_loyalty_member =Some("NO"),ip_code=Some(0l))
    )
    val mr = new MemberResolution(memberList)
    mr.populateMemberSocialIdAndEnrollDate()
    mr.collapsedMember.ip_code shouldBe Some(2345677)
  }

  "Loyaltyfields function" should "resolve correctly on basis of enroll_date" in {

    val memberList = Seq(
      new Member(member_key  =Some(1),member_status =Some("ACTIVE"),trxn_date =None,member_acct_enroll_date =Some(Date.valueOf("2018-04-01")),is_loyalty_member =Some("YES"),ip_code =Some(1234567)),
      new Member(member_key  =Some(2),member_status =Some("ACTIVE"),trxn_date =None,member_acct_enroll_date =Some(Date.valueOf("2018-03-01")),is_loyalty_member =Some("YES"),ip_code =Some(2345678)),
      new Member(member_key  =Some(3),member_status =Some("ACTIVE"),trxn_date =None,member_acct_enroll_date =None,is_loyalty_member =Some("YES"),ip_code =Some(2345677)),
      new Member(member_key  =Some(3),member_status =None,trxn_date =Some(Date.valueOf("2018-03-01")),member_acct_enroll_date =None,is_loyalty_member =Some("NO"),ip_code=Some(0l))
    )
    val mr = new MemberResolution(memberList)
    mr.populateMemberSocialIdAndEnrollDate()
    mr.collapsedMember.ip_code shouldBe Some(2345678)

  }

  "populateBirthDateField function" should "resolve correctly on basis of birth_date" in {

    val memberList = Seq(
      new Member(member_key =Some(1),birth_date=Some(Date.valueOf("2018-04-01"))),
      new Member(member_key  =Some(2),birth_date=Some(Date.valueOf("2018-05-01"))),
      new Member(member_key  =Some(3),birth_date=Some(Date.valueOf("2018-03-03"))),
      new Member(member_key  =Some(4),birth_date=Some(Date.valueOf("2018-07-01")))
    )
    val mr = new MemberResolution(memberList)
    mr.populateBirthDateField()
    mr.collapsedMember.birth_date shouldBe Some(Date.valueOf("2018-03-03"))
  }

}
