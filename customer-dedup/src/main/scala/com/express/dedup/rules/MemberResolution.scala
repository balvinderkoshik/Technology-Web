package com.express.dedup.rules

import java.sql.Date

import com.express.dedup.model.Member
import org.joda.time.DateTime

import scala.util.{Failure, Success, Try}


/**
  * This class contains Member resolution logic for Deduplication process
  * that works on group of member identified for a specific grouping condition
  *
  * @author mbadgujar
  */
class MemberResolution(inputMemberList: Seq[Member]) {

  private[rules] val collapsedMember = Member.getEmptyMember

  private val loyaltyMemberFilterFunc = (member: Member) => member.is_loyalty_member == Some("YES")
  private val overlayRankFunc = (member: Member) => member.source_overlay_rank match {
    case None => 99
    case Some(-1) => 99
    case Some(0) => 99
    case Some(rank) => rank
  }

  private val DPVRankFunc = (member: Member) => member.dpv_key match {
    case None => -1
    case Some(0) => -1
    case Some(key) => key
  }


  /**
    * Get Member attribute value. Returns minimum/maximum in-case of null values
    *
    * @param value        Member value
    * @param defaultIsMin In case value is null and 'defaultIsMin' is true,
    *                     then minimum value would be returned else the maximum value
    * @tparam T Value Type
    * @return value
    */
  private def getVal[T: Manifest](value: Option[T], defaultIsMin: Boolean = true): T = {
    val v = value

    {
      manifest[T] match {
        case t if t == manifest[Date] =>
          val default = if (defaultIsMin) "1900-01-01" else "9999-01-01"
          v.getOrElse(Date.valueOf(default))
        case t if t == manifest[Long] =>
          val default = if (defaultIsMin) -1l else Long.MaxValue
          v.fold(default)(t => {
            val value = t.asInstanceOf[Long]
            if (value == 0 || value == -1) default else value
          })
        case t if t == manifest[Int] =>
          val default = if (defaultIsMin) -1 else Int.MaxValue
          v.fold(default)(t => {
            val value = t.asInstanceOf[Int]
            if (value == 0 || value == -1) default else value
          })
        case _ =>
          v.getOrElse("")
      }
    }.asInstanceOf[T]
  }


  def getNumber(str: String, nullIsZero: Boolean = true): Long = {
    val default = if (nullIsZero) 0l else Long.MaxValue
    Option(str) match {
      case None => default
      case Some("") => default
      case Some(_) =>
        Try(str.toLong) match {
          case Success(number) => number
          case Failure(_) => default
        }
    }
  }


  /*
     Get the Collection ID this member list belongs to
   */
  val getCollectionID: Int = {
    if (inputMemberList.size == 1)
      -1
    else {
      val loyaltyMemberCount = inputMemberList.count(loyaltyMemberFilterFunc)
      loyaltyMemberCount match {
        case 0 => 1
        case 1 => 3
        case count if count == inputMemberList.size => 2
        case _ => 4
      }
    }
  }

  /*
     The Best member key selected for this Collection
   */
  val getBestMember: Option[Long] = {
    val collectionID = getCollectionID
    val resolvedMemberKey: Option[Long] = collectionID match {
      case -1 =>
        Some(-1l)
      case 1 =>
        val minIntroductionMember = inputMemberList.minBy(member => getVal(member.customer_introduction_date, false).getTime)
        val minIntroductionMemberList = inputMemberList.filter(mem => mem.customer_introduction_date == minIntroductionMember.customer_introduction_date)
        if (minIntroductionMemberList.size > 1) {
          minIntroductionMemberList.minBy(member => getVal(member.member_key,false)).member_key
        }
        else
          minIntroductionMember.member_key
      case 2 =>
        Some(-1l)
      case 3 =>
        inputMemberList.filter(loyaltyMemberFilterFunc).head.member_key
      case 4 =>
        val activeLoyaltyMember = inputMemberList.filter(_.member_status == Some("ACTIVE"))
        if (activeLoyaltyMember.size > 1) {
          val recentTransactionMember = activeLoyaltyMember.maxBy(member => getVal(member.trxn_date).getTime)
          val recentTransactionMemberList = activeLoyaltyMember.filter(mem => mem.trxn_date == recentTransactionMember.trxn_date)
          if (recentTransactionMemberList.size > 1)
            recentTransactionMemberList.minBy(member => getVal(member.member_acct_enroll_date, defaultIsMin = false).getTime).member_key
          else
            recentTransactionMember.member_key
        }
        else
          activeLoyaltyMember.headOption.fold(Option(-1l))(_.member_key)
    }
    resolvedMemberKey
  }

  val memberList: Seq[Member] = {
    if (getCollectionID == 4)
      inputMemberList.filter(member =>
        if (member.is_loyalty_member == Some("YES")) {
          if (member.member_key == getBestMember)
            true
          else
            false
        }
        else
          true)
    else
      inputMemberList
  }


  /**
    * Member resolution function which returns following information:
    * 1. Collections ID this member list belongs to
    * 2. Is the collections resolved
    * 3. Resolved member key (-1 in case it cannot be resolved)
    * 4. Resolved member record. (empty in case the collection is not resolved)
    *
    * @return Tuple 0f (collection ID, Resolved member key, isResoled flag, Resolved member information)
    */
  def resolveBestMember: (Int, Long, Boolean, Member) = {
    val collectionID = getCollectionID
    val resolvedMemberKey = getBestMember
    val isResolved = collectionID match {
      case 2 => false
      case -1 => false
      case _ => if (resolvedMemberKey == Some(-1l)) false else true
    }
    collapsedMember.member_key = resolvedMemberKey
    (collectionID, resolvedMemberKey.getOrElse(-1l), isResolved, getCollapsedMember)
  }


  /**
    * Function 1
    **/
  def populateNameAddressFields(): Unit = {
    val maxOverlayRankMember = memberList.minBy(overlayRankFunc)
    val maxOverlayList = memberList.filter(member => overlayRankFunc(member) == overlayRankFunc(maxOverlayRankMember))
    val resolvedMember = if (maxOverlayList.size > 1) {
      val memberWithBestDVPScore = maxOverlayList.maxBy(DPVRankFunc)
      val memberWithBestDPVScoreList = maxOverlayList.filter(member => DPVRankFunc(member) == DPVRankFunc(memberWithBestDVPScore)) //DPV Score : current we don't have
      if (memberWithBestDPVScoreList.size > 1) {
        memberWithBestDPVScoreList.maxBy(_.last_updated_date.map(_.getTime))
      }
      else
        memberWithBestDVPScore
    }
    else
      maxOverlayRankMember

    collapsedMember.household_key = resolvedMember.household_key
    collapsedMember.global_opt_out = resolvedMember.global_opt_out
    collapsedMember.name_prefix = resolvedMember.name_prefix
    collapsedMember.first_name = resolvedMember.first_name
    collapsedMember.last_name = resolvedMember.last_name
    collapsedMember.name_suffix = resolvedMember.name_suffix
    collapsedMember.company_name = resolvedMember.company_name
    collapsedMember.address1_scrubbed = resolvedMember.address1_scrubbed
    collapsedMember.address2_scrubbed = resolvedMember.address2_scrubbed
    collapsedMember.city_scrubbed = resolvedMember.city_scrubbed
    collapsedMember.state_scrubbed = resolvedMember.state_scrubbed
    collapsedMember.zip_code_scrubbed = resolvedMember.zip_code_scrubbed
    collapsedMember.zip4_scrubbed = resolvedMember.zip4_scrubbed
    collapsedMember.zip_full_scrubbed = resolvedMember.zip_full_scrubbed
    collapsedMember.non_us_postal_code = resolvedMember.non_us_postal_code
    collapsedMember.country_code = resolvedMember.country_code
    collapsedMember.dpv_key = resolvedMember.dpv_key
    collapsedMember.latitude = resolvedMember.latitude
    collapsedMember.longitude = resolvedMember.longitude
    collapsedMember.ncoa_last_change_date = resolvedMember.ncoa_last_change_date
    collapsedMember.first_name_scrubbed = resolvedMember.first_name_scrubbed
    collapsedMember.last_name_scrubbed = resolvedMember.last_name_scrubbed
    collapsedMember.valid_loose = resolvedMember.valid_loose
    collapsedMember.valid_strict = resolvedMember.valid_strict
    collapsedMember.county = resolvedMember.county
    collapsedMember.address1 = resolvedMember.address1
    collapsedMember.address2 = resolvedMember.address2
    collapsedMember.address_mailable = resolvedMember.address_mailable
    collapsedMember.address_type = resolvedMember.address_type
    collapsedMember.city = resolvedMember.city
    collapsedMember.state = resolvedMember.state
    collapsedMember.zip_code = resolvedMember.zip_code
    collapsedMember.zip4 = resolvedMember.zip4
    collapsedMember.name_prefix_scrubbed = resolvedMember.name_prefix_scrubbed
    collapsedMember.gender_scrubbed = resolvedMember.gender_scrubbed
    collapsedMember.name_suffix_scrubbed = resolvedMember.name_suffix_scrubbed
    collapsedMember.country_code_scrubbed = resolvedMember.country_code_scrubbed
    collapsedMember.first_trxn_store_key = resolvedMember.first_trxn_store_key
    collapsedMember.dwelling_type = resolvedMember.dwelling_type

  }

  /**
    * Function 2
    **/
  def populateOverlayFields(): Unit = {
    val resolvedMember = memberList.minBy(overlayRankFunc)
    collapsedMember.overlay_rank_key = resolvedMember.overlay_rank_key
    collapsedMember.overlay_load_id = resolvedMember.overlay_load_id
    collapsedMember.overlay_date = resolvedMember.overlay_date
  }

  /**
    * Function 3
    **/
  def populateOriginalSourceRecordInfoFields(): Unit = {
    val resolvedMember = memberList.minBy(member => getVal(member.customer_introduction_date, defaultIsMin = false).getTime)
    collapsedMember.original_source_key = resolvedMember.original_source_key
    collapsedMember.original_record_info_key = resolvedMember.original_record_info_key
    collapsedMember.customer_introduction_date = resolvedMember.customer_introduction_date
    collapsedMember.customer_introduction_date_key = resolvedMember.customer_introduction_date_key
    collapsedMember.ean = resolvedMember.ean
    collapsedMember.ean_type = resolvedMember.ean_type
    collapsedMember.rid = resolvedMember.rid
    collapsedMember.ecommerce_customer_nbr = resolvedMember.ecommerce_customer_nbr
    collapsedMember.legacy_customer_nbr = resolvedMember.legacy_customer_nbr
    collapsedMember.customer_add_date = resolvedMember.customer_add_date
    collapsedMember.customer_add_date_key = resolvedMember.customer_add_date_key
  }

  /**
    * Function 4
    **/
  def populateSourceRecordInfoFields(): Unit = {
    val resolvedMember = memberList.maxBy(_.last_updated_date.map(_.getTime))
    collapsedMember.record_info_key = resolvedMember.record_info_key
    collapsedMember.match_type_key = resolvedMember.match_type_key
    collapsedMember.current_source_key = resolvedMember.current_source_key
    collapsedMember.batch_id = resolvedMember.batch_id
    collapsedMember.last_updated_date = resolvedMember.last_updated_date
  }

  /**
    * Function 5
    *
    **/

  def populateRIDCustomerFields(): Unit = {
    val resolvedMember = memberList.minBy(_.member_key)

    collapsedMember.ean = resolvedMember.ean
    collapsedMember.ean_type = resolvedMember.ean_type
    collapsedMember.rid = resolvedMember.rid
    collapsedMember.ecommerce_customer_nbr = resolvedMember.ecommerce_customer_nbr
    collapsedMember.customer_add_date = resolvedMember.customer_add_date
    collapsedMember.customer_add_date_key = resolvedMember.customer_add_date_key

  }


  /**
    * Function 6 says all fields are null and populated using enrichment
    * Do we need to create function ?
    * Need to create function to populate field null
    **/

  def populateNullInEnrichmentFields(): Unit = {
    collapsedMember.valid_email = None
    collapsedMember.email_address = None
    collapsedMember.email_consent = None
    collapsedMember.cm_gender_merch_descr = None
    collapsedMember.preferred_store_key = None
    collapsedMember.record_type = None
    collapsedMember.phone_nbr = None //need to check
    collapsedMember.sms_consent = None
    collapsedMember.sms_consent_date = None
    collapsedMember.distance_to_preferred_store = None
    collapsedMember.email_consent_date = None
    collapsedMember.preferred_channel = None
    collapsedMember.closest_store_state = None
    collapsedMember.second_closest_store_state = None
    collapsedMember.distance_to_closest_store = None
    collapsedMember.distance_to_sec_closest_store = None
    collapsedMember.closest_store_key = None
    collapsedMember.second_closest_store_key = None
    collapsedMember.preferred_store_state = None
    collapsedMember.preferred_store_key = None
    collapsedMember.is_dm_marketable = None
    collapsedMember.is_sms_marketable = None
    collapsedMember.is_em_marketable = None
    collapsedMember.best_household_member = None
  }


  /**
    * Function 7
    *
    *
    * //TODO : check logic for non ads updated members
    **/
  def populateIndicatorsFields(): Unit = {
    val OpenInd = "O"
    val sortedMember = memberList.sortBy(_.last_updated_date.map(_.getTime)).reverse
    val adsMember = sortedMember.filter(member => member.current_source_key == Some(8) || member.current_source_key == Some(9))
    val resolvedMember = if (adsMember.size < 1)
      Some(sortedMember.head)
    else {
      val suitableMembers = adsMember.sortBy(m => m.open_close_ind.fold(OpenInd)
      (indicators =>
        Option(indicators.head).fold(OpenInd)(ind => ind))
      ).reverse
      suitableMembers.headOption
    }
    resolvedMember match {
      case Some(member) =>
        collapsedMember.ads_do_not_statement_insert = member.ads_do_not_statement_insert
        collapsedMember.ads_do_not_sell_name = member.ads_do_not_sell_name
        collapsedMember.ads_spam_indicator = member.ads_spam_indicator
        collapsedMember.ads_email_change_date = member.ads_email_change_date
        collapsedMember.ads_return_mail_ind = member.ads_return_mail_ind
      case None =>
    }
  }

  /**
    * Function 8
    * change the logic as per updated FSD
    **/

  def populateGenderField(): Unit = {

    val femaleMemberCount = memberList.count(member => member.gender == Some("FEMALE") || member.gender == Some("F"))
    val maleMemberCount = memberList.count(member => member.gender == Some("MALE") || member.gender == Some("M"))
    val nullMemberCount = memberList.count(member => member.gender.isEmpty || member.gender == Some(""))

    if (femaleMemberCount == 0 && maleMemberCount == 0)
      collapsedMember.gender = Some("UNKNOWN")
    else if (nullMemberCount == memberList.size)
      collapsedMember.gender = None
    else if (femaleMemberCount == maleMemberCount)
      collapsedMember.gender = Some("FEMALE")
    else {
      if (femaleMemberCount > maleMemberCount)
        collapsedMember.gender = Some("FEMALE")
      else
        collapsedMember.gender = Some("MALE")
    }
  }

  /**
    * Function 9
    * Need clarification on FSD
    * incomplete
    * change the logic as per the updated FSD
    **/
  def populateDirectMailConsentField(): Unit = {

    val recentConsentMember = memberList.maxBy(member => getVal(member.direct_mail_consent_date).getTime)
    val recentConsentMemberList = memberList.filter(member => member.direct_mail_consent_date == recentConsentMember.direct_mail_consent_date)
    val resolvedMember = if (recentConsentMemberList.size > 1) {
      val positiveNegativeConsent = recentConsentMemberList.filter(member => member.direct_mail_consent == Some("YES") || member.direct_mail_consent == Some("NO"))
      if (positiveNegativeConsent.nonEmpty)
        positiveNegativeConsent.sortBy(member => member.direct_mail_consent).reverse.head
      else
        recentConsentMemberList.head
    }
    else
      recentConsentMemberList.head

    collapsedMember.direct_mail_consent = resolvedMember.direct_mail_consent
    collapsedMember.direct_mail_consent_date = resolvedMember.direct_mail_consent_date

  }


  /**
    * Function 10
    **/

  def populatePromotionConsentField(): Unit = {

    if (memberList.exists(_.promotion_consent == Some("YES"))) {
      collapsedMember.promotion_consent = Some("YES")
    }
    else if (memberList.exists(_.promotion_consent == Some("NO"))) {
      collapsedMember.promotion_consent = Some("NO")
    }
    else
      collapsedMember.promotion_consent = Some("UNKNOWN")

  }


  /**
    * Function 11
    **/

  def populateBirthDateField(): Unit = {
    val currentDate = DateTime.now()
    val validBirthDatesofMember = memberList.filter(member => getVal[Date](member.birth_date).before(currentDate.toDate) &&
      getVal[Date](member.birth_date).after(currentDate.minusYears(100).toDate))

    val bestBirthDate = if (validBirthDatesofMember.size > 1) {
      val groupedDates = validBirthDatesofMember.groupBy(_.birth_date.get)
      val dateOccurrence = groupedDates.map { case (date, ml) => date -> ml.size }
      val maxOccurrence = dateOccurrence.maxBy(_._2)
      if (dateOccurrence.count { case (_, dateCount) => dateCount == maxOccurrence._2 } > 1)
        dateOccurrence.minBy(_._1.getTime)._1
      else
        maxOccurrence._1
    } else
      validBirthDatesofMember.headOption.map(_.birth_date.orNull).orNull
    collapsedMember.birth_date = Option(bestBirthDate)

  }


  /**
    * Function 12
    **/

  def populateDeceasedField(): Unit = {

    if (memberList.exists(_.deceased == Some("YES")))
      collapsedMember.deceased = Some("YES")
    else if (memberList.exists(_.deceased == Some("NO")))
      collapsedMember.deceased = Some("NO")
    else
      collapsedMember.deceased = Some("UNKNOWN")
  }


  /**
    * Function 13
    **/

  def populateAddressIsPrisonField(): Unit = {

    if (memberList.exists(_.address_is_prison == Some("YES")))
      collapsedMember.address_is_prison = Some("YES")
    else if (memberList.exists(_.address_is_prison == Some("NO")))
      collapsedMember.address_is_prison = Some("NO")
    else
      collapsedMember.address_is_prison = Some("UNKNOWN")
  }


  /**
    * Function 14
    **/

  def populateFirstTransactionDate(): Unit = {

    val memberWithMinTransactionnDate = memberList.minBy(member => getVal(member.first_trxn_date, defaultIsMin = false).getTime)
    val memberWithMinTransactionnDateList = memberList.filter(member => member.first_trxn_date == memberWithMinTransactionnDate.first_trxn_date)

    val resolvedMember = if (memberWithMinTransactionnDateList.size > 1)
      memberWithMinTransactionnDateList.minBy(member => getVal(member.first_trxn_store_key, defaultIsMin = false))
    else
      memberWithMinTransactionnDate

    collapsedMember.first_trxn_date = resolvedMember.first_trxn_date
    collapsedMember.first_trxn_date_key = resolvedMember.first_trxn_date_key
    collapsedMember.first_trxn_store_key = resolvedMember.first_trxn_store_key
  }


  /**
    * Function 15
    **/

  def populateIsExpressPLCC(): Unit = {

    if (memberList.exists(_.is_express_plcc == Some("YES")))
      collapsedMember.is_express_plcc = Some("YES")
    else if (memberList.exists(_.is_express_plcc == Some("NO")))
      collapsedMember.is_express_plcc = Some("NO")
    else
      collapsedMember.is_express_plcc = Some("UNKNOWN")


  }


  /**
    * Function 16
    **/

  def populateLastPurchaseDate(): Unit = {
    val resolvedLastStorePurchaseDate = memberList.minBy(member => getVal(member.last_store_purch_date, defaultIsMin = false).getTime)
    val resolvedLastWebPurchaseDate = memberList.minBy(member => getVal(member.last_web_purch_date, defaultIsMin = false).getTime)
    collapsedMember.last_store_purch_date = resolvedLastStorePurchaseDate.last_store_purch_date
    collapsedMember.last_web_purch_date = resolvedLastWebPurchaseDate.last_web_purch_date
    collapsedMember.last_store_purch_date_key = resolvedLastStorePurchaseDate.last_store_purch_date_key
    collapsedMember.last_web_purch_date_key = resolvedLastWebPurchaseDate.last_web_purch_date_key
  }

  /**
    * Function 17 also covers Function 20
    **/
  def populateMemberSocialIdAndEnrollDate(): Unit = {
    if (getCollectionID == 3 || getCollectionID == 4) {
      val resolvedMember = memberList.filter(_.member_key == getBestMember).head
      collapsedMember.cdh_member_key = resolvedMember.cdh_member_key
      collapsedMember.communications_pref = resolvedMember.communications_pref
      collapsedMember.confirm_age_eighteen = resolvedMember.confirm_age_eighteen
      collapsedMember.current_tier_key = resolvedMember.current_tier_key
      collapsedMember.tier_name = resolvedMember.tier_name
      collapsedMember.e_cert_opt_in = resolvedMember.e_cert_opt_in
      collapsedMember.e_cert_opt_in_date = resolvedMember.e_cert_opt_in_date
      collapsedMember.e_cert_opt_in_date_key = resolvedMember.e_cert_opt_in_date_key
      collapsedMember.facebook_id = resolvedMember.facebook_id
      collapsedMember.ip_code = resolvedMember.ip_code
      collapsedMember.is_loyalty_member = resolvedMember.is_loyalty_member
      collapsedMember.language_preference = resolvedMember.language_preference
      collapsedMember.loyalty_id = resolvedMember.loyalty_id
      collapsedMember.loyalty_type = resolvedMember.loyalty_type
      collapsedMember.lw_enrollment_source_key = resolvedMember.lw_enrollment_source_key
      collapsedMember.lw_enrollment_store_key = resolvedMember.lw_enrollment_store_key
      collapsedMember.member_acct_close_date = resolvedMember.member_acct_close_date
      collapsedMember.member_acct_close_date_key = resolvedMember.member_acct_close_date_key
      collapsedMember.member_acct_enroll_date = resolvedMember.member_acct_enroll_date
      collapsedMember.member_acct_enroll_date_key = resolvedMember.member_acct_enroll_date_key
      collapsedMember.member_acct_open_date = resolvedMember.member_acct_open_date
      collapsedMember.member_acct_open_date_key = resolvedMember.member_acct_open_date_key
      collapsedMember.member_source = resolvedMember.member_source
      collapsedMember.member_status = resolvedMember.member_status
      collapsedMember.opt_status = resolvedMember.opt_status
      collapsedMember.plcc_request_date = resolvedMember.plcc_request_date
      collapsedMember.plcc_request_user_id = resolvedMember.plcc_request_user_id
      collapsedMember.preapproval_expiration_date = resolvedMember.preapproval_expiration_date
      collapsedMember.preapproval_number = resolvedMember.preapproval_number
      collapsedMember.printed_card_req_date = resolvedMember.printed_card_req_date
      collapsedMember.printed_card_req_date_key = resolvedMember.printed_card_req_date_key
      collapsedMember.printed_cert_opt_in = resolvedMember.printed_cert_opt_in
      collapsedMember.printed_cert_opt_in_date = resolvedMember.printed_cert_opt_in_date
      collapsedMember.printed_cert_opt_in_date_key = resolvedMember.printed_cert_opt_in_date_key
      collapsedMember.request_loyalty_card = resolvedMember.request_loyalty_card
      collapsedMember.request_paper_cert_date = resolvedMember.request_paper_cert_date
      collapsedMember.security_question = resolvedMember.security_question
      collapsedMember.token_nbr_association_request = resolvedMember.token_nbr_association_request
      collapsedMember.twitter_id = resolvedMember.twitter_id
      collapsedMember.is_plcc_request = resolvedMember.is_plcc_request
      collapsedMember.member_acct_open_date_time = resolvedMember.member_acct_open_date_time
    }

  }

  /**
    * Function 18
    */

  def populateProspectCountField(): Unit = {
    collapsedMember.prospect_appearance_count = Some(memberList.map(_.prospect_appearance_count.getOrElse(0l)).sum)
    collapsedMember.prospect_contact_count = Some(memberList.map(_.prospect_contact_count.getOrElse(0l)).sum)
    collapsedMember.prospect_mb_purch_count = Some(memberList.map(_.prospect_mb_purch_count.getOrElse(0l)).sum)
  }


  /**
    * Function 19
    */
  def populatePhoneFields(): Unit = {
    collapsedMember.valid_phone_for_call = None
    collapsedMember.valid_phone_for_sms = None
    collapsedMember.phone_ext = None
    collapsedMember.phone_type = None
    collapsedMember.phone_consent = None
  }


  def getCollapsedMember: Member = {
    if (getCollectionID != 2 && getCollectionID != -1 && getBestMember != Some(-1)) {
      populateNameAddressFields()
      populateOverlayFields()
      populateOriginalSourceRecordInfoFields()
      populateSourceRecordInfoFields()
      //populateRIDCustomerFields()
      populateNullInEnrichmentFields()
      populateIndicatorsFields()
      populateGenderField()
      populateDirectMailConsentField()
      populatePromotionConsentField()
      populateBirthDateField()
      populateDeceasedField()
      populateAddressIsPrisonField()
      populateFirstTransactionDate()
      populateIsExpressPLCC()
      populateLastPurchaseDate()
      populateMemberSocialIdAndEnrollDate()
      populateProspectCountField()
      populatePhoneFields()
      collapsedMember
    }
    else
      null
  }

}
