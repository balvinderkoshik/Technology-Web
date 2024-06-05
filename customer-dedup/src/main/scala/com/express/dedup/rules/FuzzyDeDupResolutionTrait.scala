package com.express.dedup.rules

import com.express.dedup.utils.Settings._
import me.xdrop.fuzzywuzzy.FuzzySearch
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Created by aman.jain on 6/28/2017.
  */
trait FuzzyDeDupResolutionTrait extends DeDupResolutionTrait {

  val fuzzyColStruct: StructType


  private def fuzzyMatch(value1: String, value2: String) = {
    val lhs = value1.toUpperCase.replaceAll("[^a-zA-Z0-9 ]","")
    val rhs = value2.toUpperCase.replaceAll("[^a-zA-Z0-9 ]","")
    val threshold = getFuzzyThreshold
    val score = FuzzySearch.tokenSortRatio(lhs, rhs)
    val result = if (score >= threshold) true else false
    (score, result) //score
  }


  //Fuzzy Calculation for First and Last Name
  def fuzzyCalculation(members: Seq[Row]): Seq[Row] = {
    case class FuzzyInput(str: String, member_key: Long)
    val fuzzySeq =
      members
        .map(m => FuzzyInput(m.toSeq.drop(1).mkString, m.getLong(0)))
        .combinations(2).map { case Seq(x, y) => (x, y) }
        .toList
        .map(x => (x, fuzzyMatch(x._1.str, x._2.str)))
        .sortBy(x => (x._1._1.str.concat(x._1._2.str), x._2._1))
    val fuzzyMapping =
      fuzzySeq
        .filter { case ((_, _), (_, result)) => result }
        .flatMap { case ((f1, f2), (score, result)) =>
          Seq(
            f1.member_key -> (s"${f1.member_key}|${f2.member_key}", score, result),
            f2.member_key -> (s"${f1.member_key}|${f2.member_key}", score, result)
          )
        }.toMap
    members.map(m => {
      val member = m.getAs[Long](0)
      val fuzzyResult = fuzzyMapping.getOrElse(member, (null, 0, false))
      Row.fromSeq(m.toSeq ++ fuzzyResult.productIterator.toSeq)
    })
  }



}

