package RulER.DataStructure

import scala.collection.mutable

case class MatchingRule(orRules: mutable.MutableList[List[Rule]] = new mutable.MutableList[List[Rule]]()) {
  def addOrRule(r: List[Rule]):MatchingRule = {
    orRules += r
    this
  }


  def and(that: Rule): MatchingRule = {
    println("chiama me")
    ???
  }

  def or(that: Rule): MatchingRule = {
    ???
  }

  def and(that: MatchingRule): MatchingRule = {
    ???
  }

  def or(that: MatchingRule): MatchingRule = {
    ???
  }
}
