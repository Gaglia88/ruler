package RulER.DataStructure

import scala.collection.mutable

case class AndBlock(andRules: mutable.MutableList[Rule] = new mutable.MutableList[Rule]()) {
  def addRule(r: Rule): AndBlock = {
    andRules += r
    this
  }

  def and(that: Rule): AndBlock = {
    this.addRule(that)
    this
  }

  def and(that: AndBlock): AndBlock = {
    that.andRules.foreach(r => this.addRule(r))
    this
  }

  def or(that: AndBlock): MatchingRule = {
    MatchingRule().addAndBlock(this).addAndBlock(that)
  }

  def or(that: Rule): MatchingRule = {
    MatchingRule().addAndBlock(this).addRule(that)
  }

  override def toString: String = {
    andRules.mkString(" AND ")
  }

  def getFormat: Map[String, (Double, String)] = {
    andRules.map(_.getFormat).toMap
  }
}