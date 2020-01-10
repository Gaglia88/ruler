package RulER.DataStructure

case class Rule(attribute: String, ruleType: String, threshold: Double) {
  def and(that: Rule): MatchingRule = {
    MatchingRule().addOrRule(List(this, that))
  }

  def or(that: Rule): MatchingRule = {
    MatchingRule().addOrRule(List(this)).addOrRule((List(that)))
  }
}
