package RulER.DataStructure

case class Rule(attribute: String, thresholdType: String, threshold: Double, secondAttribute: String = "") {
  def and(that: Rule): AndBlock = {
    AndBlock().addRule(this).addRule(that)
  }

  def or(that: Rule): MatchingRule = {
    MatchingRule().addRule(this).addRule(that)
  }

  def or(that: AndBlock): MatchingRule = {
    MatchingRule().addRule(this).addAndBlock(that)
  }

  override def toString: _root_.java.lang.String = {
    attribute + "," + thresholdType + "," + threshold
  }

  def getFormat: (String, (Double, String)) = {
    (attribute, (threshold, thresholdType))
  }
}
