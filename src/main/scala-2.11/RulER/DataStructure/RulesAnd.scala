package RulER.DataStructure

class RulesAnd {
  private var rules: List[Rule] = Nil

  def and(attribute: String, ruleType: String, threshold: Double): RulesAnd = {
    rules = new Rule(attribute, ruleType, threshold) :: rules
    this
  }

  def getRules: Map[String, (Double, String)] = {
    rules.map { r =>
      (r.attribute, r.threshold, r.ruleType)
    }.groupBy(_._1).map(r => (r._1, (r._2.head._2, r._2.head._3)))
  }

}
