package RulER.DataStructure

class RulesOr {
  private var rules: List[RulesAnd] = Nil

  def or(conditions: RulesAnd): RulesOr = {
    rules = conditions :: rules
    this
  }

  def getRules: List[Map[String, (Double, String)]] = {
    rules.map { rulesAnd =>
      rulesAnd.getRules
    }
  }
}
