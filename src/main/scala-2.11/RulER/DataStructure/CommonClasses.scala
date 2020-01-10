package RulER.DataStructure

object CommonClasses {

  trait tokenized extends Serializable {
    def getType(): String
  }

  case class tokensJs(tokens: Array[Long]) extends tokenized {
    def getType(): String = ThresholdTypes.JS

    override def toString: String = tokens.toList.toString()
  }

  case class tokensED(qgrams: Array[(Int, Int)], originalStr: String) extends tokenized {
    def getType(): String = ThresholdTypes.ED

    override def toString: String = qgrams.toList.toString()
  }

  case class tokensStrJs(tokens: Set[String]) extends tokenized {
    def getType(): String = ThresholdTypes.JS
  }

  case class noTokens() extends tokenized {
    def getType(): String = "empty"
  }

  case class tokensStrED(qgrams: Array[(String, Int)], originalStr: String) extends tokenized {
    def getType(): String = ThresholdTypes.ED
  }

  trait indexDocs extends Serializable {
    def getType: String
  }

  case class indexJS(docId: Long, tokenPos: Int, docLen: Int) extends indexDocs {
    override def getType: String = ThresholdTypes.JS
  }

  case class indexED(docId: Long, docLength: Int, qgramPos: Int, sortedPos: Int) extends indexDocs {
    override def getType: String = ThresholdTypes.ED
  }

}
