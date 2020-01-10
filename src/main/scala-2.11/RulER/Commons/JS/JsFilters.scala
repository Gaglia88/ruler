package RulER.Commons.JS

object JsFilters {
  /**
    * Dato un array di token ritorna il prefisso in base alla soglia, implementato correttamente in base alle formule
    **/
  def getPrefix(tokens: Array[Long], threshold: Double, k: Int = 1): Array[Long] = {
    val len = tokens.length
    tokens.take(len - Math.ceil(len.toDouble * threshold).toInt + k)
  }

  /**
    * Implementa il position filter come da formule teoriche
    * */
  def positionFilter(lenDoc1: Int, lenDoc2: Int, posDoc1: Int, posDoc2: Int, o: Int, threshold: BigDecimal): Boolean = {
    val alpha = Math.ceil((threshold * (lenDoc1 + lenDoc2) / (1 + threshold)).toDouble).toInt
    (o + Math.min(lenDoc1 - posDoc1, lenDoc2 - posDoc2)) >= alpha
  }

  /**
    * Data la lunghezza di un documento e una soglia ritorna la lunghezza del prefisso
    * */
  def getPrefixLength(docLen: Int, threshold: Double, k: Int = 1) : Int = {
    docLen - Math.ceil(docLen.toDouble * threshold).toInt + 1
  }
}
