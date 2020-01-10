package RulER.DataStructure

/**
  * Rappresenta un qgramma in un documento
  **/
case class Qgram(docId: Long, docLength: Int, qgramPos: Int, sortedPos: Int)  extends Serializable
