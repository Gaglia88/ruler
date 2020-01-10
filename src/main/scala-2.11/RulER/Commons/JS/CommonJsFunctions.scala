package RulER.Commons.JS

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object CommonJsFunctions {
  /**
    * Data una stringa esegue la tokenizzazione
    **/
  def tokenize(document: String): Set[String] = {
    //val stopWords = StopWordsRemover.loadDefaultStopWords("english").map(_.toLowerCase)
    document.split("[\\W_]").map(_.trim.toLowerCase).filter(_.length > 0).toSet //.filter(x => !stopWords.contains(x)).toSet
  }

  def tokenizeAndSort(documents: RDD[(Long, String)]): RDD[(Long, Array[Long])] = {
    val tokenizedDocuments = documents.map(d => (d._1, tokenize(d._2)))
    val tf = calcTF(tokenizedDocuments)
    val sc = documents.context
    val globalOrder: Broadcast[scala.collection.Map[String, (Double, Long)]] = sc.broadcast(tf.collectAsMap())
    sortTokens(tokenizedDocuments, globalOrder)
  }

  /**
    * Dato un RDD di documenti tokenizzati calcola la term frequency di ogni token.
    * Ritorna un RDD con:
    * token -> (numero di volte in cui compare, id del token).
    * L'id del token è collegato alla sua term frequency, es. 0 è il token meno frequente, N sarà il token più frequente.
    **/
  def calcTF(tokenizedDocs: RDD[(Long, Set[String])]): RDD[(String, (Double, Long))] = {
    def create(e: Double): Double = e

    def add(acc: Double, e: Double): Double = acc + e

    def merge(acc1: Double, acc2: Double): Double = acc1 + acc2

    tokenizedDocs.flatMap(d => d._2.map(t => (t, 1.0))).combineByKey(create, add, merge).sortBy(x => (x._2, x._1)).zipWithIndex().map(x => (x._1._1, (x._1._2, x._2)))
  }

  /**
    * Dato un RDD di documenti tokenizzati e l'ordine globale (ossia la term frequency di ogni token), ordina i token all'interno
    * dei documenti. Inoltre ad ogni token è sostituito il suo id.
    **/
  def sortTokens(tokenizedDocuments: RDD[(Long, Set[String])], globalOrder: Broadcast[scala.collection.Map[String, (Double, Long)]]): RDD[(Long, Array[Long])] = {
    tokenizedDocuments.map { d =>
      val sortedTokens = d._2.toArray.map(x => globalOrder.value(x)._2).sorted
      (d._1, sortedTokens)
    }
  }


  /**
    * Crea un inverted index che per ogni documento per ogni token che ha nel prefisso ritorna
    * (docId, posizione del token nel prefiss, lunghezza del documento)
    **/
  def buildPrefixIndex(tokenizedDocOrd: RDD[(Long, Array[Long])], threshold: Double): RDD[(Long, Array[(Long, Int, Int)])] = {
    val indices = tokenizedDocOrd.flatMap { case (docId, tokens) =>
      val prefix = JsFilters.getPrefix(tokens, threshold)
      prefix.zipWithIndex.map { case (token, pos) =>
        (token, (docId, pos + 1, tokens.length))
      }
    }
    indices.groupByKey().filter(_._2.size > 1).map(x => (x._1, x._2.toArray.sortBy(_._3)))
  }

  def passJS(doc1: Array[Long], doc2: Array[Long], threshold: Double): Boolean = {
    val common = doc1.intersect(doc2).length
    (common.toDouble / (doc1.length + doc2.length - common)) >= threshold
  }
}
