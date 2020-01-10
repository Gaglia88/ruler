package RulER.Commons.ED

import RulER.DataStructure.Qgram
import org.apache.spark.rdd.RDD

object CommonEdFunctions {

  object commons {
    def fixPrefix: (Int, Int) = (-1, -1)
  }


  def getSortedQgrams2(docs: RDD[(Long, String, Array[(String, Int)])]): RDD[(Long, String, Array[(Int, Int)])] = {
    val tf = getQgramsTf(docs.map(x => (x._1, x._3)))
    val tf2 = tf.toList.sortBy(_._2).zipWithIndex.map(x => (x._1._1, x._2)).toMap
    docs.map { case (docId, str, qgrams) =>
      val sortedQgrams = qgrams.map(q => (tf2(q._1), q._2)).sortBy(q => q)
      (docId, str, sortedQgrams)
    }
  }

  /**
    * Data una stringa ne restituisce i qgrammi.
    * Il qgramma ha anche la posizione originale nel documento
    **/
  def getQgrams(str: String, qgramSize: Int): Array[(String, Int)] = {
    str.sliding(qgramSize).zipWithIndex.map(q => (q._1, q._2)).toArray
  }

  /**
    * Dati i documenti trasformati in q-grammi calcola la document frequency per ogni qgramma
    **/
  def getQgramsTf(docs: RDD[(Long, Array[(String, Int)])]): Map[String, Int] = {
    val allQgrams = docs.flatMap { case (docId, qgrams) =>
      qgrams.map { case (str, pos) =>
        str
      }
    }
    allQgrams.groupBy(x => x).map(x => (x._1, x._2.size)).collectAsMap().toMap
  }

  /**
    * Ordina i qgrammi all'interno del documento per la loro document frequency
    **/
  def getSortedQgrams(docs: RDD[(Long, Array[(String, Int)])]): RDD[(Long, Array[(Int, Int)])] = {
    val tf = getQgramsTf(docs)
    val tf2 = docs.context.broadcast(tf.toList.sortBy(_._2).zipWithIndex.map(x => (x._1._1, x._2)).toMap)
    docs.map { case (docId, qgrams) =>
      val sortedQgrams = qgrams.map(q => (tf2.value(q._1), q._2)).sortBy(q => q)
      (docId, sortedQgrams)
    }
  }

  def getSortedQgrams3(docs: RDD[(Long, Array[(String, Int)], String)]): RDD[(Long, Array[(Int, Int)], String)] = {
    val tf = getQgramsTf(docs.map(d => (d._1, d._2)))
    val tf2 = docs.context.broadcast(tf.toList.sortBy(_._2).zipWithIndex.map(x => (x._1._1, x._2)).toMap)
    docs.map { case (docId, qgrams, originalStr) =>
      val sortedQgrams = qgrams.map(q => (tf2.value(q._1), q._2)).sortBy(q => q)
      (docId, sortedQgrams, originalStr)
    }
  }

  /**
    * Dati due elementi ne calcola l'edit distance
    **/
  def editDist[A](a: Iterable[A], b: Iterable[A]): Int = {
    ((0 to b.size).toList /: a) ((prev, x) =>
      (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
        case (h, ((d, v), y)) => math.min(math.min(h + 1, v + 1), d + (if (x == y) 0 else 1))
      }) last
  }

  /** Ritorna la lunghezza del prefisso */
  def getPrefixLen(qGramLen: Int, threshold: Int): Int = {
    qGramLen * threshold + 1
  }

  /**
    * Dato l'elenco di documenti con i q-grammi ordinati crea il prefix index.
    * Nota: per risolvere il problema dei documenti troppo corti il prefix index contiene un blocco identificato dall'id
    * specificato in "fixprefix" che contiene tutti i documenti che non possono essere verificati con sicurezza.
    **/
  def buildPrefixIndex(sortedDocs: RDD[(Long, Array[(Int, Int)])], qgramLen: Int, threshold: Int): RDD[(Int, Array[Qgram])] = {
    val prefixLen = getPrefixLen(qgramLen, threshold)

    val allQgrams = sortedDocs.flatMap { case (docId, qgrams) =>
      val prefix = {
        if (qgrams.length < prefixLen) {
          qgrams.union(commons.fixPrefix :: Nil)
        }
        else {
          qgrams.take(prefixLen)
        }
      }
      prefix.zipWithIndex.map { case (qgram, index) =>
        (qgram._1, Qgram(docId, qgrams.length, qgram._2, index))
      }
    }
    allQgrams.groupBy(_._1).filter(_._2.size > 1).map(x => (x._1, x._2.map(_._2).toArray))
  }
}
