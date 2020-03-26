package RulER.SimJoins

import java.util.Calendar

import RulER.Commons.{CommonFunctions, DataPreparator}
import RulER.Commons.JS.{CommonJsFunctions, JsFilters}
import RulER.DataStructure.{MyPartitioner2, Rule}
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.DoubleAccumulator

object PPJoin {
  /**
    * Conta gli elementi comuni nel prefisso
    *
    * @param doc1Tokens token del primo documento
    * @param doc2Tokens token del secondo documento
    * @param sPos1      posizione di partenza nel primo documento (la posizione dell'ultimo token comune nel prefisso)
    * @param sPos2      posizione di partenza nel secondo documento (la posizione dell'ultimo token comune nel prefisso)
    **/
  def getCommonElementsInPrefix(doc1Tokens: Array[Long], doc2Tokens: Array[Long], sPos1: Int, sPos2: Int): (Int, Int, Int) = {
    var common = 0
    var p1 = sPos1
    var p2 = sPos2
    var c1 = -1
    var c2 = -1
    while (p1 >= 0 && p2 >= 0) {
      if (doc1Tokens(p1) == doc2Tokens(p2)) {
        if (c1 < 0) {
          c1 = p1
          c2 = p2
        }
        common = common + 1
        p1 -= 1
        p2 -= 1
      }
      else if (doc1Tokens(p1) > doc2Tokens(p2)) {
        p1 -= 1
      }
      else {
        p2 -= 1
      }
    }
    (common, c1, c2)
  }


  /**
    * Ritorna le coppie candidate che passano il length filter e il position usando il prefix filter
    **/
  def getCandidatePairs(prefixIndex: RDD[(Long, Array[(Long, Array[Long])])], threshold: Double, compNum: DoubleAccumulator): RDD[(Long, Long)] = {
    prefixIndex.flatMap {
      case (tokenId, docs) =>
        val results = scala.collection.mutable.Set[(Long, Long)]()
        var i = 0
        while (i < docs.length - 1) {
          var j = i + 1
          val doc1Id = docs(i)._1
          val doc1Tokens = docs(i)._2
          val doc1PrefixLen = JsFilters.getPrefixLength(doc1Tokens.length, threshold)

          /** Per ogni coppia che passa il length filter */
          while ((j < docs.length) && (doc1Tokens.length >= docs(j)._2.length * threshold)) { //Length filter
            compNum.add(1)
            val doc2Id = docs(j)._1
            val doc2Tokens = docs(j)._2
            val doc2PrefixLen = JsFilters.getPrefixLength(doc2Tokens.length, threshold)
            val (common, p1, p2) = getCommonElementsInPrefix(doc1Tokens, doc2Tokens, doc1PrefixLen - 1, doc2PrefixLen - 1) //Prende elementi comuni nel prefisso
            if (JsFilters.positionFilter(doc1Tokens.length, doc2Tokens.length, p1 + 1, p2 + 1, common, threshold)) { //Verifica che passi il position filter
              if (doc1Id < doc2Id) {
                results.add((doc1Id, doc2Id))
              }
              else {
                results.add((doc2Id, doc1Id))
              }
            }

            j = j + 1
          }
          i += 1
        }
        results
    }.distinct()
  }


  /**
    * Crea un inverted index che per ogni documento per ogni token che ha nel prefisso ritorna
    * (docId, token del documento)
    **/
  def buildPrefixIndex(tokenizedDocOrd: RDD[(Long, Array[Long])], threshold: Double): RDD[(Long, Array[(Long, Array[Long])])] = {
    val indices = tokenizedDocOrd.flatMap {
      case (docId, tokens) =>
        val prefix = JsFilters.getPrefix(tokens, threshold)
        prefix.zipWithIndex.map {
          case (token, pos) =>
            (token, (docId, tokens))
        }
    }

    indices.groupByKey().filter(_._2.size > 1).map {
      case (tokenId, documents) => (tokenId, documents.toArray.sortBy(x => x._2.length))
    }
  }

  /** Ritorna l'elenco di coppie candidate */
  def getCandidates(tokenizedDocSort: RDD[(Long, Array[Long])], threshold: Double, compNum: DoubleAccumulator): RDD[(Long, Long)] = {
    val ts = Calendar.getInstance().getTimeInMillis
    val prefixIndex = buildPrefixIndex(tokenizedDocSort, threshold)
    prefixIndex.count()
    val log = LogManager.getRootLogger
    val te = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] PPJOIN index time (s) " + (te - ts) / 1000.0)

    val t1 = Calendar.getInstance().getTimeInMillis
    val candidates = getCandidatePairs(prefixIndex, threshold, compNum)
    candidates.count()
    val t2 = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] PPJOIN join time (s) " + (t2 - t1) / 1000.0)
    candidates
  }

  def PPJoin(df: DataFrame, r: Rule): DataFrame = {
    val profiles = CommonFunctions.dfProfilesToRDD1(df)
    val field = CommonFunctions.extractField(profiles, r.attribute)
    val tokenizedDocSort = CommonJsFunctions.tokenizeAndSort(field)
    val compNum = SparkContext.getOrCreate().doubleAccumulator
    val sparkSession = SparkSession.builder().getOrCreate()
    sparkSession.createDataFrame(getCandidates(tokenizedDocSort, r.threshold, compNum)).toDF("id1", "id2")
  }
}
