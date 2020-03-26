package RulER.SimJoins

import java.util.Calendar

import RulER.Commons.CommonFunctions
import RulER.Commons.ED.CommonEdFunctions.{commons, getPrefixLen}
import RulER.Commons.ED.{CommonEdFunctions, EdFilters}
import RulER.Commons.JS.CommonJsFunctions
import RulER.DataStructure.{Profile, Qgram, Rule}
import RulER.SimJoins.PPJoin.getCandidates
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.DoubleAccumulator

object EDJoin {
  def buildPrefixIndex(sortedDocs: RDD[(Long, Array[(Int, Int)])], qgramLen: Int, threshold: Int): RDD[(Int, Array[(Long, Int, Array[(Int, Int)])])] = {
    val prefixLen = getPrefixLen(qgramLen, threshold)

    val allQgrams = sortedDocs.flatMap { case (docId, qgrams) =>
      /*val prefix = {
        if (qgrams.length < prefixLen) {
          qgrams.union(commons.fixPrefix :: Nil)
        }
        else {
          qgrams.take(prefixLen)
        }
      }*/
      val prefix = qgrams.take(prefixLen)
      prefix.zipWithIndex.map { case (qgram, index) =>
        (qgram._1, (docId, index, qgrams))
      }
    }


    val blocks = allQgrams.groupByKey().filter(_._2.size > 1)

    blocks.map(b => (b._1, b._2.toArray.sortBy(_._3.length)))
  }


  def getCandidatePairs(prefixIndex: RDD[(Int, Array[(Long, Int, Array[(Int, Int)])])], qgramLength: Int, threshold: Int, compNum: DoubleAccumulator): RDD[(Long, Long)] = {
    prefixIndex.flatMap { case (blockId, block) =>
      var results: List[(Long, Long)] = Nil
      var i = 0
      while (i < block.length) {
        var j = i + 1
        val d1Id = block(i)._1
        val d1Pos = block(i)._2
        val d1Qgrams = block(i)._3
        while (j < block.length) {
          val d2Id = block(j)._1
          val d2Pos = block(j)._2
          val d2Qgrams = block(j)._3

          if (d1Id != d2Id && math.abs(d1Pos - d2Pos) <= threshold && math.abs(d1Qgrams.length - d2Qgrams.length) <= threshold) {
            compNum.add(1)
            if (EdFilters.commonFilter(d1Qgrams, d2Qgrams, qgramLength, threshold)) {
              if (d1Id < d2Id) {
                results = (d1Id, d2Id) :: results
              }
              else {
                results = (d2Id, d1Id) :: results
              }
            }
          }
          j += 1
        }
        i += 1
      }
      results
    }
  }


  def getCandidates(sortedDocs: RDD[(Long, Array[(Int, Int)])], qgramLength: Int, threshold: Int, compNum: DoubleAccumulator): RDD[(Long, Long)] = {
    val ts = Calendar.getInstance().getTimeInMillis
    val prefixIndex = buildPrefixIndex(sortedDocs, qgramLength, threshold)
    prefixIndex.cache()
    prefixIndex.count()
    val te = Calendar.getInstance().getTimeInMillis
    val log = LogManager.getRootLogger
    log.info("[GraphJoin] EDJOIN index time (s) " + (te - ts) / 1000.0)

    val t1 = Calendar.getInstance().getTimeInMillis
    val candidates = getCandidatePairs(prefixIndex, qgramLength, threshold, compNum).distinct()
    val nc = candidates.count()
    val t2 = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] Candidates number " + nc)
    log.info("[GraphJoin] EDJOIN join time (s) " + (t2 - t1) / 1000.0)

    candidates
  }

  def EDJoin(df: DataFrame, r: Rule, qgramLen: Int = 7): DataFrame = {
    val profiles = CommonFunctions.dfProfilesToRDD1(df)
    val docs = CommonFunctions.extractField(profiles, r.attribute).map(x => (x._1, CommonEdFunctions.getQgrams(x._2, qgramLen)))
    val tokenizedDocSort = CommonEdFunctions.getSortedQgrams(docs)
    val compNum = SparkContext.getOrCreate().doubleAccumulator
    val sparkSession = SparkSession.builder().getOrCreate()
    sparkSession.createDataFrame(getCandidates(tokenizedDocSort, qgramLen, r.threshold.toInt, compNum)).toDF("id1", "id2")
  }
}
