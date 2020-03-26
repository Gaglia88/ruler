package RulER.Commons

import java.util

import RulER.DataStructure.{AndBlock, KeyValue, MatchingRule, Profile, Rule}
import RulER.SimJoins.RulERJoin
import org.apache.spark.sql.{DataFrame, SparkSession}

class implicits(df: DataFrame) {

  def joinWithRules(df2: DataFrame, rule: Rule): DataFrame = {
    val m = MatchingRule()
    m.addRule(rule)
    joinWithRules(df2, m)
  }

  def joinWithRules(df2: DataFrame, rule: AndBlock): DataFrame = {
    val m = MatchingRule()
    m.addAndBlock(rule)
    joinWithRules(df2, m)
  }

  def joinWithRules(df2: DataFrame, rule: Rule, id1: String, id2: String): DataFrame = {
    val m = MatchingRule()
    m.addRule(rule)
    joinWithRules(df2, m, id1, id2)
  }

  def joinWithRules(df2: DataFrame, rule: AndBlock, id1: String, id2: String): DataFrame = {
    val m = MatchingRule()
    m.addAndBlock(rule)
    joinWithRules(df2, m, id1, id2)
  }

  def joinWithRules(df2: DataFrame, rule: MatchingRule, id1: String = "_rowId", id2: String = "_rowId"): DataFrame = {
    val res = if (df == df2) {
      val profiles = CommonFunctions.dfProfilesToRDD1(df, realIDField = id1)

      val conditionsPerAttribute = DataPreparator.parseConditions(rule.getFormat)
      val tokenizedProfiles = DataPreparator.tokenizeProfiles(profiles, conditionsPerAttribute, 5)

      val results = RulERJoin.getMatches(tokenizedProfiles, rule.getFormat)
      /*val p = profiles.context.broadcast(profiles.map(p => (p.id, p.originalID)).collectAsMap())
      val originalIDs = results.map { case (id1, id2) =>
        (p.value(id1), p.value(id2))
      }*/

      val sparkSession = SparkSession.builder().getOrCreate()
      sparkSession.createDataFrame(results).toDF("id1", "id2")
    }
    else {
      val profiles1 = CommonFunctions.dfProfilesToRDD1(df, realIDField = id1)
      val separator = profiles1.map(_.id).max()
      val profiles2 = CommonFunctions.dfProfilesToRDD1(df2, realIDField = id2, startIDFrom = separator + 1)

      val attrMap = rule.getUniqueRuleRep

      val profiles = profiles1.union(profiles2).map { p =>
        val p2 = Profile(id = p.id, originalID = p.originalID)
        p.attributes.withFilter(a => attrMap.keySet.contains(a.key)).foreach { kv =>
          p2.addAttribute(KeyValue(attrMap(kv.key), kv.value))
        }
        p2
      }

      val newRule = rule.convertRule(attrMap)

      val conditionsPerAttribute = DataPreparator.parseConditions(newRule.getFormat)
      val tokenizedProfiles = DataPreparator.tokenizeProfiles(profiles, conditionsPerAttribute, 5)

      val results = RulERJoin.getMatches(tokenizedProfiles, newRule.getFormat)
      val sparkSession = SparkSession.builder().getOrCreate()
      sparkSession.createDataFrame(results.map(x => (x._1, x._2 - separator - 1))).toDF("id1", "id2")
    }

    val r1 = df.join(res, df(id1) === res("id1"))
    df2.join(r1, r1("id2") === df2(id2))
  }
}

object implicits {
  implicit def addCustomFunctions(df: DataFrame): implicits = new implicits(df)
}