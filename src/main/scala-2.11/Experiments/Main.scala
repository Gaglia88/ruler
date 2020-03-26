package Experiments

import java.util.Calendar

import RulER.Commons.{CommonFunctions, DataPreparator}
import RulER.DataStructure.{KeyValue, Profile, Rule}
import RulER.SimJoins.RulERJoin
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import RulER.Commons.CommonFunctions.loadProfilesAsDF
import RulER.DataStructure.ThresholdTypes.ED
import RulER.DataStructure.ThresholdTypes.JS

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "32")
      .set("spark.local.dir", "/data2/tmp")
      .set("spark.driver.maxResultSize", "0")
      .set("spark.executor.heartbeatInterval", "200000")
      .set("spark.network.timeout", "300000")

    val sc = new SparkContext(conf)

    val df = loadProfilesAsDF("C:\\Users\\gagli\\Desktop\\Nuova cartella\\demoEDBT2020\\rotten_tomatoes.csv")
    val df2 = loadProfilesAsDF("C:\\Users\\gagli\\Desktop\\Nuova cartella\\demoEDBT2020\\imdb.csv")

    val profiles1 = CommonFunctions.dfProfilesToRDD1(df)
    val separator = profiles1.map(_.id).max()
    val profiles2 = CommonFunctions.dfProfilesToRDD1(df2, startIDFrom = separator + 1)

    val r1 = Rule("title", JS, 0.1, "name")
    val r2 = Rule("director", JS, 0.1, "director")
    val rule = r1 or r2

    val attrMap = rule.getUniqueRuleRep

    println(attrMap)

    ???

    val profiles = profiles1.union(profiles2).map { p =>
      val p2 = Profile(id = p.id, originalID = p.originalID)
      p.attributes.withFilter(a => attrMap.keySet.contains(a.key)).foreach { kv =>
        p2.addAttribute(KeyValue(attrMap(kv.key), kv.value))
      }
      p2
    }


    profiles.take(10).foreach(println)

    ???
    val newRule = rule.convertRule(attrMap)

    val conditionsPerAttribute = DataPreparator.parseConditions(newRule.getFormat)
    val tokenizedProfiles = DataPreparator.tokenizeProfiles(profiles, conditionsPerAttribute, 5)

    val results = RulERJoin.getMatches(tokenizedProfiles, newRule.getFormat)
  }
}
