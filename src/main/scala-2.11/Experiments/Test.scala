package Experiments

import RulER.Commons.{CommonFunctions, DataPreparator}
import java.util.Calendar

import RulER.SimJoins.{JoinChain, RulERJoin}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}

object Test {
  object TestType{
    val JoinChain = "JoinChain"
    val RuleER = "RulER"
  }


  def main(args: Array[String]): Unit = {
    val logFilePath = "/data2/omdb_and.txt"
    val inputFilePath = "/data2/ER/imdb_dbp/omdb.csv"
    val qgramLen = 7
    val algo = TestType.RuleER

    val titleJS = ("Title", (0.9, "JS"))
    val castJS = ("Cast", (0.8, "JS"))


    val matchingRule = List(
      Map(titleJS, castJS)
    )


    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "32")
      .set("spark.local.dir", "/data2/tmp")
      .set("spark.driver.maxResultSize", "0")
      .set("spark.executor.heartbeatInterval", "200000")
      .set("spark.network.timeout", "300000")

    val sc = new SparkContext(conf)

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val layout = new SimpleLayout()
    val appender = new FileAppender(layout, logFilePath, false)
    log.addAppender(appender)


    log.info("[GraphJoin] Algorithm: "+algo)
    log.info("[GraphJoin] QgramLen: "+qgramLen)
    log.info("[GraphJoin] InputFile: "+inputFilePath)
    log.info("[GrapjJoin] Matching rule: "+matchingRule)

    val profiles = CommonFunctions.loadProfiles(filePath = inputFilePath, header = true, realIDField = "id", separator = ",")
    profiles.cache()
    val numprofiles = profiles.count()
    log.info("[GraphJoin] Number of profiles " + numprofiles)

    val conditionsPerAttribute = DataPreparator.parseConditions(matchingRule)
    val tokenizedProfiles = DataPreparator.tokenizeProfiles(profiles, conditionsPerAttribute, qgramLen)
    tokenizedProfiles.cache()
    tokenizedProfiles.count()

    val t1 = Calendar.getInstance().getTimeInMillis
    if(algo == TestType.RuleER){
      val m1 = RulERJoin.getMatches(tokenizedProfiles, matchingRule, qgramLen)
    }
    else{
      val m2 = JoinChain.getMatches(tokenizedProfiles, matchingRule, qgramLen)
    }
    val t2 = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] Total time " + (t2 - t1))

    sc.stop()
  }
}
