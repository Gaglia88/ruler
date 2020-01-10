package RulER.Commons

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import RulER.DataStructure.{KeyValue, Profile}

import scala.collection.mutable

object CommonFunctions {

  def extractField(profiles: RDD[Profile], fieldName: String): RDD[(Long, String)] = {
    profiles.map { profile =>
      (profile.id, profile.attributes.filter(_.key == fieldName).map(_.value).mkString(" ").toLowerCase)
    }.filter(!_._2.trim.isEmpty)
  }

  /**
    * Given a row return the list of attributes
    *
    * @param columnNames names of the dataframe columns
    * @param row         single dataframe row
    **/
  def rowToAttributes(columnNames: Array[String], row: Row, explodeInnerFields: Boolean = false, innerSeparator: String = ","): mutable.MutableList[KeyValue] = {
    val attributes: mutable.MutableList[KeyValue] = new mutable.MutableList()
    for (i <- 0 until row.size) {
      try {
        val value = row(i)
        val attributeKey = columnNames(i)

        if (value != null) {
          value match {
            case listOfAttributes: Iterable[Any] =>
              listOfAttributes map {
                attributeValue =>
                  attributes += KeyValue(attributeKey, attributeValue.toString)
              }
            case stringAttribute: String =>
              if (explodeInnerFields) {
                stringAttribute.split(innerSeparator) map {
                  attributeValue =>
                    attributes += KeyValue(attributeKey, attributeValue)
                }
              }
              else {
                attributes += KeyValue(attributeKey, stringAttribute)
              }
            case singleAttribute =>
              attributes += KeyValue(attributeKey, singleAttribute.toString)
          }
        }
      }
      catch {
        case e: Throwable => println(e)
      }
    }
    attributes
  }


  def dfProfilesToRDD(df: DataFrame, startIDFrom: Long = 0, explodeInnerFields: Boolean = false, innerSeparator: String = ",", realIDField: String = ""): RDD[Profile] = {
    val columnNames = df.columns

    df.rdd.map(row => rowToAttributes(columnNames, row, explodeInnerFields, innerSeparator)).zipWithIndex().map {
      profile =>
        val profileID = profile._2 + startIDFrom
        val attributes = profile._1
        val realID = {
          if (realIDField.isEmpty) {
            ""
          }
          else {
            attributes.filter(_.key == realIDField).map(_.value).mkString("").trim
          }
        }
        Profile(profileID, attributes.filter(kv => kv.key != realIDField), realID)
    }
  }

  def loadProfilesAsDF(filePath: String, separator: String = ",", header: Boolean = true): DataFrame = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val df = sparkSession.read.option("header", header).option("sep", separator).option("delimiter", "\"").option("escape", "\"").option("quote", "\"").csv(filePath)
    df
  }

  def loadProfiles(filePath: String, startIDFrom: Long = 0, separator: String = ",", header: Boolean = false,
                   explodeInnerFields: Boolean = false, innerSeparator: String = ",", realIDField: String = ""): RDD[Profile] = {
    val df = loadProfilesAsDF(filePath, separator, header)
    dfProfilesToRDD(df, startIDFrom, explodeInnerFields, innerSeparator, realIDField)
  }
}
