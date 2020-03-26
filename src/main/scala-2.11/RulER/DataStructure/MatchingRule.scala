package RulER.DataStructure

import java.util

import org.jgrapht.alg.ConnectivityInspector
import org.jgrapht.graph.{DefaultEdge, SimpleGraph}

import scala.collection.mutable

case class MatchingRule(orRules: mutable.MutableList[AndBlock] = new mutable.MutableList[AndBlock]()) {

  def or(that: AndBlock): MatchingRule = {
    addAndBlock(that)
  }

  def addRule(that: Rule): MatchingRule = {
    this.orRules += AndBlock().addRule(that)
    this
  }

  def addAndBlock(that: AndBlock): MatchingRule = {
    this.orRules += that
    this
  }

  def and(that: AndBlock): MatchingRule = {
    addAndBlock(that)
  }

  def and(that: Rule): MatchingRule = {
    addRule(that)
  }

  override def toString: String = {
    orRules.map(x => "(" + x + ")").mkString(" OR ")
  }

  def getFormat: List[Map[String, (Double, String)]] = {
    orRules.map(_.getFormat).toList
  }


  def getUniqueRuleRep: Map[String, String] = {
    val edges = orRules.flatMap { x =>
      x.andRules.map { y =>
        (y.attribute + "_d1", y.secondAttribute + "_d2")
      }
    }

    val graph = new SimpleGraph[String, DefaultEdge](classOf[DefaultEdge])


    val vertex = edges.flatMap(x => x._1 :: x._2 :: Nil).toSet
    vertex.foreach { v =>
      graph.addVertex(v)
    }

    edges.foreach { case (from, to) =>
      graph.addEdge(from, to)
    }

    val ci = new ConnectivityInspector(graph)

    val connectedComponents = ci.connectedSets()

    def removeDataset(a: String): String = {
      val p = a.split("_")
      p.take(p.length - 1).mkString("_")
    }

    val clusters: Iterable[(Iterable[String], Int)] = (for (i <- 0 until connectedComponents.size()) yield {
      val a = connectedComponents.get(i).asInstanceOf[util.HashSet[String]].iterator()
      var l: List[String] = Nil
      while (a.hasNext) {
        l = removeDataset(a.next()) :: l
      }
      (l, i)
    }).filter(_._1.nonEmpty)

    clusters.flatMap(x => x._1.map(y => (y, "c_" + x._2))).toMap
  }

  def convertRule(conv: Map[String, String]): MatchingRule = {
    MatchingRule(orRules.map { andBlock =>
      AndBlock(
        andBlock.andRules.map { rule =>
          Rule(conv(rule.attribute), rule.thresholdType, rule.threshold)
        }
      )
    })
  }

}