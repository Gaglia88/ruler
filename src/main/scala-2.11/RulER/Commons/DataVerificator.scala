package RulER.Commons

import RulER.Commons.ED.CommonEdFunctions
import RulER.DataStructure.CommonClasses.{tokenized, tokensED, tokensJs}
import RulER.DataStructure.ThresholdTypes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object DataVerificator {

  /**
    * Verifica i candidati, mantenendo solo quelli che soddisfano le condizioni
    **/
  def verify(candidates: RDD[(Long, Long)],
             docTokens: Broadcast[scala.collection.Map[Long, Map[String, Map[String, tokenized]]]],
             conditionsOR: List[Map[String, (Double, String)]]): RDD[(Long, Long)] = {


    //docTokens = Map(2012 -> Map(JS -> Map(authors -> tokensJs([J@351cc4b5), title -> tokensJs([J@6dfd4db2))))
    //conditions OR = List(Map(title -> (0.8,JS), authors -> (0.5,JS)))


    /**
      * Dati due documenti e la soglia, verifica che la passino
      **/
    def passJS(doc1: Array[Long], doc2: Array[Long], threshold: Double): Boolean = {
      val common = doc1.intersect(doc2).length
      (common.toDouble / (doc1.length + doc2.length - common)) >= threshold
    }

    candidates.filter { case (doc1, doc2) =>
      val d1 = docTokens.value.get(doc1)
      val d2 = docTokens.value.get(doc2)
      var noPassOR = true
      var passAND: Boolean = true

      if (d1.isDefined && d2.isDefined) {
        val docs1 = d1.get
        val docs2 = d2.get

        val conditionsORIt = conditionsOR.iterator

        /**
          * Continua o finché non sono finite le condizioni in OR
          * o finché uno dei blocchi in AND non dà esito positivo, se uno dei blocchi
          * AND dà esito positivo è inutile provare gli altri.
          **/
        while (conditionsORIt.hasNext && noPassOR) {
          val conditionsAND = conditionsORIt.next()
          val conditionsANDIt = conditionsAND.iterator
          passAND = true

          /**
            * Continua finché passa tutte le singole condizioni in AND tra loro, o finché non sono terminate
            **/
          while (conditionsANDIt.hasNext && passAND) {
            val (attribute, (threshold, thresholdType)) = conditionsANDIt.next()
            val docs1T = docs1.get(thresholdType)
            val docs2T = docs2.get(thresholdType)
            if (docs1T.isDefined && docs2T.isDefined) {
              val tokensD1 = docs1T.get.get(attribute)
              val tokensD2 = docs2T.get.get(attribute)
              if (tokensD1.isDefined && tokensD2.isDefined) {
                if (thresholdType == ThresholdTypes.JS) {
                  //Controllo la jaccard
                  passAND = passJS(tokensD1.get.asInstanceOf[tokensJs].tokens, tokensD2.get.asInstanceOf[tokensJs].tokens, threshold)
                }
                else {
                  //Controllo con l'edit distance
                  passAND = CommonEdFunctions.editDist(tokensD1.get.asInstanceOf[tokensED].originalStr, tokensD2.get.asInstanceOf[tokensED].originalStr) <= threshold
                }
              }
            }
          }

          /**
            * Se ha terminato il ciclo perché ha finito (passAND = true), allora fermo il processo, le altre condizioni non devono essere verificate.
            * Altrimenti continua.
            **/
          noPassOR = !passAND
        }
      }
      else {
        noPassOR = true
      }
      !noPassOR
    }
  }

}
