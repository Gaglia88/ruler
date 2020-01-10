package RulER.Commons

import RulER.Commons.ED.CommonEdFunctions
import RulER.Commons.JS.CommonJsFunctions
import RulER.DataStructure.{Profile, ThresholdTypes}
import RulER.DataStructure.CommonClasses._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object DataPreparator {

  /**
    * Prende in ingresso le condizioni in OR tra di loro
    * e le formatta in modo da averle in un'unica struttura nel formato:
    * attributo -> tipo condizione (ED/JS) -> [soglie]
    **/
  def parseConditions(conditions: List[Map[String, (Double, String)]]): Map[String, Map[String, Set[Double]]] = {
    val allConditions = conditions.flatten
    val conditionsPerAttribute = allConditions.groupBy(_._1)
    conditionsPerAttribute.map { case (attribute, conditions) =>

      val singleConditions = conditions.toSet
      val thresholds = singleConditions.map(_._2.swap)
      val thresholdsPerConditionType = thresholds.groupBy(_._1).map(x => (x._1, x._2.map(_._2)))

      (attribute, thresholdsPerConditionType)
    }
  }

  /**
    * Accumulatore per memorizzare le frequenze dei token
    **/
  class customAccumulator(counter: mutable.Map[(String, String, String), Double]) extends AccumulatorV2[(String, String, String), mutable.Map[(String, String, String), Double]] {

    override def isZero: Boolean = counter.isEmpty

    override def copy(): AccumulatorV2[(String, String, String), mutable.Map[(String, String, String), Double]] = {
      new customAccumulator(counter.clone())
    }

    override def reset(): Unit = counter.clear()

    override def add(v: (String, String, String)): Unit = {
      if (counter.contains(v)) {
        counter.update(v, counter(v) + 1)
      }
      else {
        counter.put(v, 1)
      }
    }

    override def merge(other: AccumulatorV2[(String, String, String), mutable.Map[(String, String, String), Double]]): Unit = {
      other.value.foreach { case (k, v) =>
        if (counter.contains(k)) {
          counter.update(k, counter(k) + v)
        }
        else {
          counter.put(k, v)
        }
      }
    }

    override def value: mutable.Map[(String, String, String), Double] = counter
  }

  /** Tokenizza gli attributi richiesti per la condizione */
  def tokenizeProfiles(profiles: RDD[Profile],
                       conditionsPerAttribute: Map[String, Map[String, Set[Double]]],
                       qgramLength: Int): RDD[(Long, Map[String, Map[String, tokenized]])] = {

    /** Definisco l'accumulatore in cui memorizzare le frequenze, il formato è (tipo soglia, attributo, token) (frequenza) */
    val accum = new customAccumulator(mutable.Map[(String, String, String), Double]())
    profiles.context.register(accum, "Freq. accumulator") // Optional, name it for SparkUI


    /** Estraggo gli elementi (es. token) dagli attributi coinvolti nella condizione di match */
    val profileElements = profiles.map { profile =>

      /** Scorro le condizioni per ogni attributo */
      val elementsPerAttribute = conditionsPerAttribute.flatMap { case (attribute, conditions) =>

        /** Estraggo il valore dell'attributo */
        val originalStr = profile.attributes.filter(_.key == attribute).map(_.value.trim).mkString(" ").toLowerCase.trim

        /* Per ogni tipologia di soglia richiesta (es. JS/ED) estraggo i token o gli n-grammi */
        val elements: Iterable[(String, String, tokenized)] =
          conditions.map { case (thresholdType, threshold) =>

            /** Se la soglia è di tipo JS estraggo i token */
            if (thresholdType == ThresholdTypes.JS) {
              /** Tokenizzo la stringa */
              val tokens = CommonJsFunctions.tokenize(originalStr)

              /** Aggiungo ogni token nell'accumulatore */
              tokens.foreach { x =>
                accum.add((thresholdType, attribute, x))
              }

              /** Se non ci sono token lo segno, altrimenti li aggiungo */
              if (tokens.isEmpty) {
                (thresholdType, attribute, noTokens())
              }
              else {
                (thresholdType, attribute, tokensStrJs(tokens))
              }
            }

            /** Edit distance, devo fare gli n-grammi */
            else {
              /** Ottengo gli ngrammi */
              val qgrams = CommonEdFunctions.getQgrams(originalStr, qgramLength)

              /** Segno la frequenza nell'accumulatore */
              qgrams.foreach { case (qgram, pos) =>
                accum.add((thresholdType, attribute, qgram))
              }

              /** Se non ci sono qgrammi lo segnalo, altrimenti li aggiungo */
              if (qgrams.isEmpty) {
                (thresholdType, attribute, noTokens())
              }
              else {
                (thresholdType, attribute, tokensStrED(qgrams, originalStr))
              }
            }
          }
        elements
      }

      (profile.id, elementsPerAttribute)
    }

    /** Mantengono solo i profili per cui sono stati estratti tutti gli elementi */
    val validProfiles = profileElements.filter { case (profileID, elementsPerAttribute) =>
      !elementsPerAttribute.exists { case (thresholdType, attribute, elements) =>
        elements.getType() == "empty"
      }
    }
    validProfiles.cache()
    validProfiles.count()


    /** Suddivido le tf per tipo di soglia e attributo, es (JS, Title) => (tokens, freq), (ED, Title) => (qgrams, freq) */
    val tfPerThresholdTypeAndAttribute = accum.value.toArray.map { case ((thresholdType, attribute, token), df) =>
      ((thresholdType, attribute), (token, df))
    }.groupBy(_._1)

    val tfMap = tfPerThresholdTypeAndAttribute.map { case (key, values) =>

      /** Estraggo le tf dai gruppi, quindi avrò la forma (token, tf)
        * e ordino per tf in modo da avere prima i token meno frequenti
        * */
      val tf = values.map(_._2).sortBy { case (token, tf) => (tf, token) }
      /** Ottengo una mappa in cui i token vengono numerati in base all'ordine hanno in base alla TF */
      val tfID = tf.zipWithIndex.map { case ((token, tf), id) => (token, id) }
      (key, tfID.toMap)
    }

    /** Invio in broadcast la mappa che mi consente di ottenere per ogni token/qgramma un id univoco */
    val tf = profiles.context.broadcast(tfMap)

    /** Sostituisco le stringhe con gli id generati */
    val profileTokens = validProfiles.map { case (profileId, attributesElements) =>
      val attributesElementsConv: Iterable[(String, String, tokenized)] = attributesElements.map { case (thresholdType, attribute, elements) =>

        /** Se il tipo di soglia è jaccard */
        if (thresholdType == ThresholdTypes.JS) {
          /** Leggo i token che erano stati prodotti */
          val tokens = elements.asInstanceOf[tokensStrJs].tokens
          /** Sostituisco i token testuali con gli interi e poi li ordino */
          val sortedTokens = tokens.map { token =>
            tf.value(thresholdType, attribute)(token).toLong
          }.toArray.sorted
          (thresholdType, attribute, tokensJs(sortedTokens))
        }
        else {
          /** Se il tipo di soglia è ED leggo i qgrammi generati prima */
          val qgrams = elements.asInstanceOf[tokensStrED]
          /** Converto i qgrammi da stringhe a interi e poi li ordino, in modo da avere prima i meno frequenti e poi i più frequenti */
          val intQgrams = qgrams.qgrams.map { case (qgram, pos) =>
            (tf.value(thresholdType, attribute)(qgram), pos)
          }
          val sortedQgrams = intQgrams.sortBy { case (qgramId, qgramPos) => (qgramId, qgramPos) }
          (thresholdType, attribute, tokensED(sortedQgrams, qgrams.originalStr))
        }
      }

      /**
        * Alla fine converto tutto in una mappa che data la tipologia di soglia (ED/JS) e l'attributo mi restituisce
        * gli elementi (token/qgrammi)
        **/
      val attributesElementsConvPerThresholdType = attributesElementsConv.groupBy { case (thresholdType, attribute, elements) => thresholdType }

      val finalMap = attributesElementsConvPerThresholdType.map { case (thresholdType, attributeElements1) =>
        val attributeElements = attributeElements1.map { case (thresType, attribute, elements) =>
          (attribute, elements)
        }
        (thresholdType, attributeElements.toMap)
      }

      (profileId, finalMap)
    }

    profileTokens.cache()
    profileTokens.count()
    tf.unpersist()

    profileTokens
  }
}
