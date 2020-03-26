package RulER.SimJoins

import java.util.Calendar

import RulER.Commons.ED.CommonEdFunctions.commons
import RulER.Commons.ED.EdFilters
import RulER.Commons.JS.JsFilters
import RulER.Commons.{DataPreparator, DataVerificator}
import RulER.DataStructure.CommonClasses._
import RulER.DataStructure.{Profile, ThresholdTypes}
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.DoubleAccumulator

object RulERJoin {
  /**
    * Esegue il calcolo per l'edit distance
    **/
  def computeED(
                 qgrams: Array[(Int, Int)],
                 qgramLength: Int,
                 threshold: Int,
                 prefixes: Map[Int, Array[indexDocs]],
                 docId: Long,
                 cbs: Array[Int],
                 neighbors: Array[(Long, Int)],
                 numneighborsExt: Int,
                 numExcludedExt: Int,
                 excluded: Array[Int],
                 pos: Array[Int],
                 myPos: Array[Int],
                 visited: Array[Boolean],
                 separatorID: Long,
                 compNumber: DoubleAccumulator
               ): (Int, Int, Int) = {
    var numneighbors = numneighborsExt
    var numExcluded = numExcludedExt
    val prefixLen = EdFilters.getPrefixLen(qgramLength, threshold.toInt)
    val docLen = qgrams.length
    //Prendo il suo prefisso, nel caso in cui sia troppo corto allora devo guardare anche il blocco speciale
    val prefix = {
      if (docLen < prefixLen) {
        qgrams.union(commons.fixPrefix :: Nil)
      }
      else {
        qgrams.take(prefixLen)
      }
    }
    //Per ogni elemento nel prefisso
    for (i <- prefix.indices) {
      val qgram = prefix(i)._1
      val qgramPos = prefix(i)._2
      //Prendo il blocco relativo a quell'elemento (se esiste)
      val block = prefixes.get(qgram)
      if (block.isDefined) {
        val validneighbors = block.get.map(i => i.asInstanceOf[indexED]).withFilter(n => docId < n.docId && cbs(n.docId.toInt) >= 0 && (separatorID < 0 || (docId <= separatorID && n.docId > separatorID)))

        /** Per ogni vicino valido */
        validneighbors.foreach { neighbor =>
          val nId = neighbor.docId.toInt

          if (!visited(nId)) {
            /** Se il CBS è 0 è la prima volta che lo trovo */
            if (cbs(nId) == 0) {
              /** Essendo la prima volta che lo incontro lo testo con il length filter */
              if (math.abs(docLen - neighbor.docLength) <= threshold) {
                compNumber.add(1)
                /** Lo posso aggiungere solo se le loro posizioni non sono distanti più della soglia
                  * altrimenti non posso dire niente. Non lo posso escludere ancora però,
                  * perché potrebbero avere una posizione abbastanza vicina in qualche altro blocco.
                  * */
                if (math.abs(qgramPos - neighbor.qgramPos) <= threshold) {
                  neighbors.update(numneighbors, (nId, 0))
                  numneighbors += 1
                }
              }
              else {
                /** Se non lo passa lo droppo */
                cbs.update(nId, -1)
                excluded.update(numExcluded, nId)
                numExcluded += 1
              }
            }

            /**
              * Devo verificare che sia valido, potrei averlo annullato con il length filter
              * Inoltre perché sia valido, oltre ad appartenere allo stesso blocco, deve anche essere
              * sufficientemente vicino (positional filter).
              **/
            if (cbs(nId) >= 0 && math.abs(qgramPos - neighbor.qgramPos) <= threshold) {
              /** Metto a +1 le volte in cui l'ho visto (alla fine ottengo i q-grammi comuni nel prefisso) */
              cbs.update(nId, cbs(nId) + 1)

              /** Salvo l'ultima posizione in cui l'ho visto */
              if (pos(nId) < neighbor.sortedPos) {
                pos.update(nId, neighbor.sortedPos)
              }

              /** Salvo la mia posizione in cui l'ho visto (di sicuro è sempre l'ultima) */
              myPos.update(nId, i + 1)
            }
          }
        }
      }
    }
    (docLen, numneighbors, numExcluded)
  }

  /**
    * Esegue il calcolo per la Jaccard Similarity
    **/
  def computeJS(tokens: Array[Long],
                threshold: Double,
                prefixes: Map[Int, Array[indexDocs]],
                docId: Int,
                cbs: Array[Int],
                neighbors: Array[(Long, Int)],
                numneighborsCur: Int,
                numExcludedCur: Int,
                excluded: Array[Int],
                pos: Array[Int],
                myPos: Array[Int],
                visited: Array[Boolean],
                separatorID: Long,
                compNumber: DoubleAccumulator
               ): (Int, Int, Int) = {
    var numneighbors = numneighborsCur
    var numExcluded = numExcludedCur
    /** Calcola la lunghezza dell'attributo (numero di token) */
    val docLen = tokens.length
    /** Legge il prefix-index per questo attributo */
    val prefixLen = JsFilters.getPrefixLength(docLen, threshold)

    /** Per ogni token all'interno del prefisso */
    for (i <- 0 until prefixLen) {
      /** Legge dall'indice (se esiste) il blocco corrispondente a questo token */
      val block = prefixes.get(tokens(i).toInt)
      if (block.isDefined) {
        /**
          * Prende i vicini che:
          *  - Hanno Id < del documento attuale (evito duplicati)
          *  - Sono ancora validi, ossia hanno passato uno step precedente cbs >= 0
          **/
        val validneighbors = block.get.map(i => i.asInstanceOf[indexJS]).withFilter(n => docId < n.docId && cbs(n.docId.toInt) >= 0 && (separatorID < 0 || (docId <= separatorID && n.docId > separatorID)))

        /** Per ogni vicino */
        validneighbors.foreach { neighbor =>

          val nId = neighbor.docId.toInt


          if (!visited(nId)) {

            /** Se il CBS è 0 è la prima volta che lo trovo */
            if (cbs(nId) == 0) {
              /** Essendo la prima volta che lo incontro lo testo con il length filter */
              if (math.min(docLen, neighbor.docLen) >= math.max(docLen, neighbor.docLen) * threshold) {
                compNumber.add(1)
                neighbors.update(numneighbors, (nId, neighbor.docLen))
                numneighbors += 1
              }
              else {
                /** Se non lo passa lo droppo */
                cbs.update(nId, -1)
                excluded.update(numExcluded, nId)
                numExcluded += 1
              }
            }

            /**
              * Devo verificare che sia valido, potrei averlo annullato con il length filter
              **/
            if (cbs(nId) >= 0) {
              /** Salvo l'ultima posizione in cui l'ho visto */
              if (pos(nId) < neighbor.tokenPos) {
                pos.update(nId, neighbor.tokenPos)
              }

              /** Salvo la mia posizione in cui l'ho visto (di sicuro è sempre l'ultima) */
              myPos.update(nId, i + 1)

              /** Metto a +1 le volte in cui l'ho visto (alla fine ottengo i token comuni nel prefisso) */
              cbs.update(nId, cbs(nId) + 1)
            }
          }
        }
      }
    }

    (docLen, numneighbors, numExcluded)
  }

  /** Crea i prefix index */
  def buildPrefixIndexes(tokenizedProfiles: RDD[(Long, Map[String, Map[String, tokenized]])],
                         conditionsPerAttribute: Map[String, Map[String, Set[Double]]],
                         qgramLength: Int): Map[(String, String, Double), Map[Int, Array[indexDocs]]] = {
    /* Per ogni profilo */
    val partIndex = tokenizedProfiles.flatMap { case (profileID, conditionsMap) =>
      /* Per ogni tipologia di condizione (ED, JS) */
      conditionsMap.flatMap { case (conditionType, tokensPerAttribute) =>
        /* Per ogni attributo su cui vi è una condizione */
        tokensPerAttribute.flatMap { case (attribute, tokens) =>
          /*  Per ognuna delle soglie specificate*/
          val thresholds = conditionsPerAttribute(attribute)(conditionType)
          /* Genera i dati che servono nel prefix index */
          thresholds.flatMap { threshold =>
            if (conditionType == ThresholdTypes.JS) {
              val tokensJs = tokens.asInstanceOf[tokensJs].tokens
              val prefix = JsFilters.getPrefix(tokensJs, threshold)
              val el: Array[((String, String, Double, Int), indexDocs)] = prefix.zipWithIndex.map { case (token, pos) =>
                ((conditionType, attribute, threshold, token.toInt), indexJS(profileID, pos + 1, tokensJs.length))
              }
              el
            }
            else {
              val qgrams = tokens.asInstanceOf[tokensED].qgrams
              val prefixLen = EdFilters.getPrefixLen(qgramLength, threshold.toInt)
              /*val prefix = {
                if (qgrams.length < prefixLen) {
                  qgrams.union(commons.fixPrefix :: Nil)
                }
                else {
                  qgrams.take(prefixLen)
                }
              }*/
              val prefix = qgrams.take(prefixLen)
              val el: Array[((String, String, Double, Int), indexDocs)] = prefix.zipWithIndex.map { case (qgram, index) =>
                ((conditionType, attribute, threshold, qgram._1), indexED(profileID, qgrams.length, qgram._2, index))
              }
              el
            }
          }
        }
      }
    }

    /* Genera i blocchi */
    val nonEmptyBlocks = partIndex.groupByKey().filter(_._2.size > 1).map(g => (g._1, g._2.toArray))

    /** Genera una mappa che data tipo soglia, attributo, soglia ritorna il prefix index */
    val prefixIndexes = nonEmptyBlocks.collect().map { case ((thresholdType, attribute, threshold, blockID), block) =>
      ((thresholdType, attribute, threshold), (blockID, block))
    }.groupBy(_._1).map(x => (x._1, x._2.map(_._2).toMap))

    prefixIndexes
  }


  /**
    * Ritorna l'elenco di coppie candidate a superare tutte le condizioni
    **/
  def getCandidates(tokenizedProfiles: RDD[(Long, Map[String, Map[String, tokenized]])],
                    conditionsOR: List[Map[String, (Double, String)]],
                    prefixIndexes: Broadcast[Map[(String, String, Double), Map[Int, Array[indexDocs]]]],
                    maxId: Int,
                    qgramLength: Int,
                    separatorID: Long,
                    compNumber: DoubleAccumulator): RDD[(Long, Long)] = {
    tokenizedProfiles.mapPartitions { part =>

      /** Elenco di candidati */
      var candidates: List[(Long, Long)] = Nil

      /** --------------------- VARIABILI PER ESEGUIRE L'OR ------------------------ */

      /** Nel processo di OR indica i vicini che sono già stati visti */
      val visited = Array.fill[Boolean](maxId) {
        false
      }
      /** Numero di record emessi per ogni profilo */
      var numEmitted = 0
      /** Record emessi per ogni profilo, serve per evitarne la computazione quando si fanno le condizioni di AND */
      val emitted = Array.ofDim[Int](maxId)

      /** --------------------- VARIABILI PER ESEGUIRE L'AND ------------------------ */

      /** Lunghezza documento (usato nella JS) */
      var docLen = 0
      /** Numero record esclusi */
      var numExcluded = 0
      /** Record esclusi */
      val excluded: Array[Int] = Array.ofDim(maxId)
      /** Record vicini */
      val neighbors = Array.ofDim[(Long, Int)](maxId)
      /** Posizione in cui il record esterno ha matchato con un vicino */
      val myPos = Array.ofDim[Int](maxId)
      /** Posizione in cui ho trovato il vicino */
      val pos = Array.ofDim[Int](maxId)
      /** Numero di blocchi in comune con un certo vicino, in pratica è il numero di token */
      val cbs = Array.fill[Int](maxId) {
        0
      }
      /** Serve ad indicare che si sta eseguendo la prima condizione */
      var isFirst = true
      /** Numero di vicini */
      var numneighbors = 0
      /** Primi vicini validi trovati alla prima condizione */
      val firstneighbors = Array.ofDim[Int](maxId)
      /** Numero dei primi vicini validi trovati */
      var firstneighborsNum = 0

      part.foreach { case (docId, attributesTokens) =>

        /** Per ogni set di condizioni in OR tra di loro */
        conditionsOR.foreach { conditionsAND =>

          /** Segno che è la prima condizione tra quelle in AND che viene eseguita */
          isFirst = true

          /** Per ogni condizione in questo blocco AND calcolo i vicini validi */
          conditionsAND.foreach { case (attribute, (threshold, thresholdType)) =>

            /** Controllo che il record corrente abbia l'attributo su cui applicare la condizione e ottengo i token */
            val attributeIndex = attributesTokens.get(thresholdType)

            if (attributeIndex.isDefined) {
              val tokenIndex = attributeIndex.get.get(attribute)
              if (tokenIndex.isDefined) {
                val tokens = tokenIndex.get

                /** Calcola la Jaccard Similarity, vuole dire che la condizione da applicare è una JS */
                if (thresholdType == ThresholdTypes.JS) {
                  computeJS(
                    tokens.asInstanceOf[tokensJs].tokens,
                    threshold,
                    prefixIndexes.value((thresholdType, attribute, threshold)),
                    docId.toInt,
                    cbs,
                    neighbors,
                    numneighbors,
                    numExcluded,
                    excluded,
                    pos,
                    myPos,
                    visited,
                    separatorID,
                    compNumber
                  )
                  match {
                    case (dLen, nneighbors, nExcl) =>
                      docLen = dLen
                      numneighbors = nneighbors
                      numExcluded = nExcl
                  }
                }

                /** Calcola l'Edit Distance */
                else {
                  computeED(
                    tokens.asInstanceOf[tokensED].qgrams,
                    qgramLength,
                    threshold.toInt,
                    prefixIndexes.value((thresholdType, attribute, threshold)),
                    docId,
                    cbs,
                    neighbors,
                    numneighbors,
                    numExcluded,
                    excluded,
                    pos,
                    myPos,
                    visited,
                    separatorID,
                    compNumber
                  )
                  match {
                    case (dLen, nneighbors, nExcl) =>
                      docLen = dLen
                      numneighbors = nneighbors
                      numExcluded = nExcl
                  }
                }


                /**
                  * Se alla fine del processo ci sono dei vicini che erano nel primo attributo che non sono stati trovati
                  * in questo attributo, devono essere annullati.
                  * Se non sono stati trovati hanno il cbs a 0
                  **/
                for (i <- 0 until firstneighborsNum if cbs(firstneighbors(i)) == 0) {
                  cbs.update(firstneighbors(i), -1)
                  excluded.update(numExcluded, firstneighbors(i))
                  numExcluded += 1
                }

                /** Scorre i vicini validi */
                for (i <- 0 until numneighbors) {
                  val nId = neighbors(i)._1.toInt
                  if (thresholdType == ThresholdTypes.JS) {
                    /** Li testa con il position filter, se lo passano li aggiunge tra quelli mantenuti, altrimenti li droppa */
                    if (JsFilters.positionFilter(docLen, neighbors(i)._2, myPos(nId), pos(nId), cbs(nId), threshold)) {
                      /** Lo mantengo, l'aggiunta a fistneighbors la fa solo il primo attributo, gli altri non ha senso che la facciano,
                        * al massimo rimuovono! Quindi questi poi li verificherò alla fine che non abbiano il cbs settato a -1 */
                      if (isFirst) {
                        firstneighbors.update(firstneighborsNum, nId)
                        firstneighborsNum += 1
                      }
                      cbs.update(nId, 0)
                    }
                    else {
                      /** Droppo il vicino */
                      cbs.update(nId, -1)
                      excluded.update(numExcluded, nId)
                      numExcluded += 1
                    }
                  }
                  else {
                    /** Lo mantengo, l'aggiunta a firstneighbors la fa solo il primo attributo, gli altri non ha senso che la facciano,
                      * al massimo rimuovono! Quindi questi poi li verificherò alla fine che non abbiano il cbs settato a -1 */
                    if (isFirst) {
                      firstneighbors.update(firstneighborsNum, nId)
                      firstneighborsNum += 1
                    }
                    cbs.update(nId, 0)
                  }
                  pos.update(nId, 0)
                }

                /** Resetto il numero di vicini per il prossimo giro */
                numneighbors = 0

                /** Dice che ha finito di fare il primo attributo */
                isFirst = false
              }
            }
          }

          /** Emetto le coppie candidate relative a questo profilo */
          for (i <- 0 until firstneighborsNum if cbs(firstneighbors(i)) >= 0) {
            candidates = (docId, firstneighbors(i).toLong) :: candidates
            emitted.update(numEmitted, firstneighbors(i))
            numEmitted += 1

            /** Segno che il profilo è stato visitato, in questo modo se un succesivo blocco AND lo trova, non lo fa */
            visited.update(firstneighbors(i), true)
          }

          /** Alla fine di tutti gli attributi, resetto tutto per il prossimo documento */
          for (i <- 0 until numExcluded) {
            cbs.update(excluded(i), 0)
          }
          numExcluded = 0
          firstneighborsNum = 0
        }

        /** Alla fine del documento, sono state eseguite tutte le condizioni in OR, resetto i dati sui profili visti */
        for (i <- 0 until numEmitted) {
          visited.update(emitted(i), false)
        }
        numEmitted = 0
      }

      candidates.toIterator
    }
  }


  def getMatches(tokenizedProfiles: RDD[(Long, Map[String, Map[String, tokenized]])],
                 conditionsOR: List[Map[String, (Double, String)]],
                 qgramLength: Int = 2,
                 separatorID: Long = -1): RDD[(Long, Long)] = {

    val log = LogManager.getRootLogger

    val t1 = Calendar.getInstance().getTimeInMillis

    /** Id massimo dei profili */
    val maxId = tokenizedProfiles.map(_._1).max().toInt + 1
    /**
      * Formatta le condizioni in AND tra loro, in modo da averle in un'unica lista
      **/
    val conditionsPerAttribute = DataPreparator.parseConditions(conditionsOR)

    val tj = Calendar.getInstance().getTimeInMillis
    val prefixIndexes = tokenizedProfiles.context.broadcast(buildPrefixIndexes(tokenizedProfiles, conditionsPerAttribute, qgramLength))
    val tk = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] Tempo indexes" + (tk - tj) / 1000.0)

    val t2 = Calendar.getInstance().getTimeInMillis
    log.info("[GraphJoin] - Preprocessing time (min) " + ((t2 - t1) / 1000.0) / 60.0)

    val compNum = tokenizedProfiles.context.doubleAccumulator("comparisonNumber")
    val candidates = getCandidates(tokenizedProfiles, conditionsOR, prefixIndexes, maxId, qgramLength, separatorID, compNum)
    candidates.cache()
    val nc = candidates.count()
    val t3 = Calendar.getInstance().getTimeInMillis
    prefixIndexes.unpersist()
    log.info("[GraphJoin] - Num candidates " + nc)
    log.info("[GraphJoin] - Number of comparisons " + compNum.value)
    log.info("[GraphJoin] - Join time (min) " + ((t3 - t2) / 1000.0) / 60.0)


    val docTokens = tokenizedProfiles.context.broadcast(tokenizedProfiles.collectAsMap())
    val results = DataVerificator.verify(candidates, docTokens, conditionsOR)
    val matches = results.count()
    val t4 = Calendar.getInstance().getTimeInMillis
    docTokens.unpersist()
    log.info("[GraphJoin] - Verify time (min) " + ((t4 - t3) / 1000.0) / 60.0)
    log.info("[GraphJoin] - Num matches " + matches)

    results
  }

}
