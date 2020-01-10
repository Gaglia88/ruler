package RulER.DataStructure

import org.apache.spark.Partitioner

class MyPartitioner(numPart: Int) extends Partitioner {
  val load = Array.fill[Double](numPart)(0.0)

  override def getPartition(key: Any): Int = {
    val part = load.indexOf(load.min)
    val s = key.asInstanceOf[Array[(Long, Int, Int)]].length.toDouble
    load.update(part, load(part) + (s * s - 1))
    part
  }

  override def numPartitions: Int = numPart
}