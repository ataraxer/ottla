package com.ataraxer.ottla

import scala.collection.mutable
import scala.util.Random


object Kafka {
  case class Partition(topic: String, id: Int)


  def balanceReplicas(
    brokerList: Seq[Int],
    partitionCount: Int,
    replicationFactor: Int) =
  {
    val result = mutable.HashMap.empty[Int, List[Int]]

    val brokerCount = brokerList.size

    val startIndex = Random.nextInt(brokerCount)
    var nextReplicaShift = Random.nextInt(brokerCount)

    for (partitionId  <- 0 until partitionCount) {
      if (partitionId > 0 && (partitionId % brokerCount == 0)) {
        nextReplicaShift += 1
      }

      val firstReplicaIndex = (partitionId + startIndex) % brokerCount

      val replicaList = for {
        j <- 0 until replicationFactor - 1
      } yield {
        brokerList(
          replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerCount))
      }

      result.put(partitionId, brokerList(firstReplicaIndex) :: replicaList.toList)
    }

    result.toMap
  }


  private def replicaIndex(
    firstReplicaIndex: Int,
    secondReplicaShift: Int,
    replicaIndex: Int,
    nBrokers: Int): Int =
  {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }
}


// vim: set ts=2 sw=2 et:
