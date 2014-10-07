package com.ataraxer.ottla
package client
package protocol

import java.nio.ByteBuffer
import scala.language.postfixOps


object PartitionRequest {
  def size = {
    4 + // partition
    8 + // before time
    4   // number limit
  }
}


case class PartitionRequest(
  partition: Kafka.Partition,
  beforeTime: Long,
  numberLimit: Int)


case class OffsetRequest(
    requests: Seq[PartitionRequest],
    topics: Seq[String],
    consumerId: Int,
    clientId: String,
    correlationId: Int)
  extends KafkaMessage
{
  import KafkaProtocol._

  private val clientIdBytes = clientId.getBytes(Encoding)

  private val groupedRequests = requests.groupBy(_.partition.topic)

  private val requestsSize = {
    groupedRequests map { case (topic, requests) =>
      topic.getBytes(Encoding).size +
      4 + // partition count
      PartitionRequest.size * requests.size
    } sum
  }


  def size = {
    2 + // version
    4 + // correlation id
    2 + // size of a client id
    clientIdBytes.size +
    4 + // replica id
    4 + // number of topics
    requestsSize
  }


  def getBytes: ByteBuffer = {
    val buffer = ByteBuffer.allocate(0)
    // TODO: implement
    buffer
  }
}


// vim: set ts=2 sw=2 et:
