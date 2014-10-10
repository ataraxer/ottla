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
    4 + // request size
    2 + // request id size
    2 + // version
    4 + // correlation id
    2 + // size of a client id
    clientIdBytes.size +
    4 + // consumer id
    4 + // number of topics
    requestsSize
  }


  def getBytes: ByteBuffer = {
    val buffer = ByteBuffer.allocate(this.size)

    buffer.putInt(2 + this.size) // request size
    buffer.putShort(2) // offset request api key

    buffer.putShort(0) // version
    buffer.putInt(correlationId)

    buffer.putString(clientId)

    buffer.putInt(consumerId)

    buffer.putInt(groupedRequests.size)

    groupedRequests foreach { case (topic, requestList) =>
      buffer.putString(topic)

      buffer.putInt(requestList.size)

      requestList foreach { request =>
        buffer.putInt(request.partition.id)
        buffer.putLong(request.beforeTime)
        buffer.putInt(request.numberLimit)
      }
    }

    buffer
  }
}


// vim: set ts=2 sw=2 et:
