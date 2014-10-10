package com.ataraxer.ottla
package client
package protocol

import java.nio.ByteBuffer


case class TopicMetadataRequest(
    topics: Seq[String],
    clientId: String,
    correlationId: Int)
  extends KafkaMessage
{
  import KafkaProtocol._

  private val clientIdBytes = clientId.getBytes(Encoding)


  def size = {
    4 + // request size
    2 + // request id size
    2 + // version
    4 + // correlation id
    2 + // size of a client id
    clientIdBytes.size +
    4 + // number of topics
    topics.map(2 + _.getBytes(Encoding).size).sum
  }


  def getBytes: ByteBuffer = {
    val buffer = ByteBuffer.allocate(this.size)

    buffer.putInt(2 + this.size) // request size
    buffer.putShort(3) // topic request api key

    buffer.putShort(0) // version
    buffer.putInt(correlationId)

    buffer.putString(clientId)

    buffer.putInt(topics.size)

    topics.foreach(buffer.putString)

    buffer
  }
}


// vim: set ts=2 sw=2 et:
