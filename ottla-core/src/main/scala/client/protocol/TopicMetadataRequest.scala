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
  import KafkaMessage.Encoding

  private val clientIdBytes = clientId.getBytes(Encoding)
  private val topicListBytes = topics.map(_.getBytes(Encoding))


  def size = {
    2 + // version
    4 + // correlation id
    2 + // size of a client id
    clientIdBytes.size +
    4 + // number of topics
    topicListBytes.map(2 + _.size).sum
  }


  def getBytes: ByteBuffer = {
    val buffer = ByteBuffer.allocate(
      4 + // for request size
      2 + // for request id size
      this.size)

    buffer.putInt(this.size + 2)
    buffer.putShort(3: Short)

    buffer.putShort(0) // version
    buffer.putInt(correlationId)

    buffer.putShort(clientIdBytes.size.asInstanceOf[Short])
    buffer.put(clientIdBytes)

    buffer.putInt(topics.size)

    for (topicBytes <- topicListBytes) {
      buffer.putShort(topicBytes.size.asInstanceOf[Short])
      buffer.put(topicBytes)
    }

    buffer
  }
}


// vim: set ts=2 sw=2 et:
