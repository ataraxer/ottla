package com.ataraxer.ottla
package client
package protocol

import java.nio.ByteBuffer


class TopicMetadataRequest(
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
    topicListBytes.size
  }


  def getBytes: ByteBuffer = {
    val buffer = ByteBuffer.allocate(this.size)
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
