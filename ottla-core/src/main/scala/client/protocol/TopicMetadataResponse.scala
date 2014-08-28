package com.ataraxer.ottla
package client
package protocol

import java.nio.ByteBuffer


object TopicMetadataResponse {
  import KafkaProtocol._


  object Broker {
    def readFrom(buffer: ByteBuffer) = {
      val node = buffer.getInt
      val host = buffer.getString
      val port = buffer.getInt
      Broker(node, host, port)
    }
  }


  case class Broker(node: Int, host: String, port: Int)


  object TopicMetadata {
    def readFrom(buffer: ByteBuffer) = {
      val errorCode = buffer.getShort

      val topic = buffer.getString

      val partitionsMetadata = buffer.getSeq(
        PartitionMetadata.readFrom _)

      TopicMetadata(topic, partitionsMetadata, errorCode)
    }
  }


  case class TopicMetadata(
    topic: String,
    partitionsMetadata: Seq[PartitionMetadata],
    errorCode: Short)


  object PartitionMetadata {
    def readFrom(buffer: ByteBuffer) = {
      val errorCode = buffer.getShort

      val partitionId = buffer.getInt
      val leaderId = buffer.getInt

      val replicaIds = buffer.getSeq(_.getInt)

      val inSyncReplicaIds = buffer.getSeq(_.getInt)

      PartitionMetadata(
        partitionId,
        Some(leaderId),
        replicaIds,
        inSyncReplicaIds,
        errorCode)
    }
  }


  case class PartitionMetadata(
    partitionId: Int,
    leader: Option[Int],
    replicas: Seq[Int],
    isr: Seq[Int] = Seq.empty,
    errorCode: Short)


  def apply(buffer: ByteBuffer): TopicMetadataResponse = {
    val size = buffer.getInt

    val correlationId = buffer.getInt

    val brokers = buffer.getSeq(
      Broker.readFrom _)

    val brokerMap = brokers.map(b => (b.node, b)).toMap

    val topicsMetadata = buffer.getSeq(
      TopicMetadata.readFrom _)

    TopicMetadataResponse(topicsMetadata, correlationId)
  }
}


case class TopicMetadataResponse(
    topicMetadata: Seq[TopicMetadataResponse.TopicMetadata],
    correlationId: Int)


// vim: set ts=2 sw=2 et:
