package com.ataraxer.ottla
package client
package protocol

import java.nio.ByteBuffer


object OffsetResponse {
  import KafkaProtocol._


  def apply(buffer: ByteBuffer): OffsetResponse = {
    val size = buffer.getInt

    val correlationId = buffer.getInt

    val result = buffer.getSeq { buffer =>
      val topic = buffer.getString
      val partitionOffsets = buffer.getSeq { buffer =>
        val partition = buffer.getInt
        val errorCode = buffer.getShort
        val offsets = {
          if (errorCode == 0) {
            Offsets(buffer.getSeq(_.getLong))
          } else {
            OffsetRequestError(errorCode)
          }
        }
        (partition, offsets)
      }
      (topic, partitionOffsets.toMap)
    }

    OffsetResponse(result.toMap, correlationId)
  }
}


trait PartitionOffsets
case class OffsetRequestError(errorCode: Int) extends PartitionOffsets
case class Offsets(offsets: Seq[Long]) extends PartitionOffsets


case class OffsetResponse(
    offsets: Map[String, Map[Int, PartitionOffsets]],
    correlationId: Int)


// vim: set ts=2 sw=2 et:
