package com.ataraxer.ottla
package client
package protocol

import java.nio.ByteBuffer


object KafkaMessage {
  def apply(buffer: ByteBuffer) = {

  }
}


trait KafkaMessage {
  /**
   * Message size in bytes.
   */
  def size: Int

  /**
   * Message representation as a buffer of bytes.
   */
  def getBytes: ByteBuffer
}


// vim: set ts=2 sw=2 et:
