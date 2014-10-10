package com.ataraxer.ottla.client.protocol

import java.nio.ByteBuffer
import scala.language.implicitConversions


object KafkaProtocol {
  val Encoding = "UTF-8"


  implicit class CountLoop(count: Int) {
    /**
     * Calls provided code specified number of times,
     * and collects results into a list.
     */
    def times[T](code: => T): Seq[T] = {
      val result = for (_ <- 1 to count) yield code
      result.toSeq
    }
  }


  implicit class RichByteBuffer(buffer: ByteBuffer) {
    def getString: String = {
      // TODO: throw more informative exception on None
      getOptionalString.get
    }


    def putString(string: String): Unit = {
      putOptionalString(Some(string))
    }


    /**
     * Deserializes a short string wrapping it in Option to
     * prevent exposing possible `null` value.
     */
    def getOptionalString: Option[String] = {
      val size = buffer.getShort

      if (size > 0) {
        val bytes = new Array[Byte](size)
        buffer.get(bytes)
        Some(new String(bytes, Encoding))
      } else {
        None
      }
    }


    /**
     * Serializes an optinal string, writing None as null.
     */
    def putOptionalString(optionalString: Option[String]): Unit = {
      optionalString match {
        case Some(string) => {
          val stringBytes = string.getBytes(Encoding)
          val stringSize = stringBytes.size
          buffer.putShort(stringSize.toShort)
          buffer.put(stringBytes)
        }

        case None => {
          buffer.putShort(-1)
        }
      }
    }


    def getSeq[T](deserialize: ByteBuffer => T): Seq[T] = {
      val size = buffer.getInt
      size times deserialize(buffer)
    }
  }
}


// vim: set ts=2 sw=2 et:
