package com.ataraxer.ottla
package client

import akka.actor._
import akka.actor.Actor.Receive
import akka.io.{ IO, Tcp }
import akka.util.ByteString

import java.net.InetSocketAddress
import java.nio.ByteBuffer



class KafkaConnection(remote: InetSocketAddress, listener: ActorRef)
    extends Actor
{
  import Tcp._
  import context.system

  IO(Tcp) ! Connect(remote)


  def receive = {
    case cmd @ Connected(remote, local) => {
      listener ! cmd
      val connection = sender
      connection ! Register(self)
      context.become(active(connection))
    }

    case CommandFailed(_: Connect) => // TODO
  }


  def active(connection: ActorRef): Receive = {
    case Received(data) => {
      // process response from Kafka
      // and send back to requester
      listener ! data
    }

    case request: Write => {
      connection ! request
    }

    case CommandFailed(_: Write) => // TODO
    case c: ConnectionClosed     => // TODO
  }
}


// vim: set ts=2 sw=2 et:
