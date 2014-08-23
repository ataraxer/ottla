package com.ataraxer.ottla.kafka

import akka.actor._
import akka.actor.Actor.Receive
import akka.io.{ IO, Tcp }

import java.net.InetSocketAddress


class KafkaConnection(remote: InetSocketAddress, listener: ActorRef)
    extends Actor
{
  import Tcp._
  import context.system

  IO(Tcp) ! Connect(remote)


  def receive = {
    case CommandFailed(_: Connect) => // TODO

    case cmd @ Connected(remote, local) => {
      listener ! cmd
      val connection = sender
      connection ! Register(self)
      context.become(active(connection))
    }
  }


  def active(connection: ActorRef): Receive = {
    case Received(data) => {
      // process response from Kafka
      // and send back to requester
    }

    case writeRequest: Write => {
      connection ! writeRequest
    }

    case CommandFailed(_: Write) => // TODO
    case _: ConnectionClosed     => // TODO
  }
}


// vim: set ts=2 sw=2 et:
