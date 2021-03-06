package com.ataraxer.ottla

import com.ataraxer.zooowner.actor.ZooownerActor
import com.ataraxer.zooowner.{message => ZK}
import com.ataraxer.akkit.Spawner

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import org.json4s.NoTypeHints
import org.json4s.native.{Serialization => Json}

import scala.concurrent.duration._
import scala.collection.mutable


object KafkaMetaStorage {
  case object GetBrokerList
  case class BrokerList(brokers: Seq[Int])

  case class SaveTopicConfig(
    topic: String,
    config: Map[String, String])

  case class SaveTopicPartitionAssignment(
    topic: String,
    replicaAssignment: Map[Int, Seq[Int]],
    update: Boolean = false)

  case class GetTopicInfo(topic: String)
  case class GetPartitionState(topic: String, partition: Int)

  case class SaveOffset(
    consumer: String,
    topic: String,
    partition: String,
    offset: Long)
}


class KafkaMetaStorage(zk: ActorRef) extends Actor with Spawner {
  import Kafka._
  import KafkaMetaStorage._
  import KafkaJsonStructures._

  implicit val jsonFormatter = Json.formats(NoTypeHints)


  def receive = {
    case GetBrokerList => {
      val client = sender

      val handler = spawn.handler {
        case ZK.NodeChildren(_, children) => {
          client ! BrokerList(children.map(_.toInt).sorted)
        }
      }

      zk.tell(
        ZK.GetChildren(KafkaZK.BrokersDirectory),
        sender = handler)
    }


    case SaveTopicConfig(topic, config) => {
      val topicConfig = Admin.TopicConfiguration(
        version = 1,
        config = config)

      zk ! ZK.Set(KafkaZK.topicConfigPath(topic), Json.write(topicConfig))
    }


    case SaveTopicPartitionAssignment(topic, replicaAssignment, update) => {
      val zkPath = KafkaZK.topicPath(topic)
      val partitionData = TopicInfo(
        version = 1,
        partitions = replicaAssignment)

      val partitionDataJson = Json.write(partitionData)

      if (!update) {
        //info("Topic creation " + jsonPartitionData.toString)

        val client = sender
        val handler = spawn.handler {
          case ZK.NoNode => client ! KafkaAdminManager.TopicExists(topic)
        }

        zk.tell(ZK.Create(zkPath, Some(partitionDataJson)), handler)
      } else {
        //info("Topic update " + jsonPartitionData.toString)
        zk ! ZK.Set(zkPath, partitionDataJson)
      }

      /*
      debug(
        "Updated path %s with %s for replica assignment"
        .format(zkPath, jsonPartitionData))
      */
    }


    case GetTopicInfo(topic) => {
      val topicPath = KafkaZK.topicPath(topic)
      val client = sender

      val handler = spawn.handler {
        case ZK.NodeData(_, Some(zkTopicInfo)) => {
          val topicInfo = Json.read[TopicInfo](zkTopicInfo)
          client ! topicInfo
        }
      }

      zk.tell(
        ZK.Get(topicPath),
        sender = handler)
    }


    case GetPartitionState(topic, partitionId) => {
      val partition = Partition(topic, partitionId)
      val partitionPath = KafkaZK.partitionPath(partition)
      val client = sender

      val handler = spawn.handler {
        case ZK.NodeData(_, Some(zkPartitionState)) => {
          val partitionInfo = Json.read[PartitionState](zkPartitionState)
          client ! partitionInfo
        }
      }

      zk.tell(
        ZK.Get(partitionPath),
        sender = handler)
    }


    case SaveOffset(consumer, topic, partitionId, offset) => {
      val partition = Partition(topic, partitionId.toInt)
      val offsetPath = KafkaZK.offsetPath(consumer, partition)
      zk ! ZK.Set(offsetPath, offset.toString)
    }
  }
}


// vim: set ts=2 sw=2 et:
