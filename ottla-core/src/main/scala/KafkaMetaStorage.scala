package com.ataraxer.ottla

import com.ataraxer.zooowner.actor.ZooownerActor
import com.ataraxer.zooowner.{message => ZK}

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import org.json4s.NoTypeHints
import org.json4s.native.{Serialization => Json}

import kafka.utils.ZkUtils

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

  case class GetTopicPartitionAssignment(topic: String)
}


class KafkaMetaStorage(zk: ActorRef) extends Actor {
  import KafkaMetaStorage._
  import KafkaJsonStructures._

  implicit val timeout: Timeout = 5.seconds
  implicit val ex = context.dispatcher

  implicit val jsonFormatter = Json.formats(NoTypeHints)


  def receive = {
    case GetBrokerList => {
      val brokerList = zk ? ZK.GetChildren(ZkUtils.BrokerIdsPath)
      val result = brokerList map {
        case ZK.NodeChildren(_, children) => BrokerList {
          children.map(_.toInt).sorted
        }
      }
      result pipeTo sender
    }


    case SaveTopicConfig(topic, config) => {
      val topicConfig = Admin.TopicConfiguration(
        version = 1,
        config = config)

      zk ! ZK.Set(ZkUtils.getTopicConfigPath(topic), Json.write(topicConfig))
    }


    case SaveTopicPartitionAssignment(topic, replicaAssignment, update) => {
      val zkPath = ZkUtils.getTopicPath(topic)
      val partitionData = TopicInfo(
        version = 1,
        partitions = replicaAssignment)

      if (!update) {
        //info("Topic creation " + jsonPartitionData.toString)
        zk ! ZK.Create(zkPath, Some(Json.write(partitionData)))
      } else {
        //info("Topic update " + jsonPartitionData.toString)
        zk ! ZK.Set(zkPath, Json.write(partitionData))
      }
      /*
      try {
        debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionData))
      } catch {
        case e: ZkNodeExistsException => throw new TopicExistsException("topic %s already exists".format(topic))
        case e2: Throwable => throw new AdminOperationException(e2.toString)
      }
      */
    }


    case GetTopicPartitionAssignment(topic) => {
      val topicPath = ZkUtils.getTopicPath(topic)
      val futurePartitionMap = zk ? ZK.Get(topicPath)

      val result = futurePartitionMap map {
        case ZK.NodeData(_, Some(zkPartitionMap)) => {
          val topicInfo = Json.read[TopicInfo](zkPartitionMap)
          topicInfo.partitions
        }

        case _ => Map.empty[Int, Seq[Int]]
      }

      result pipeTo sender

      //debug(
        //"Partition map for /brokers/topics/%s is %s"
        //.format(topic, partitionMap))
    }
  }
}


// vim: set ts=2 sw=2 et:
