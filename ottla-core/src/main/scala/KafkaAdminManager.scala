package com.ataraxer.ottla

import com.ataraxer.zooowner.actor.ZooownerActor
import com.ataraxer.zooowner.{message => ZK}

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import kafka.admin.AdminUtils
import kafka.common.Topic
import kafka.log.LogConfig

import scala.concurrent.duration._
import scala.collection.JavaConversions._

import java.util.Properties

object KafkaAdminManager {
  case class CreateTopic(
    topic: String,
    partitions: Int,
    replication: Int,
    config: Properties = new Properties)

  case class TopicExists(topic: String)

  case class PersistReplicaAssignmentPath(
    topic: String,
    replicaAssignment: Map[Int, Seq[Int]],
    config: Properties = new Properties,
    update: Boolean = false)
}


class KafkaAdminManager(
  metaStorage: ActorRef,
  zk: ActorRef)
    extends Actor
{
  import KafkaAdminManager._
  import KafkaMetaStorage._

  implicit val timeout: Timeout = 5.seconds
  implicit val ex = context.dispatcher


  def receive = {
    case CreateTopic(topic, partitions, replication, config) => {
      val brokers = metaStorage ? GetBrokerList

      brokers foreach {
        case BrokerList(brokerList) => {
          val replicaAssignment = AdminUtils.assignReplicasToBrokers(
            brokerList, partitions, replication).toMap

          self.tell(
            PersistReplicaAssignmentPath(
              topic, replicaAssignment, config),
            sender = sender)
        }
      }
    }


    case PersistReplicaAssignmentPath(topic, replicaAssignment, config, update) => {
      Topic.validate(topic)
      LogConfig.validate(config)

      require(
        replicaAssignment.values.map(_.size).toSet.size == 1,
        "All partitions should have the same number of replicas.")

      val topicPath = KafkaZK.topicPath(topic)

      replicaAssignment.values.foreach { reps =>
        require(
          reps.size == reps.toSet.size,
          "Duplicate replica assignment found: " + replicaAssignment)
      }

      // write out the config if there is any
      // this isn't transactional with the partition assignments
      metaStorage ! SaveTopicConfig(topic, config.toMap)

      // create the partition assignment
      metaStorage ! SaveTopicPartitionAssignment(
        topic, replicaAssignment, update)
    }

  }
}



// vim: set ts=2 sw=2 et:
