package com.ataraxer.ottla


object KafkaJsonStructures {
  case class TopicInfo(
    version: Int,
    partitions: List[Int])

  case class PartitionState(
    version: Int,
    isr: List[Int],
    leader: Int,
    controllerEpoch: Int,
    leaderEpoch: Int)

  case class BrokerRegistrationInfo(
    version: Int,
    host: String,
    port: Int,
    jmxPort: Int)

  case class ConsumerRegistrationInfo(
    version: Int,
    pattern: String,
    subscription: Map[String, Int])


  object Admin {
    case class PartitionInfo(
      topic: String,
      partition: Int,
      replicas: Option[List[Int]])

    case class ReassignPartitions(
      version: Int,
      partitions: List[PartitionInfo])

    case class PreferredReplicaElection(
      version: Int,
      partitions: List[PartitionInfo])

    case class DeleteTopics(
      version: Int,
      topics: List[String])

    case class TopicConfiguration(
      version: Int,
      config: Map[String, String])
  }

}


// vim: set ts=2 sw=2 et:
