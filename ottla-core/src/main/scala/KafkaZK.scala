package com.ataraxer.ottla


object KafkaZK {
  import Kafka._

  implicit class PathBuilder(string: String) {
    def / (subpath: String) =
      string + "/" + subpath

    def / (subpath: Int) =
      string + "/" + subpath.toString
  }


  val BrokersDirectory = "/brokers/ids"
  val TopicsDirectory = "/brokers/topics"
  val TopicsConfigsDirectpry = "/config/topics"
  val ConsumersDirectory = "/consumers"


  def topicPath(topic: String) =
    TopicsDirectory/topic

  def partitionPath(partition: Partition) =
    topicPath(partition.topic)/"partitions"/partition.id

  def topicConfigPath(topic: String) =
    TopicsConfigsDirectpry/topic

  def brokerPath(id: String) =
    BrokersDirectory/id

  def offsetPath(consumerId: String, partition: Partition) =
    ConsumersDirectory/consumerId/"offsets"/partition.topic/partition.id
}


// vim: set ts=2 sw=2 et:
