package com.pactera.poc

import java.util.Properties

import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * 用Flink消费kafka
  */
object ReadingFromKafka {

  private val ZOOKEEPER_HOST = "mini1:2181,mini2:2181,mini3:2181"
  private val KAFKA_BROKER = "kafka01:9092,kafka02:9092,kafka03:9092"
  /*private val ZOOKEEPER_HOST = "cdh001:2181,cdh002:2181,cdh003:2181"
  private val KAFKA_BROKER = "cdh001:9092,cdh002:9092,cdh003:9092"*/
  private val TRANSACTION_GROUP = "com.poc.flink"

  def main(args : Array[String]){
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // configure Kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    kafkaProps.setProperty("auto.offset.reset","earliest");

    //topicd的名字是new，schema默认使用SimpleStringSchema()即可
    val transaction = env
      .addSource(
        new FlinkKafkaConsumer010[String]("poctopic", new SimpleStringSchema(), kafkaProps)
      ).map(new OperationMap())
    /*transaction.print()*/
    /*transaction.writeAsText("file:///root/flinklog")*/

    env.execute("IteblogFlinkKafkaStreaming")

  }

}
