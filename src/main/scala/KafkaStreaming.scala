package com.anjuke.dm

import org.slf4j.LoggerFactory
import kafka.utils.ZkUtils
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils


object KafkaStreamingJob {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KafkaStreamingJob")
    val ssc = new StreamingContext(conf, Seconds(3))

    val zkQuorum = "127.0.0.1:2181"
    val topic = "test_topic"
    val group = "test_group"

    // reset
    logger.info("Reset consumer group: " + group)
    ZkUtils.maybeDeletePath(zkQuorum, s"/consumers/$group")

    // create stream
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, Map(topic -> 1)).map(_._2)
    lines.count.foreachRDD { rdd =>
      rdd.foreach(println)
    }

    ssc.start()
    logger.debug("Job started.")
    ssc.awaitTermination()
  }

}
