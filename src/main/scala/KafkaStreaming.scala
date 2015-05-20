import org.slf4j.LoggerFactory
import kafka.utils.ZkUtils
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils


object KafkaStreaming {

  val logger = LoggerFactory.getLogger(getClass)

  implicit class AugmentArray[T](val arr: Array[T]) {
    def getOrElse(index: Int, default: T): T = if (arr.length > index) {
      arr(index)
    } else {
      default
    }
  }

  def main(args: Array[String]): Unit = {

    val zkQuorum = args.getOrElse(0, "127.0.0.1:2181")
    val topic = args.getOrElse(1, "test_topic")
    val group = args.getOrElse(2, "test_group")
    val batchInterval = args.getOrElse(3, "3").toInt

    val conf = new SparkConf().setAppName("KafkaStreamingJob")
    val ssc = new StreamingContext(conf, Seconds(batchInterval))

    // reset
    logger.info("Reset consumer group: " + group)
    ZkUtils.maybeDeletePath(zkQuorum, s"/consumers/$group")

    // create stream
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, Map(topic -> 1)).map(_._2)
    lines.count.foreachRDD { rdd =>
      rdd.foreach { count =>
        logger.info(count.toString)
      }
    }

    ssc.start()
    logger.debug("Job started.")
    ssc.awaitTermination()
  }

}
