import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

object DirectKafka {

  def main(args: Array[String]): Unit = {

    val Array(brokers, topics) = args
    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> brokers,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "spark-sandbox",
        "auto.offset.reset" -> "latest")
    val topicSet = topics.split(",").toSet

    val conf = new SparkConf().setAppName("DirectKafka").setIfMissing("spark.master", "local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))

    var offsetRanges = Array.empty[OffsetRange]
    val messages = KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams))
      .transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    messages.flatMap(_.value.split(" ")).map(_ -> 1).reduceByKey(_ + _).foreachRDD { (rdd, time) =>
      println("Time: " + time)
      offsetRanges.foreach(println)
      rdd.foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
