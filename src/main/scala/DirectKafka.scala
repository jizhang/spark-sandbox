import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object DirectKafka {

  def main(args: Array[String]): Unit = {

    val Array(brokers, topics) = args
    val kafkaParams = Map("bootstrap.servers" -> brokers)
    val topicSet = topics.split(",").toSet

    val conf = new SparkConf().setAppName("DirectKafka").setIfMissing("spark.master", "local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))

    var offsetRanges = Array.empty[OffsetRange]
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
      .transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    messages.flatMap(_._2.split(" ")).map(_ -> 1).reduceByKey(_ + _).foreachRDD { (rdd, time) =>
      println("Time: " + time)
      offsetRanges.foreach(println)
      rdd.foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
