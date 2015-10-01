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

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    messages.map(_._2).flatMap(_.split(" ")).map(_ -> 1).reduceByKey(_ + _).print

    ssc.start()
    ssc.awaitTermination()
  }

}
