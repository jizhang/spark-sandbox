import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object StreamingWordcount extends App {

  val conf = new SparkConf().setAppName("Wordcount")
  val ssc = new StreamingContext(conf, Seconds(3))

  ssc.socketTextStream("localhost", 9999)
     .flatMap(_.split(" "))
     .map(_ -> 1)
     .reduceByKey(_ + _)
     .print

  ssc.start()
  ssc.awaitTermination()
}
