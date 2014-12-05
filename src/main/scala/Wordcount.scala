import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Wordcount extends App {

  val conf = new SparkConf().setAppName("Wordcount")
  val sc = new SparkContext(conf)

  val input = args(0)
  val words = sc.textFile(input).flatMap(_.split(" "))
  val counts = words.map(_ -> 1).reduceByKey(_ + _)

  counts.foreach(println)

}
