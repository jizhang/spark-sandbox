import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Wordcount extends App {

  val conf = new SparkConf().setAppName("Wordcount")
  val sc = new SparkContext(conf)

  val inputFile = args(0)

  sc.textFile(inputFile)
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey((a, b) => a + b)
    .foreach(word => println(word))

//  sc.textFile(inputFile)
//    .flatMap(_.split(" "))
//    .map(_ -> 1)
//    .reduceByKey(_ + _)
//    .foreach(println)
}
