import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object LogMining extends App {

  val conf = new SparkConf().setAppName("LogMining")
  val sc = new SparkContext(conf)

  val inputFile = args(0)

  val logs = sc.textFile(inputFile)
  val errors = logs.filter(log => log.contains("ERROR"))
  errors.cache()

  // count errors
  println(errors.count())

  // count by apps
  val apps = errors map { error =>
    val fields = error.split("\t")
    (fields(2), 1)
  }
  apps.reduceByKey(_ + _).foreach(println)

}
