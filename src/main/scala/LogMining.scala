import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object LogMining extends App {

  val conf = new SparkConf().setAppName("LogMining")
  val sc = new SparkContext(conf)

  val inputFile = args(0)

  val lines = sc.textFile(inputFile)

  // parse logs
  val logs = lines.map(_.split("\t"))
  val errors = logs.filter(_(1) == "ERROR")
  errors.cache()

  // count error
  println(errors.count())

  // mysql errors
  val mysqlErrors = errors.filter(_(2) == "MySQL")
  mysqlErrors.take(10).map(_ mkString "\t").foreach(println)

  // count error by app
  val errorApps = errors.map(_(2) -> 1)
  errorApps.countByKey().foreach(println)

}
