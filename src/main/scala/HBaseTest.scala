import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object HBaseTest extends App {

  val conf = new SparkConf().setAppName("HBaseTest")
  val sc = new SparkContext(conf)

  sc.parallelize(Seq(0)).mapPartitions{ iter =>
    iter.map { i =>
      HBaseClient.getValue()
    }
  }.collect.foreach(println)

}
