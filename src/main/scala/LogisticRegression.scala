import java.util.Random
import scala.math.exp
import breeze.linalg.{Vector, DenseVector}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object LogisticRegression {

  case class DataPoint(x: Vector[Double], y: Double)

  def parsePoint(line: String, d: Int): DataPoint = {
    val data = line.split(" ").take(d + 1).map(_.toDouble)
    DataPoint(new DenseVector(data.drop(1)), data(0))
  }

  def main(args: Array[String]) {

    if (args.length < 3) {
      println("Usage: LogisticRegression <file> <dimensions> <iterations>")
      System.exit(1)
    }

    val inputPath = args(0)
    val dimensions = args(1).toInt
    val iterations = args(2).toInt

    val sparkConf = new SparkConf().setAppName("LogisticRegression")
    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile(inputPath)
    val points = lines.map(parsePoint(_, dimensions)).cache

    val rand = new Random(42)
    var w = DenseVector.fill(dimensions) {
      2 * rand.nextDouble - 1
    }
    println("Initial w: " + w)

    for (i <- 1 to iterations) {
      println("On iteration " + i)
      val gradient = points map { p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      } reduce(_ + _)
      w -= gradient
    }

    println("Final w: " + w)
    sc.stop
  }

}
