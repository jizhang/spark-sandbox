package recommendation

import scala.util.Random
import org.slf4j.LoggerFactory
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.recommendation.Rating


case class Config(
    inputPath: String = "",
    algorithm: String = "",
    numRecommendations: Int = 30)

object MainClass {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[Config]("recommendation") {
      head("recommendation", "1.0")
      opt[String]('i', "input-path") required() action { (x, c) =>
        c.copy(inputPath = x) } text("input path")
      opt[String]('a', "algorithm") required() action { (x, c) =>
        c.copy(algorithm = x) } text("algorithm name")
      opt[Int]('n', "num-recommendations") action { (x, c) =>
        c.copy(numRecommendations = x) } text ("number of recommendations")
      help("help") text ("prints this usage text")
    }

    val config = parser.parse(args, Config()) match {
      case Some(config) => println(config); config
      case None => return
    }

    val conf = new SparkConf().setAppName("Recommendation").setIfMissing("spark.master", "local[4]")
    val sc = new SparkContext(conf)

    // read logs - logs are pre-grouped by user-item
    val logs = sc.textFile(config.inputPath).flatMap { line =>
      try {
        val arr = line.split("\\s+")
        Some(arr(0), arr(1).toInt, arr(2).toInt)
      } catch {
        case _: Exception => None
      }
    }

    // generate integer user id
    val userIds = logs.map(_._1).distinct.zipWithUniqueId

    val ratings = logs.map { case (userName, itemId, clicks) =>
      userName -> (itemId, clicks)
    }.join(userIds).map { case (userName, ((itemId, clicks), userId)) =>
      Rating(userId.toInt, itemId, clicks.toDouble)
    }.cache()

    val numRatings = ratings.count
    val numUsers = ratings.map(_.user).distinct.count
    val numProducts = ratings.map(_.product).distinct.count

    println(s"Got $numRatings ratings from $numUsers users on $numProducts products.")

    // split
    val ratingSplits = ratings.randomSplit(Array.fill(10)(0.1), 913)

    val trainingSet = sc.union(ratingSplits.dropRight(1)).cache()
    val testingSet = ratingSplits.last.cache()

    val commonParams = Map(
      "numNeighbours" -> 50,
      "numRecommendations" -> config.numRecommendations
    )

    // model
    val userRecomm = config.algorithm match {
      case "sim" =>
        val numColumns = ratings.map(_.product).max + 1
        SimilarityRecommender.recommend(trainingSet, commonParams ++ Map("numColumns" -> numColumns))

      case "als" =>
        AlsRecommender.recommend(trainingSet, commonParams)

      case _ => throw new IllegalArgumentException("Unkown algorithm " + config.algorithm)
    }

    userRecomm.cache()

    val (precision, recall, f1) = evaluatePrecision(testingSet, userRecomm)
    val coverage = evaluateCoverage(trainingSet, userRecomm)
    val popularity = evaluatePopularity(trainingSet, userRecomm)

    println("Precision=%.4f%% Recall=%.4f%% F1=%.6f Coverage=%.4f%% Popularity=%.6f".format(
        precision * 100, recall * 100, f1, coverage * 100, popularity))

    sc.stop()
  }

  def evaluatePrecision(testingSet: RDD[Rating], userRecomm: RDD[(Int, Seq[Rating])]
      ): (Double, Double, Double) = {

    val userTest = testingSet.map { rating =>
      rating.user -> rating
    }.groupByKey

//    println(userTest.keys.count)
//    println(userRecomm.keys.count)
//    println(userTest.join(userRecomm).keys.count)

    val (tp, tpFn, tpFp) = userTest.join(userRecomm).values.map { case (tests, recomms) =>
      val t = tests.map(_.product).toSet
      val r = recomms.map(_.product).toSet
      ((t & r).size, t.size, r.size)
    }.reduce { (t1, t2) =>
      ((t1._1 + t2._1), (t1._2 + t2._2), (t1._3 + t2._3))
    }

    val precision = tp.toDouble / tpFp
    val recall = tp.toDouble / tpFn
    val f1 = 2 * precision * recall / (precision + recall)

    (precision, recall, f1)
  }

  def evaluateCoverage(trainingSet: RDD[Rating], userRecomm: RDD[(Int, Seq[Rating])]): Double = {
    val total = trainingSet.map(_.product).distinct.count
    val recomm = userRecomm.values.flatMap(_.map(_.product)).distinct.count
    recomm.toDouble / total
  }

  def evaluatePopularity(trainingSet: RDD[Rating], userRecomm: RDD[(Int, Seq[Rating])]): Double = {

    val itemCounts = trainingSet.map(_.product -> 1).reduceByKey(_ + _)
    val itemRecomm = userRecomm.values.flatMap(_.map(_.product -> 1))

    val (ret, n) = itemCounts.join(itemRecomm).values.map { case (totalCount, count) =>
      (math.log(1 + totalCount), count)
    }.reduce { (t1, t2) =>
      (t1._1 + t2._1, t1._2 + t2._2)
    }

    ret.toDouble / n
  }

  def compare(mat1: CoordinateMatrix, mat2: CoordinateMatrix): Unit = {

    val mat1Entries = mat1.entries.map { case MatrixEntry(i, j, u) => ((i, j), u) }
    val mat2Entries = mat2.entries.map { case MatrixEntry(i, j, u) => ((i, j), u) }
    val MAE = mat1Entries.leftOuterJoin(mat2Entries).values.map {
      case (u, Some(v)) => math.abs(u - v)
      case (u, None) => math.abs(u)
    }.mean()

    println(s"MAE = $MAE")

  }

  def calcSim(mat: RowMatrix, sc: SparkContext): CoordinateMatrix = {

    val sums = mat.rows.flatMap { v =>
      v.toSparse.indices.map { i =>
        i -> math.pow(v(i), 2)
      }
    }.reduceByKey(_ + _).mapValues(math.sqrt).collectAsMap

    println("sums size " + sums.size)

    val pairs = mat.rows.flatMap { v =>
      val indices = v.toSparse.indices
      indices.flatMap { i =>
        indices.filter(_ > i).map { j =>
          (i, j) -> v(i) * v(j)
        }
      }
    }

    val bcSums = sc.broadcast(sums)
    val entries = pairs.reduceByKey(_ + _).flatMap { case ((i, j), s) =>
      val value = s / bcSums.value(i) / bcSums.value(j)
      Seq(MatrixEntry(i, j, value), MatrixEntry(j, i, value))
    }

    new CoordinateMatrix(entries)
  }

}
