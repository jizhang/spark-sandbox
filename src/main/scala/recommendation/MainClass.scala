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
    similarityMethod: String = "",
    recommendMethod: String = "",
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
      opt[String]('s', "similarity-method") action { (x, c) =>
        c.copy(similarityMethod = x) } text("similarity method")
      opt[String]('r', "recommend-method") action { (x, c) =>
        c.copy(recommendMethod = x) } text("recommend method")
      opt[Int]('n', "num-recommendations") action { (x, c) =>
        c.copy(numRecommendations = x) } text ("number of recommendations")
      help("help") text ("prints this usage text")
    }

    val config = parser.parse(args, Config()) match {
      case Some(config) => logger.info("Parameters: " + config); config
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

    logger.info(s"Got $numRatings ratings from $numUsers users on $numProducts products.")

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
        val params = commonParams ++ Map(
          "numColumns" -> numColumns,
          "similarityMethod" -> config.similarityMethod,
          "recommendMethod" -> config.recommendMethod
        )
        SimilarityRecommender.recommend(trainingSet, params)

      case "als" =>
        val params = commonParams ++ Map(
          "recommendMethod" -> config.recommendMethod
        )
        AlsRecommender.recommend(trainingSet, params)

      case _ => throw new IllegalArgumentException("Unkown algorithm " + config.algorithm)
    }

    userRecomm.cache()

    val (precision, recall, f1) = evaluatePrecision(testingSet, userRecomm)
    val coverage = evaluateCoverage(trainingSet, userRecomm)
    val popularity = evaluatePopularity(trainingSet, userRecomm)

    logger.info("Precision=%.2f%% Recall=%.2f%% F1=%.4f Coverage=%.2f%% Popularity=%.4f".format(
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

}
