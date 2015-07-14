package recommendation

import scala.util.Random
import org.slf4j.LoggerFactory
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.recommendation.Rating


object MainClass {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Recommendation").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // read logs - logs are pre-grouped by user-item
    val logs = sc.textFile("/Users/jizhang/data/comm.txt").flatMap { line =>
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
    }.persist(StorageLevel.DISK_ONLY)

    val numColumns = ratings.map(_.product).max + 1
    println(s"numColumns = $numColumns")

    // split
    val ratingSplits = ratings.randomSplit(Array.fill(10)(0.1), 913)

    val trainingSet = sc.union(ratingSplits.dropRight(1))
    val testingSet = ratingSplits.last.persist(StorageLevel.DISK_ONLY)

    // model
    val userRecomm = args(0) match {
      case "cf" =>
        SimilarityRecommender.recommend(trainingSet, Map("numColumns" -> numColumns))

      case "als" =>
        AlsRecommender.recommend(trainingSet, Map.empty)

      case _ => throw new IllegalArgumentException
    }

    println("Recall = %.4f%%".format(recall(testingSet, userRecomm) * 100))

    sc.stop()
  }

  def recall(testingSet: RDD[Rating], userRecomm: RDD[(Int, Seq[Rating])]): Double = {

    val userTest = testingSet.map { rating =>
      rating.user -> rating
    }.groupByKey

//    println(userTest.keys.count)
//    println(userRecomm.keys.count)
//    println(userTest.join(userRecomm).keys.count)

    val (tp, total) = userTest.join(userRecomm).values.map { case (tests, recomms) =>
      val t = tests.map(_.product).toSet
      val r = recomms.map(_.product).toSet
      ((t & r).size, t.size)
    }.reduce { (t1, t2) =>
      ((t1._1 + t2._1), (t1._2 + t2._2))
    }

    tp.toDouble / total
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
