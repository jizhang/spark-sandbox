import scala.util.Random
import org.slf4j.LoggerFactory
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed._


object CosineSimilarity {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CosineSimilarity").setMaster("local[2]")
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

    val ratings = logs.map { case (user, itemId, clicks) =>
      user -> (itemId, clicks)
    }.join(userIds).map { case (user, ((itemId, clicks), userId)) =>
      (userId, itemId, clicks.toDouble)
    }

    val numColumns = ratings.map(_._2).max + 1
    println(s"numColumns = $numColumns")

    // split
    val ratingSplits = ratings.randomSplit(Array.fill(10)(0.1), 913)

    val trainingSet = sc.union(ratingSplits.dropRight(1))
    val testingSet = ratingSplits.last

    // train
    val userComms = trainingSet.map { case (userId, commId, clicks) =>
      (userId, Seq(commId -> clicks))
    }.reduceByKey(_ ++ _)

    val rows = userComms.values.map { commClicks =>
      Vectors.sparse(numColumns, commClicks)
    }

    val mat = new RowMatrix(rows)
    val sim = mat.columnSimilarities(0.1)

    // recommend
    val entries = sim.entries.map { case MatrixEntry(i, j, u) =>
      i.toInt -> (j.toInt, u)
    }.groupByKey.mapValues { iter =>
      iter.toSeq.sortWith(_._2 > _._1)//.take(50)
    }

    val testUserId = trainingSet.first._1

    // TODO normalize, visited item

    trainingSet.map { case (userId, commId, clicks) =>
      commId -> (userId, clicks)
    }.filter(_._2._1 == testUserId).join(entries).flatMap { case (commId, ((userId, clicks), comms)) =>
      comms.map { case (commId, similarity) =>
        (userId, commId) -> (clicks, similarity)
      }
    }.groupByKey.map { case ((userId, commId), comms) =>
      val score = comms.map(t => t._1 * t._2).sum
      val total = comms.map(t => t._2).sum
      userId -> (commId, score / total)
    }.groupByKey.map { case (userId, comms) =>
      comms.toSeq.sortWith(_._2 > _._2).take(20)
    }.take(10).foreach(println)

    sc.stop()
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

  def testOne(userComms: RDD[(String, Seq[(Int, Double)])], colsim: CoordinateMatrix): Unit = {

    val sim = colsim.toIndexedRowMatrix

    sim.rows.take(10).foreach { row =>
      val items = (0 until row.vector.size).map(i => i -> row.vector(i)).filter(_._2 > 0).sortWith(_._2 > _._2).take(100)
      println(row.index + ": " + items.mkString(", "))
    }

    userComms.filter(_._1 == "000b63c9ea5a4b4840663f2ea5b59635").take(1).foreach { case (userId, comms) =>

      val l2norm = math.sqrt(comms.map(t => t._2 * t._2).sum)
      val l2normComms = comms.map(t => t._1 -> t._2 / l2norm)

      println(l2normComms)

      val commIds = l2normComms.map(_._1).toSet
      val matches = sim.rows.filter(row => commIds.contains(row.index.toInt))
        .map { row =>
          val items = (0 until row.vector.size).map(i => i -> row.vector(i))
            .filter { case (commId, similarity) => similarity > 0 && !commIds.contains(commId) }
          (row.index, items.toMap)
        }
        .collectAsMap()

      println(matches.mapValues(_.take(100)))

      val recommendations = l2normComms.flatMap { case (commId, clicks) =>
        matches.getOrElse(commId, Map.empty).mapValues(similarity => (similarity, clicks))
      }.groupBy(_._1).mapValues(_.map(_._2)).mapValues { items =>
        val score = items.map(item => item._1 * item._2).sum
        val total = items.map(_._1).sum
        (score / total, score, total)
      }.toSeq.sortWith(_._2._1 > _._2._1)

      println(recommendations.size)

      recommendations.take(100).foreach(println)

    }

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
