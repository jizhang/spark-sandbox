import scala.util.Random
import org.slf4j.LoggerFactory
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
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
    }.persist(StorageLevel.DISK_ONLY)

    val numColumns = ratings.map(_._2).max + 1
    println(s"numColumns = $numColumns")

    // split
    val ratingSplits = ratings.randomSplit(Array.fill(10)(0.1), 913)

    val trainingSet = sc.union(ratingSplits.dropRight(1))
    val testingSet = ratingSplits.last

    // train
    val userComms = trainingSet.map { case (userId, commId, clicks) =>
      (userId, (commId, clicks))
    }.groupByKey

    val rows = userComms.values.map { commClicks =>
      Vectors.sparse(numColumns, commClicks.toSeq)
    }

    val mat = new RowMatrix(rows)
    val sim = mat.columnSimilarities(0.1)

    // recommend
    val simTop = sim.entries.map { case MatrixEntry(i, j, u) =>
      i.toInt -> (j.toInt, u)
    }.groupByKey.mapValues { comms =>
      val commsTop = comms.toSeq.sortWith(_._2 > _._1).take(50)
      normalizeOne(commsTop)
    }

    val t1 = simTop.flatMap { case (i, items) =>
      items.map { case (j, u) =>
        j -> (i, u)
      }
    }

    val t2 = userComms.flatMap { case (userId, comms) =>
      normalizeOne(comms).map { case (commId, clicks) =>
        commId -> (userId, clicks)
      }
    }

    val userRecomm = t1.join(t2).map { case (commId, ((i, u), (userId, clicks))) =>
      userId -> (commId, i, u, clicks)
    }.groupByKey.mapValues { comms =>

      val visited = comms.map(_._1).toSet
      val newItems = comms.filterNot(t => visited(t._2)).map(t => t._2 -> (t._3, t._4))

      newItems.groupBy(_._1).mapValues(_.map(_._2)).map { case (commId, comms) =>
        val score = comms.map(t => t._1 * t._2).sum
        val total = comms.map(_._1).sum
        commId -> score / total
      }.toSeq.sortWith(_._2 > _._2).take(30)

    }

    val testUserId = trainingSet.first._1
    userRecomm.filter(_._1 == testUserId).foreach(println)

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

  def normalizeOne(items: Iterable[(Int, Double)]): Iterable[(Int, Double)] = {
    val l2norm = math.sqrt(items.map(_._2).sum)
    items.map(t => t._1 -> t._2 / l2norm)
  }

}
