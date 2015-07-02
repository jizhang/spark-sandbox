import org.slf4j.LoggerFactory
import org.apache.spark._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}


object CosineSimilarity {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CosineSimilarity").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // read logs
    val logs = sc.textFile("/Users/jizhang/data/comm.txt").flatMap { line =>
      try {
        val arr = line.split("\\s+")
        Some(arr(0), arr(1).toInt, arr(2).toInt)
      } catch {
        case _: Exception => None
      }
    }.cache()

    val numColumns = logs.map(_._2).max + 1
    println(s"numColumns = $numColumns")

    val userComms = logs.map { case (userId, commId, clicks) =>
      (userId, Seq(commId -> clicks.toDouble))
    }.reduceByKey(_ ++ _).cache()

    val rows = userComms.values.map { commClicks =>
      Vectors.sparse(numColumns, commClicks)
    }

    val mat = new RowMatrix(rows)
    val sim = mat.columnSimilarities(0.1).toIndexedRowMatrix

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

    sc.stop()
  }

}
