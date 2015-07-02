import org.slf4j.LoggerFactory
import org.apache.spark._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}


object CosineSimilarity {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CosineSimilarity").setMaster("local[2]")
    val sc = new SparkContext(conf)

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

    val rows = logs.map { case (userId, commId, clicks) =>
      (userId, Seq(commId -> clicks.toDouble))
    }.reduceByKey(_ ++ _).values.map { commClicks =>
      Vectors.sparse(numColumns, commClicks)
    }

    val mat = new RowMatrix(rows)
    val sim = mat.columnSimilarities(0.1)

    sim.entries.filter(entry => Set(253147, 251367).contains(entry.i.toInt))
      .map(entry => (entry.i, Seq(entry.j -> entry.value)))
      .reduceByKey(_ ++ _)
      .foreach { case (i, items) =>
        val itemList = items.sortWith(_._2 > _._2).take(5)
        println(i + ": " + itemList.mkString(", "))
      }

    sc.stop()
  }

}
