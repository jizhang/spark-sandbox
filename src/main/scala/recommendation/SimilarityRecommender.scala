package recommendation

import scala.collection.mutable
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.recommendation.Rating


object SimilarityRecommender extends Recommender {

  val logger = LoggerFactory.getLogger(getClass)

  override def recommend(trainingSet: RDD[Rating], params: Map[String, Any]): RDD[(Int, Seq[Rating])] = {

    val numNeighbours = params.getInt("numNeighbours")
    val numRecommendations = params.getInt("numRecommendations")
    val similarityMethod = params.getString("similarityMethod")
    val recommendMethod = params.getString("recommendMethod")

    // train
    val entries = trainingSet.map { case Rating(user, product, rating) =>
      MatrixEntry(user, product, rating)
    }

    val coorMat = new CoordinateMatrix(entries)
    val mat = coorMat.toRowMatrix

    val sim = similarityMethod match {
      case "spark" => mat.columnSimilarities()
      case "spark-appr" => mat.columnSimilarities(0.1)
      case "cosine" => cosineSimilarity(mat)
      case "cosine-mod" => modifiedCosineSimilarity(mat)
      case "llr" => llr(mat)
      case "jaccard" => jaccard(mat)
      case "euclidean" => euclidean(coorMat)
      case _ => throw new IllegalArgumentException("unknown similarity method")
    }

    // recommend
    val simTop = sim.entries.map { case MatrixEntry(i, j, u) =>
      i.toInt -> (j.toInt, u)
    }.groupByKey.mapValues { products =>
      val productsTop = products.toSeq.sortWith(_._2 > _._2).take(numNeighbours)
      normalizeRange(productsTop)
    }

    recommendMethod match {
      case "score" => recommendByScore(simTop, trainingSet, numRecommendations)
      case "weighted-sum" => recommendByWeightedSum(simTop, trainingSet, numRecommendations)
      case _ => throw new IllegalArgumentException("unknown recommend method")
    }

  }

  def recommendByWeightedSum(simTop: RDD[(Int, Iterable[(Int, Double)])], trainingSet: RDD[Rating],
      numRecommendations: Int): RDD[(Int, Seq[Rating])] = {

    trainingSet.map { case Rating(user, product, rating) =>
      user -> (product, rating)
    }.groupByKey.flatMap { case (user, products) =>
      normalizeRange(products).map { case (product, rating) =>
        product -> (user, rating)
      }
    }.join(simTop).flatMap { case (product, ((user, rating), products)) =>
      products.map { case (j, u) =>
        user -> (product, j, u, rating)
      }
    }.groupByKey.map { case (user, products) =>

      val visited = products.map(_._1).toSet
      val newProducts = products.filterNot(t => visited(t._2)).map(t => t._2 -> (t._3, t._4))

      val topProducts = newProducts.groupBy(_._1).mapValues(_.map(_._2)).map { case (product, products) =>

        val total = products.map(_._1).sum
        val rating = if (total != 0) {
          val score = products.map(t => t._1 * t._2).sum
          score / total
        } else 0.0

        Rating(user, product, rating)

      }.toSeq.sortWith(_.rating > _.rating).take(numRecommendations)

      user -> topProducts
    }

  }

  def recommendByScore(simTop: RDD[(Int, Iterable[(Int, Double)])], trainingSet: RDD[Rating],
      numRecommendations: Int): RDD[(Int, Seq[Rating])] = {

    trainingSet.map { case Rating(user, product, rating) =>
      product -> user
    }.join(simTop).flatMap { case (product, (user, products)) =>
      products.map { case (j, u) =>
        user -> (product, j, u)
      }
    }.groupByKey.map { case (user, products) =>
      val visited = products.map(_._1).toSet
      val newProducts = products.filterNot(t => visited(t._2)).map { case (_, product, rating) =>
        Rating(user, product, rating)
      }
      val topProducts = newProducts.toSeq.sortWith(_.rating > _.rating).take(numRecommendations)
      user -> topProducts
    }

  }

  def normalizeLp(products: Iterable[(Int, Double)], p: Int): Iterable[(Int, Double)] = {
    val lpnorm = math.pow(products.map(t => math.pow(t._2, p)).sum, 1.0 / p)
    products.map(t => t._1 -> t._2 / lpnorm)
  }

  def normalizeRange(products: Iterable[(Int, Double)]): Iterable[(Int, Double)] = {
    val min = products.map(_._2).min
    val max = products.map(_._2).max
    if (min == max) {
      products.map(t => t._1 -> 0.5)
    } else {
      products.map(t => t._1 -> (t._2 - min) / (max - min))
    }
  }

  def cosineSimilarityInternal(mat: RowMatrix, f: (Double, Double, Double) => Double): CoordinateMatrix = {

    val sums = mat.rows.flatMap { v =>
      v.toSparse.indices.map { i =>
        i -> math.pow(v(i), 2)
      }
    }.reduceByKey(_ + _)

    val pairs = mat.rows.flatMap { v =>
      val indices = v.toSparse.indices
      indices.flatMap { i =>
        indices.filter(_ > i).map { j =>
          (i, j) -> v(i) * v(j)
        }
      }
    }.reduceByKey(_ + _)

    val entries = pairs.map { case ((i, j), v) =>
      i -> (j, v)
    }.join(sums).map { case (i, ((j, v), si)) =>
      j -> (i, v, si)
    }.join(sums).flatMap { case (j, ((i, v, si), sj)) =>
      val value = f(v, si, sj)
      Seq(MatrixEntry(i, j, value), MatrixEntry(j, i, value))
    }

    new CoordinateMatrix(entries)
  }

  def cosineSimilarity(mat: RowMatrix): CoordinateMatrix = {
    cosineSimilarityInternal(mat, (v, si, sj) => v / math.sqrt(si) / math.sqrt(sj))
  }

  def modifiedCosineSimilarity(mat: RowMatrix): CoordinateMatrix = {
    cosineSimilarityInternal(mat, (v, si, sj) => v / math.pow(si, 0.6) / math.pow(sj, 0.4))
  }

  def llr(mat: RowMatrix): CoordinateMatrix = {

    mat.rows.cache()
    val numRows = mat.numRows

    val pairs = mat.rows.flatMap { v =>
      val indices = v.toSparse.indices
      indices.flatMap { i =>
        indices.filter(_ > i).map { j =>
          (i, j) -> 1
        }
      }
    }.reduceByKey(_ + _)

    val items = mat.rows.flatMap { v =>
      val indices = v.toSparse.indices
      indices.map { i =>
        i -> 1
      }
    }.reduceByKey(_ + _).cache()

    val entries = pairs.map { case ((i, j), v) =>
      i -> (j, v)
    }.join(items).map { case (i, ((j, v), si)) =>
      j -> (i, v, si)
    }.join(items).flatMap { case (j, ((i, v, si), sj)) =>
      val k11 = v
      val k12 = sj - v
      val k21 = si - v
      val k22 = numRows - si - sj + v
      val value = logLikelihoodRatio(k11, k12, k21, k22)
      Seq(MatrixEntry(i, j, value), MatrixEntry(j, i, value))
    }

    new CoordinateMatrix(entries)
  }

  def logLikelihoodRatio(k11: Long, k12: Long, k21: Long, k22: Long): Double = {
    val rowEntropy = entropy(k11 + k12, k21 + k22)
    val columnEntropy = entropy(k11 + k21, k12 + k22)
    val matrixEntropy = entropy(k11, k12, k21, k22)
    if (rowEntropy + columnEntropy < matrixEntropy) {
      0.0
    } else {
      2.0 * (rowEntropy + columnEntropy - matrixEntropy)
    }
  }

  def entropy(elements: Long*): Double = {
    val result = elements.map(xLogX).sum
    val sum = elements.sum
    xLogX(sum) - result
  }

  def xLogX(x: Long): Double = {
    if (x == 0) 0.0 else x * math.log(x)
  }

  def jaccard(mat: RowMatrix): CoordinateMatrix = {

    mat.rows.cache()

    val pairs = mat.rows.flatMap { v =>
      val indices = v.toSparse.indices
      indices.flatMap { i =>
        indices.filter(_ > i).map { j =>
          (i, j) -> 1
        }
      }
    }.reduceByKey(_ + _)

    val items = mat.rows.flatMap { v =>
      val indices = v.toSparse.indices
      indices.map { i =>
        i -> 1
      }
    }.reduceByKey(_ + _).cache()

    val entries = pairs.map { case ((i, j), v) =>
      i -> (j, v)
    }.join(items).map { case (i, ((j, v), si)) =>
      j -> (i, v, si)
    }.join(items).flatMap { case (j, ((i, v, si), sj)) =>
      val value = v.toDouble / (si + sj - v)
      Seq(MatrixEntry(i, j, value), MatrixEntry(j, i, value))
    }

    new CoordinateMatrix(entries)
  }

  def normalize(values: Iterable[(Int, Double)]): Iterable[(Int, Double)] = {
    val count = values.size
    val mean = values.map(_._2).sum / count
    val sd = values.map(_._2).map(v => math.pow(v - mean, 2)).sum / count
    val std = math.sqrt(sd)
    values.map(t => t._1 -> (t._2 - mean) / std)
  }

  def euclidean(mat: CoordinateMatrix): CoordinateMatrix = {

    // transpose
    // normalize
    // cartesian
    // calc

    mat.transpose.toIndexedRowMatrix.rows.map { row =>
      val values = row.vector.toSparse.indices.map { i =>
        i -> row.vector(i)
      }
      Vectors.sparse(row.vector.size, normalize(values).toSeq)
    }

    null
  }

}
