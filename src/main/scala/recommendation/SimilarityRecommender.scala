package recommendation

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, MatrixEntry, CoordinateMatrix}
import org.apache.spark.mllib.recommendation.Rating


object SimilarityRecommender extends Recommender {

  override def recommend(trainingSet: RDD[Rating], params: Map[String, Any]): RDD[(Int, Seq[Rating])] = {

    val numNeighbours = params.getInt("numNeighbours")
    val numRecommendations = params.getInt("numRecommendations")
    val similarityMethod = params.getString("similarityMethod")

    // train
    val userProducts = trainingSet.map { case Rating(user, product, rating) =>
      (user, (product, rating))
    }.groupByKey

    val numColumns = params.getInt("numColumns")
    val rows = userProducts.values.map { productRatings =>
      Vectors.sparse(numColumns, productRatings.toSeq)
    }

    val mat = new RowMatrix(rows)
    val sim = similarityMethod match {
      case "spark" => mat.columnSimilarities()
      case "spark-appr" => mat.columnSimilarities(0.1)
      case "cosine" => cosineSimilarity(mat)
      case "cosine-mod" => modifiedCosineSimilarity(mat)
      case _ => throw new IllegalArgumentException("unkown similarity method")
    }

    // recommend
    val simTop = sim.entries.map { case MatrixEntry(i, j, u) =>
      i.toInt -> (j.toInt, u)
    }.groupByKey.mapValues { products =>
      val productsTop = products.toSeq.sortWith(_._2 > _._2).take(numNeighbours)
      normalizeRange(productsTop)
    }

    val t1 = simTop.flatMap { case (i, products) =>
      products.map { case (j, u) =>
        j -> (i, u)
      }
    }

    val t2 = trainingSet.map { case Rating(user, product, rating) =>
      user -> (product, rating)
    }.groupByKey.flatMap { case (user, products) =>
      normalizeRange(products).map { case (product, rating) =>
        product -> (user, rating)
      }
    }

    t1.join(t2).map { case (product, ((i, u), (user, rating))) =>
      user -> (product, i, u, rating)
    }.groupByKey.map { case (user, products) =>

      val visited = products.map(_._1).toSet
      val newProducts = products.filterNot(t => visited(t._2)).map(t => t._2 -> (t._3, t._4))

      val productsTop = newProducts.groupBy(_._1).mapValues(_.map(_._2)).map { case (product, products) =>

        val total = products.map(_._1).sum
        if (total == 0) {
          product -> 0.0
        } else {
          val score = products.map(t => t._1 * t._2).sum
          product -> score / total
        }

      }.toSeq.sortWith(_._2 > _._2).take(numRecommendations)

      user -> productsTop.map { case (product, rating) =>
        Rating(user, product, rating)
      }

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
    }

    val entries = pairs.reduceByKey(_ + _).map { case ((i, j), v) =>
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

}
