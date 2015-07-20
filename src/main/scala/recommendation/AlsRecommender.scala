package recommendation

import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, Rating}


object AlsRecommender extends Recommender {

  val logger = LoggerFactory.getLogger(getClass)

  override def recommend(trainingSet: RDD[Rating], params: Map[String, Any]): RDD[(Int, Seq[Rating])] = {

    val numRecommendations = params.getInt("numRecommendations")
    val recommendMethod = params.getString("recommendMethod")

    val rank = 10
    val numIterations = 20
    val model = new ALS()
      .setRank(rank)
      .setIterations(numIterations)
      .setImplicitPrefs(true)
      .run(trainingSet)

    recommendMethod match {
      case "user-product" => model.recommendProductsForUsers(numRecommendations).mapValues(_.toSeq)

      case "product-user" =>
        model.recommendUsersForProducts(numRecommendations * 10).values.flatMap { products =>
          products.map(r => r.user -> r)
        }.groupByKey.mapValues { products =>
          products.toSeq.sortWith(_.rating > _.rating).take(numRecommendations)
        }

      case _ => throw new IllegalArgumentException("unknown recommend method")
    }

  }

}
