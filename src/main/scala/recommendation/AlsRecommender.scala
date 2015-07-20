package recommendation

import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, Rating}


object AlsRecommender extends Recommender {

  val logger = LoggerFactory.getLogger(getClass)

  override def recommend(trainingSet: RDD[Rating], params: Map[String, Any]): RDD[(Int, Seq[Rating])] = {

    val rank = 10
    val numIterations = 20
    val model = new ALS()
      .setRank(rank)
      .setIterations(numIterations)
      .setImplicitPrefs(true)
      .run(trainingSet)

    val numRecommendations = params.getInt("numRecommendations")
    model.recommendProductsForUsers(numRecommendations).mapValues(_.toSeq)
  }

}
