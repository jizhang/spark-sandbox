package recommendation

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, Rating}


object AlsRecommender extends Recommender {

  override def recommend(trainingSet: RDD[Rating], params: Map[String, Any]): RDD[(Int, Seq[Rating])] = {

    val rank = 10
    val numIterations = 20
    val model = ALS.trainImplicit(trainingSet, rank, numIterations)

    model.recommendProductsForUsers(30).mapValues(_.toSeq)
  }

}
