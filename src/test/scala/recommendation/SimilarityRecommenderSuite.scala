package recommendation

import org.scalatest.FunSuite
import SimilarityRecommender._


class SimilarityRecommenderSuite extends FunSuite {

  test("normalize") {
    val input = Seq[Double](1, 2, 3).zipWithIndex.map(_.swap)
    assert(normalize(input).map(_._2) == input.map(_._2).map(v => (v - 2) / math.sqrt(2.0 / 3)))
  }

}
