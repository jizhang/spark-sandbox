import org.slf4j.LoggerFactory
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object PageRankRecommender {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("PageRankRecommender").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val users: RDD[(VertexId, (String, String))] = sc.parallelize(Array(
        (3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof"))))

    val relationships: RDD[Edge[String]] = sc.parallelize(Array(
        Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "pi")))

    val defaultUser = ("John Doe", "Missing")

    val graph = Graph(users, relationships, defaultUser)

    val cnt = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count

    println(cnt)

    pageRank(sc)

    sc.stop()

  }

  def pageRank(sc: SparkContext): Unit = {

    val graph = GraphLoader.edgeListFile(sc, "data/followers.txt")
    val ranks = graph.pageRank(0.0001).vertices
    val users = sc.textFile("data/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }

    val ranksByUsername = users.join(ranks).map { case(id, (username, rank)) => (username, rank) }

    println(ranksByUsername.collect.mkString("\n"))

  }

}
