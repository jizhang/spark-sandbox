import org.slf4j.LoggerFactory
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object PageRankRecommender {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("PageRankRecommender").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val relationshipMap = Map(
      "St" -> Seq("Forrest Gump", "Gladiator"),
      "Sm" -> Seq("Forrest Gump"),
      "Ab" -> Seq("The Reader", "Slumdog"),
      "Se" -> Seq("Pulp Fiction", "The Godfather")
    )

    val users = relationshipMap.keys.toSeq
    val movies = relationshipMap.values.flatMap(identity).toSet.toSeq
    val nodeMap = (users ++ movies).zipWithIndex.toMap

    val nodeRdd: RDD[(VertexId, Unit)] = sc.parallelize(users.map { user =>
      (nodeMap(user).toLong, ())
    }).union(sc.parallelize(movies.map { movie =>
      (nodeMap(movie).toLong, ())
    }))

    val relationshipRdd: RDD[Edge[Unit]] = sc.parallelize(relationshipMap.flatMap { case (user, movies) =>
      val userId = nodeMap(user)
      movies.flatMap { movie =>
        val movieId = nodeMap(movie)
        Seq(Edge(userId, movieId, ()), Edge(movieId, userId, ()))
      }
    }.toSeq)

    val graph = Graph(nodeRdd, relationshipRdd)

    val nodeIdMap = nodeMap.map(_.swap)
    val res = graph.personalizedPageRank(nodeMap("Sm"), 0.0001).vertices.flatMap { case (id, rank) =>
      val name = nodeIdMap(id.toInt)
      if (name.length > 2) {
        Some(name, rank)
      } else {
        None
      }
    }.collect

    res.sortBy(_._2).foreach(println)

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
    ranksByUsername.collect.sortBy(_._2).foreach(println)

    ranks.collect.sortBy(_._2).foreach(println)

  }

}
