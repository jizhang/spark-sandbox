package datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession, types}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import scalikejdbc._

case class JdbcRelationV1(
    url: String,
    user: String,
    password: String,
    table: String)(@transient val sparkSession: SparkSession)
  extends BaseRelation with PrunedFilteredScan {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = {
    ConnectionPool.singleton(url, user, password)
    val fields = DB.readOnly { implicit session =>
      SQL(s"desc $table")
        .map(rs => StructField(rs.string("Field"), parseType(rs.string("Type"))))
        .list
        .apply()
    }
    StructType(fields)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    sparkSession.sparkContext.emptyRDD
  }

  private def parseType(dbType: String): DataType = {
    dbType match {
      case _ if dbType.contains("int") => IntegerType
      case _ if dbType.contains("decimal") => FloatType
      case _ => StringType
    }
  }
}

class JdbcSourceV1 extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    JdbcRelationV1(parameters("url"), parameters("user"), parameters("password"),
      parameters("table"))(sqlContext.sparkSession)
  }
}

object JdbcExampleV1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val df = spark.read
      .format("datasource.JdbcSourceV1")
      .option("url", "jdbc:mysql://localhost/spark")
      .option("user", "root")
      .option("password", "")
      .option("table", "employee")
      .load()

    df.printSchema()

    spark.stop()
  }
}
