package datasource

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._


case class JdbcRelationV1(
    url: String,
    user: String,
    password: String,
    table: String)(@transient val sparkSession: SparkSession)
  extends BaseRelation with PrunedFilteredScan {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType = StructType(Seq(
    StructField("id", IntegerType),
    StructField("emp_name", StringType),
    StructField("dep_name", StringType),
    StructField("salary", DecimalType(7, 2)),
    StructField("age", DecimalType(3, 0))
  ))

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    new JdbcRDD(sparkSession.sparkContext, requiredColumns, filters, url, user, password, table, schema)
  }
}


class JdbcRDD(
    sc: SparkContext,
    columns: Array[String],
    filters: Array[Filter],
    url: String,
    user: String,
    password: String,
    table: String,
    schema: StructType)
  extends RDD[Row](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val conn = DriverManager.getConnection(url, user, password)
    val stmt = conn.prepareStatement(s"SELECT * FROM $table",
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(1000)
    val rs = stmt.executeQuery()

    context.addTaskCompletionListener(_ => conn.close())

    new Iterator[Row] {
      def hasNext: Boolean = rs.next()
      def next: Row = {
        Row(
          rs.getInt("id"),
          rs.getString("emp_name"),
          rs.getString("dep_name"),
          rs.getBigDecimal("salary").floatValue(),
          rs.getBigDecimal("age").floatValue()
        )
      }
    }

}

  override protected def getPartitions: Array[Partition] = Array(JdbcPartition(0))
}


case class JdbcPartition(idx: Int) extends Partition {
  override def index: Int = idx
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
    df.show()

    spark.stop()
  }
}
