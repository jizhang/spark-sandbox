package datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, TableScan}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

case class SimpleScan(from: Int, to: Int)(@transient val sparkSession: SparkSession)
  extends BaseRelation with TableScan {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType =
    StructType(StructField("i", IntegerType, nullable = false) :: Nil)

  override def buildScan(): RDD[Row] = {
    sparkSession.sparkContext.parallelize(from to to).map(Row(_))
  }
}

class SimpleScanSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    SimpleScan(parameters("from").toInt, parameters("TO").toInt)(sqlContext.sparkSession)
  }
}

object ExampleV1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val df = spark.read
      .format("datasource.SimpleScanSource")
      .option("from", 1)
      .option("to", 100)
      .load()

    df.show()

    spark.stop()
  }
}
