package datasource

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types._

class JdbcSourceV2 extends DataSourceV2 with ReadSupport {
  def createReader(options: DataSourceOptions) = new JdbcDataSourceReader(
    options.get("url").get(),
    options.get("user").get(),
    options.get("password").get(),
    options.get("table").get()
  )
}

class JdbcDataSourceReader(
    url: String,
    user: String,
    password: String,
    table: String)
  extends DataSourceReader {

  def readSchema() = StructType(Seq(
    StructField("id", IntegerType),
    StructField("emp_name", StringType),
    StructField("dep_name", StringType),
    StructField("salary", DecimalType(7, 2)),
    StructField("age", DecimalType(3, 0))
  ))

  def createDataReaderFactories()= {
    val factoryList = new java.util.ArrayList[DataReaderFactory[Row]]
    factoryList.add(new JdbcDataReaderFactory(url, user, password, table))
    factoryList
  }
}

class JdbcDataReaderFactory(
    url: String,
    user: String,
    password: String,
    table: String)
  extends DataReaderFactory[Row] {

  def createDataReader() = new JdbcDataReader(url, user, password, table)
}

class JdbcDataReader(
    url: String,
    user: String,
    password: String,
    table: String)
  extends DataReader[Row] {

  val conn = DriverManager.getConnection(url, user, password)
  val stmt = conn.prepareStatement(s"SELECT * FROM $table",
    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
  stmt.setFetchSize(1000)
  val rs = stmt.executeQuery()

  def next() = rs.next()

  def get() = Row(
    rs.getInt("id"),
    rs.getString("emp_name"),
    rs.getString("dep_name"),
    rs.getBigDecimal("salary"),
    rs.getBigDecimal("age")
  )

  def close() = {
    conn.close()
  }
}

object JdbcExampleV2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val df = spark.read
      .format("datasource.JdbcSourceV2")
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
