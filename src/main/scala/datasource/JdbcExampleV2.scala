package datasource

import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory


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
  extends DataSourceReader with SupportsPushDownRequiredColumns {

  val tableSchema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("emp_name", StringType),
    StructField("dep_name", StringType),
    StructField("salary", DecimalType(7, 2)),
    StructField("age", DecimalType(3, 0))
  ))

  var prunedSchema = tableSchema

  def readSchema = prunedSchema

  def createDataReaderFactories() = {
    val columns = prunedSchema.fields.map(_.name)
    val factoryList = new java.util.ArrayList[DataReaderFactory[Row]]
    factoryList.add(new JdbcDataReaderFactory(url, user, password, table, columns))
    factoryList
  }

  def pruneColumns(requiredSchema: StructType) = {
    val names = requiredSchema.fields.map(_.name.toLowerCase).toSet
    val fields = tableSchema.fields.filter(field => names(field.name.toLowerCase))
    prunedSchema = StructType(fields)
  }
}


class JdbcDataReaderFactory(
    url: String,
    user: String,
    password: String,
    table: String,
    columns: Seq[String])
  extends DataReaderFactory[Row] {

  def createDataReader() = new JdbcDataReader(url, user, password, table, columns)
}


class JdbcDataReader(
    url: String,
    user: String,
    password: String,
    table: String,
    columns: Seq[String])
  extends DataReader[Row] {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private var conn: Connection = null
  private var rs: ResultSet = null

  def next() = {
    if (rs == null) {
      conn = DriverManager.getConnection(url, user, password)
      val sql = s"SELECT ${columns.mkString(", ")} FROM $table"
      logger.info(sql)
      val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY)
      stmt.setFetchSize(1000)
      rs = stmt.executeQuery()
    }

    rs.next()
  }

  def get() = {
    val values = columns.map {
      case "id" => rs.getInt("id")
      case "emp_name" => rs.getString("emp_name")
      case "dep_name" => rs.getString("dep_name")
      case "salary" => rs.getBigDecimal("salary")
      case "age" => rs.getBigDecimal("age")
    }
    Row.fromSeq(values)
  }

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

    df.createTempView("employee")
    val dfSelect = spark.sql("SELECT id, emp_name, age FROM employee LIMIT 1")
    dfSelect.explain(true)
    dfSelect.show()

    spark.stop()
  }
}
