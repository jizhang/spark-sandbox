package datasource

import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


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
  extends DataSourceReader with SupportsPushDownRequiredColumns
  with SupportsPushDownFilters {

  val tableSchema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("emp_name", StringType),
    StructField("dep_name", StringType),
    StructField("salary", DecimalType(7, 2)),
    StructField("age", DecimalType(3, 0))
  ))

  var prunedSchema = tableSchema
  var filters = Array.empty[Filter]
  var wheres = Array.empty[String]

  def readSchema = prunedSchema

  def createDataReaderFactories() = {
    val columns = prunedSchema.fields.map(_.name)
    Seq((1, 6), (7, 11)).map { case (minId, maxId) =>
      val partition = s"id BETWEEN $minId AND $maxId"
      new JdbcDataReaderFactory(url, user, password, table, columns, wheres, partition)
        .asInstanceOf[DataReaderFactory[Row]]
    }.asJava
  }

  def pruneColumns(requiredSchema: StructType) = {
    val names = requiredSchema.fields.map(_.name.toLowerCase).toSet
    val fields = tableSchema.fields.filter(field => names(field.name.toLowerCase))
    prunedSchema = StructType(fields)
  }

  def pushFilters(filters: Array[Filter]) = {
    val pushed = ArrayBuffer.empty[Filter]
    val rejected = ArrayBuffer.empty[Filter]
    val wheres = ArrayBuffer.empty[String]

    filters.foreach {
      case filter: EqualTo => {
        pushed += filter
        wheres += s"${filter.attribute} = '${filter.value}'"
      }
      case filter => rejected += filter
    }

    this.filters = pushed.toArray
    this.wheres = wheres.toArray
    rejected.toArray
  }

  def pushedFilters = filters
}


class JdbcDataReaderFactory(
    url: String,
    user: String,
    password: String,
    table: String,
    columns: Seq[String],
    wheres: Seq[String],
    partition: String)
  extends DataReaderFactory[Row] {

  def createDataReader() = new JdbcDataReader(url, user, password, table, columns, wheres, partition)
}


class JdbcDataReader(
    url: String,
    user: String,
    password: String,
    table: String,
    columns: Seq[String],
    wheres: Seq[String],
    partition: String)
  extends DataReader[Row] {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private var conn: Connection = null
  private var rs: ResultSet = null

  def next() = {
    if (rs == null) {
      conn = DriverManager.getConnection(url, user, password)

      val sqlBuilder = new StringBuilder()
      sqlBuilder ++= s"SELECT ${columns.mkString(", ")} FROM $table WHERE $partition"
      if (wheres.nonEmpty) {
        sqlBuilder ++= " AND " + wheres.mkString(" AND ")
      }
      val sql = sqlBuilder.toString
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
    val dfSelect = spark.sql("SELECT id, emp_name, salary FROM employee WHERE dep_name = 'Management'")
    dfSelect.explain(true)
    dfSelect.show()

    spark.stop()
  }
}
