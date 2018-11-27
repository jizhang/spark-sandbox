package datasource

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Optional

import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer


object JdbcSourceV2 {
  val schema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("emp_name", StringType),
    StructField("dep_name", StringType),
    StructField("salary", DecimalType(7, 2)),
    StructField("age", DecimalType(3, 0))
  ))
}


class JdbcSourceV2 extends DataSourceV2 with ReadSupport with WriteSupport {
  def createReader(options: DataSourceOptions) = new JdbcDataSourceReader(
    options.get("url").get(),
    options.get("user").get(),
    options.get("password").get(),
    options.get("table").get()
  )

  override def createWriter(
      jobId: String,
      schema: StructType,
      mode: SaveMode,
      options: DataSourceOptions): Optional[DataSourceWriter] = {

    Optional.of(new JdbcDataSourceWriter(
      options.get("url").get(),
      options.get("user").get(),
      options.get("password").get(),
      options.get("table").get(),
      schema
    ))
  }
}


class JdbcDataSourceReader(
    url: String,
    user: String,
    password: String,
    table: String)
  extends DataSourceReader with SupportsPushDownRequiredColumns
  with SupportsPushDownFilters {

  var requiredSchema = JdbcSourceV2.schema

  var filters = Array.empty[Filter]
  var wheres = Array.empty[String]

  def readSchema = requiredSchema

  def createDataReaderFactories() = {
    val columns = requiredSchema.fields.map(_.name)
    Seq((1, 6), (7, 11)).map { case (minId, maxId) =>
      val partition = s"id BETWEEN $minId AND $maxId"
      new JdbcDataReaderFactory(url, user, password, table, columns, wheres, partition)
        .asInstanceOf[DataReaderFactory[Row]]
    }.asJava
  }

  def pruneColumns(requiredSchema: StructType) = {
    this.requiredSchema = requiredSchema
  }

  def pushFilters(filters: Array[Filter]) = {
    val supported = ListBuffer.empty[Filter]
    val unsupported = ListBuffer.empty[Filter]
    val wheres = ListBuffer.empty[String]

    filters.foreach {
      case filter: EqualTo => {
        supported += filter
        wheres += s"${filter.attribute} = '${filter.value}'"
      }
      case filter => unsupported += filter
    }

    this.filters = supported.toArray
    this.wheres = wheres.toArray
    unsupported.toArray
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


class JdbcDataSourceWriter(
    url: String,
    user: String,
    password: String,
    table: String,
    schema: StructType)
  extends DataSourceWriter {

  override def createWriterFactory(): DataWriterFactory[Row] = {
    new JdbcDataWriterFactory(url, user, password, table, schema)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = Unit

  override def abort(messages: Array[WriterCommitMessage]): Unit = Unit
}


class JdbcDataWriterFactory(
    url: String,
    user: String,
    password: String,
    table: String,
    schema: StructType)
  extends DataWriterFactory[Row] {

  override def createDataWriter(
      partitionId: Int,
      attemptNumber: Int): DataWriter[Row] = {

    new JdbcDataWriter(url, user, password, table, schema)
  }
}


class JdbcDataWriter(
    url: String,
    user: String,
    password: String,
    table: String,
    schema: StructType)
  extends DataWriter[Row] {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private var conn: Connection = null
  private var stmt: PreparedStatement = null

  override def write(record: Row): Unit = {
    if (stmt == null) {
      conn = DriverManager.getConnection(url, user, password)
      conn.setAutoCommit(false)

      val columns = schema.fields.map(_.name).mkString(", ")
      val placeholders = ("?" * schema.fields.length).mkString(", ")
      stmt = conn.prepareStatement(s"REPLACE INTO $table ($columns) VALUES ($placeholders)")
    }

    schema.fields.map(_.name).zipWithIndex.foreach { case (name, index) =>
      name match {
        case "id" => stmt.setInt(index + 1, record.getInt(index))
        case "emp_name" => stmt.setString(index + 1, record.getString(index))
        case "dep_name" => stmt.setString(index + 1, record.getString(index))
        case "salary" => stmt.setBigDecimal(index + 1, record.getDecimal(index))
        case "age" => stmt.setBigDecimal(index + 1, record.getDecimal(index))
      }
    }

    logger.info(stmt.toString)
    stmt.executeUpdate()
  }

  override def commit(): WriterCommitMessage = {
    conn.commit()
    conn.close()
    null
  }

  override def abort(): Unit = {
    conn.rollback()
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

    val dfWrite = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(100, "John Doe", "Test", BigDecimal.decimal(1500), BigDecimal.decimal(32)),
        Row(101, "Jane Doe", "Test", BigDecimal.decimal(1000), BigDecimal.decimal(28))
      )),
      JdbcSourceV2.schema
    )

    dfWrite.write
      .format("datasource.JdbcSourceV2")
      .option("url", "jdbc:mysql://localhost/spark")
      .option("user", "root")
      .option("password", "")
      .option("table", "employee")
      .save()

    spark.stop()
  }
}
