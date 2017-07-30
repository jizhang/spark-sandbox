import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import scalikejdbc._


object ExactlyOnce {
  def main(args: Array[String]): Unit = {
    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "exactly-once",
        "auto.offset.reset" -> "latest")

    val conf = new SparkConf().setAppName("ExactlyOnce").setIfMissing("spark.master", "local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val messages = KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Seq("alog"), kafkaParams))

    messages.map(_.value)
      .flatMap(parseLog)
      .filter(_.level == "ERROR")
      .map(log => log.time.truncatedTo(ChronoUnit.MINUTES) -> 1)
      .reduceByKey(_ + _)
      .foreachRDD { (rdd, time) =>
        rdd.foreachPartition { iter =>
          ConnectionPool.singleton("jdbc:mysql://localhost:3306/spark", "root", "")
          iter.foreach { case (time, count) =>
            DB.autoCommit { implicit session =>
              sql"""
              insert into error_log (log_time, log_count)
              value (${time}, ${count})
              on duplicate key update log_count = log_count + values(log_count)
              """.update.apply()
            }
          }
        }
      }

    ssc.start()
    ssc.awaitTermination()
  }

  case class Log(time: LocalDateTime, level: String)

  val logPattern = "^(.{19}) ([A-Z]+).*".r
  val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def parseLog(line: String): Option[Log] = {
    line match {
      case logPattern(timeString, level) => {
        val timeOption = try {
          Some(LocalDateTime.parse(timeString, dateTimeFormatter))
        } catch {
          case _: DateTimeParseException => None
        }
        timeOption.map(Log(_, level))
      }
      case _ => {
        None
      }
    }
  }
}
