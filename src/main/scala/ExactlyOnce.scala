import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.ConsumerRecord
import scalikejdbc._


object ExactlyOnce {
  def main(args: Array[String]): Unit = {
    val brokers = "localhost:9092"
    val topic = "alog"

    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> brokers,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "exactly-once",
        "enable.auto.commit" -> (false: java.lang.Boolean),
        "auto.offset.reset" -> "none")

    ConnectionPool.singleton("jdbc:mysql://localhost:3306/spark", "root", "")

    val conf = new SparkConf().setAppName("ExactlyOnce").setIfMissing("spark.master", "local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val fromOffsets = DB.readOnly { implicit session =>
      sql"""
      select `partition`, offset from kafka_offset
      where topic = ${topic}
      """.map { rs =>
        new TopicPartition(topic, rs.int("partition")) -> rs.long("offset")
      }.list.apply().toMap
    }

    val messages = KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets))

    messages.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val result = processLogs(rdd).collect()

      DB.localTx { implicit session =>
        result.foreach { case (time, count) =>
          sql"""
          insert into error_log (log_time, log_count)
          value (${time}, ${count})
          on duplicate key update log_count = log_count + values(log_count)
          """.update.apply()
        }

        offsetRanges.foreach { offsetRange =>
          sql"""
          insert ignore into kafka_offset (topic, `partition`, offset)
          value (${topic}, ${offsetRange.partition}, ${offsetRange.fromOffset})
          """.update.apply()

          val affectedRows = sql"""
          update kafka_offset set offset = ${offsetRange.untilOffset}
          where topic = ${topic} and `partition` = ${offsetRange.partition}
          and offset = ${offsetRange.fromOffset}
          """.update.apply()

          if (affectedRows != 1) {
            throw new Exception("fail to update offset")
          }
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def processLogs(messages: RDD[ConsumerRecord[String, String]]): RDD[(LocalDateTime, Int)] = {
    messages.map(_.value)
      .flatMap(parseLog)
      .filter(_.level == "ERROR")
      .map(log => log.time.truncatedTo(ChronoUnit.MINUTES) -> 1)
      .reduceByKey(_ + _)
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
