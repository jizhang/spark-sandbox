package datasource

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

case class KafkaPartition(
    partitionId: Int,
    leaderHost: String)
  extends Partition {

  def index: Int = partitionId
}

class KafkaRDD(sc: SparkContext) extends RDD[Row](sc, Nil) {
  def compute(split: Partition, context: TaskContext): Iterator[Row] = Iterator.empty

  def getPartitions: Array[Partition] = Array(
    // populate with Kafka PartitionInfo
    KafkaPartition(0, "broker_0"),
    KafkaPartition(1, "broker_1")
  )

  override def getPreferredLocations(split: Partition): Seq[String] = Seq(
    split.asInstanceOf[KafkaPartition].leaderHost
  )
}
