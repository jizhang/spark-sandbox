import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HConnectionManager, Get}
import org.apache.hadoop.hbase.util.Bytes


object HBaseClient {

  lazy val conn = {
    val conf = new Configuration()
    conf.set("hbase.zookeeper.quorum", "ods10-008:2181")
    HConnectionManager.createConnection(conf)
  }

  def getValue(): String = {
    val table = conn.getTable("zj_mobile_bfu_props")
    try {
      val get = new Get(Bytes.toBytes("00:10000051:esf"))
      Bytes.toString(table.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("comm_name")))
    } finally {
      table.close()
    }
  }

}
