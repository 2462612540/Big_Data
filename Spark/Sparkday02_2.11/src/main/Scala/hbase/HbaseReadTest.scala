package hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Hbase的数据连接 并重读取数据
 */
object HbaseReadTest {
  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {
      //TODO 构建一个spark的对象
      //构建Spark Application 应用的入口实例
      val sc: SparkContext = {
        val sparkConf: SparkConf = new SparkConf()
          .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
          .setMaster("local[2]")
          //TOD0:设置使用Kryo序列化方式
          .set("spark.serializer", "org.apache.spark.serializer. KryoSerializer")
          .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))

        SparkContext.getOrCreate(sparkConf)
      }

      //TODO 连接hbase的客户端的信息
      val conf = HBaseConfiguration.create()
      //TOD0:连接HBase表Zookeeper相关信息
      conf.set("hbase.zookeeper. quorum", " ") //主机IP
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("zookeeper.znode.parent", "/hbase")
      //TOD0:表的名称  注意导入的包文件
      conf set(TableInputFormat.INPUT_TABLE, "#表名")

      //TODO 读取HBase的数据
      /**
       * def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
       * path: String,
       * fClass: Class[F],
       * kClass: Class[K],
       * vClass: Class[V],
       * conf: Configuration = hadoopConfiguration): RDD[(K, V)]
       */
      val HbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
        conf,
        classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result]
      )
      //TODO 关闭spark
      sc.stop()
    }
  }
}
