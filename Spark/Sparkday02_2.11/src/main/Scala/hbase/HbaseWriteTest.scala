package hbase

import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import search.SougouRecord


/**
 * Hbase的数据连接 并将数据的写入的Hbase中
 */
object HbaseWriteTest {
  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {
      //TODO 构建一个spark的对象
      //构建Spark Application 应用的入口实例
      val sc: SparkContext = {
        val sparkConf: SparkConf = new SparkConf()
          .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
          .setMaster("local[2]")
        SparkContext.getOrCreate(sparkConf)
      }
      //TODO:1 加载搜狗的数据的集合使用小数据集合
      val inputpath = "E:\\GItHub_project\\Big_Data\\Spark\\Sparkday02_2.11\\src\\main\\resources\\SogouQ.sample"
      val sougouRDD = sc.textFile(inputpath, minPartitions = 2)
      print(s"count=${sougouRDD.count()}")
      println(sougouRDD.first())

      //TODO 2:数据的ETL操作的
      val etlRDD: RDD[SougouRecord] = sougouRDD
        .filter(line => null != line && line.trim.split("\\s+").length == 6)
        .mapPartitions { iter =>
          iter.map { line =>
            val array = line.trim.split("\\s+")
            //构建一个对象
            SougouRecord(
              array(0), array(1),
              array(2).replace("\\[\\]", ""),
              array(3).toInt, array(4).toInt,
              array(5)
            )
          }
        }
      //由于数据使用多次 需要缓存数据
      etlRDD.persist(StorageLevel.MEMORY_AND_DISK)

      //TODO:搜索关键次统计
      val resultRDD: RDD[(String, Int)] = etlRDD
        .filter(recode => null != recode.queryWords && recode.queryWords.trim.length > 0)
        .flatMap { record =>
          //360安全卫士
          val words = record.queryWords.trim
          //使用的HanLP分词进行中文分词 360  安全  卫士
          val terms: util.List[Term] = HanLP.segment(words)
          //将java中的list转化为的scala中的list
          import scala.collection.JavaConverters._
          //封装到二元组的中的表示每一个搜索单词的出现的一次
          val result = terms.asScala.map {
            term => (term.word, 1)
          }
          //返回的结果
          result
        }
        //分组聚合
        .reduceByKey((tmp, item) => tmp + item)

      //TODO:将来结果的数据保存都hbase
      /**
       * 表名：htb_wordCount
       * rowkey  word
       * columFamily :info
       * columns:count
       *
       * 创建表的语句是
       */
      //第一步是将RDD转换为的RDD[(IMMutableByWriteTable,Put)]
      val putsRDD: RDD[(ImmutableBytesWritable, Put)] = resultRDD.map { case (word, count) =>
        //创建的rowkey
        val rowkey = new ImmutableBytesWritable(Bytes.toBytes(word))
        //创建put对象
        val put: Put = new Put(rowkey.get())
        //添加column
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(count.toString))
        //返回二元组
        (rowkey, put)
      }

      //TODO 保存数据的Hbase
      /**
       * def saveAsNewAPIHadoopFile(
       * path: String,
       * keyClass: Class[_],
       * valueClass: Class[_],
       * outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
       * conf: Configuration = self.context.hadoopConfiguration)
       */
      //连接hbase的客户端的信息
      val conf = HBaseConfiguration.create()
      //TOD0:连接HBase表Zookeeper相关信息
      conf.set("hbase.zookeeper. quorum", " ") //主机IP
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("zookeeper.znode.parent", "/hbase")
      //TOD0:表的名称
      conf.set(TableOutputFormat.OUTPUT_TABLE, "htb_wordcount")

      //调用的底层的信息保存tableOutPUtForamt来数据
      putsRDD.saveAsNewAPIHadoopFile(
        path = " ",
        classOf[ImmutableBytesWritable],
        classOf[Put],
        classOf[TableOutputFormat[ImmutableBytesWritable]],
        conf
      )
      //TODO 关闭spark
      sc.stop()
    }
  }
}
