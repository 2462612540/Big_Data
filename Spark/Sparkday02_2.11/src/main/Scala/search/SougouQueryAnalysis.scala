package search

import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 用户查询日志(SogouQ)分析，数据来源Sogou搜索引擎部分网页查询需求及用户点击情况的网页查询日志数据集合。
 * 1．搜索关键词统计，使用HanLP中文分词
 * 2．用户搜索次数统计
 * 3．搜索时间段统计*数据格式:访问时间\t用户ID\t[查询词]\t i该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
 * 其中，用户ID是根据用户使用浏览器访问搜索引擎时的Cookie信息自动赋值，即同一次使用浏览器输入的不同查询对应同一个用户ID
 */

object SougouQueryAnalysis {
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
    val inputpath2 = "E:\\GItHub_project\\Big_Data\\Spark\\Sparkday02_2.11\\src\\main\\resources\\SogouQ.reduced"
    val sougouRDD = sc.textFile(inputpath2, minPartitions = 2)
    print(s"count=${sougouRDD.count()}")
    println(sougouRDD.first())


    //TODO 2:数据的ETL操作的
    val etlRDD = sougouRDD
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
    println(etlRDD.first())
    //由于数据使用多次 需要缓存数据
    etlRDD.persist(StorageLevel.MEMORY_AND_DISK)

    //TODO:搜索关键次统计
    val resultRDD = etlRDD
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
    //查找次数最多的10个单词
    resultRDD
      .sortBy(tuple => tuple._2, ascending = false)
      .take(10)
      .foreach(println)

    //TODO:用户搜索及统计
    /**
     * 分组的字段 先按照用户分组 在按照的搜索四分词分组
     */

    val perUserQueryWordsCountRDD: RDD[((String, String), Int)] = etlRDD
      .mapPartitions { iter =>
        iter.map { record =>
          val userID = record.userId
          val querywords = record.queryWords
          //组合用户的ID和queryword为key
          ((userID, querywords), 1)
        }
      }
      //分组聚合
      .reduceByKey((tmp, item) => tmp + item)
    //TODO :获取搜索的点击的次数的最大值和最小中 和平均值
    val restRDD: RDD[Int] = perUserQueryWordsCountRDD.map(tuple => tuple._2)
    println(s"max click count=${restRDD.max()}")
    println(s"min click count=${restRDD.min()}")
    println(s"avg click count=${restRDD.mean()}")

    //TODO:搜索的时间的统计
    etlRDD
      .map { record =>
        //获取小时
        val hourStr: String = record.queryTime.substring(0, 2)
        //返回二元组
        (hourStr, 1)
      }
      .reduceByKey((tmp, item) => tmp + item)
      .top(num = 24)(Ordering.by(tuple => tuple._2))
      .foreach(println)

    //释放资源资源
    etlRDD.unpersist()

    Thread.sleep(100000000)

    //TODO 关闭的对象
    sc.stop()
  }
}
