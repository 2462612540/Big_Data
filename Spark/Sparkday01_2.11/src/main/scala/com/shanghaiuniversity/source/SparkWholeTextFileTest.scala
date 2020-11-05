package com.shanghaiuniversity.source

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 采用的是SparkContextWholeTextFiles()方法读取小文件
 * 实际项目中，可以先使用wholeTextFiles方法读取数据T设置适当RDD分区，再将数据保存到文件系统，以便后续应用读取处理，大大提升性能。
 */
object SparkWholeTextFileTest {
  def main(args: Array[String]): Unit = {
    //构建一个spark Content 对象
    val sc: SparkContext = {
      // a 创建sparkConf对象
      val Sparkconf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      // b 传递sparkConf对象 创建实例
      val context = SparkContext.getOrCreate(Sparkconf)
      //c 返回实例对象
      context
    }

    /**
     * def wholeTextFiles(
     * path: String,
     * minPartitions: Int = defaultMinPartitions
     * ): RDD[(String,String)]
     */
    val filepath = ""
    //TODO :读取小文件的数据  wholetextFiles函数
    val inputRDD: RDD[(String, String)] = sc.wholeTextFiles(filepath, minPartitions = 2)
    println(s"RDD 分区数目=${
      inputRDD.getNumPartitions
    }")

    //打印样本数据
    inputRDD.take(10).foreach(item => println(item))
    //关闭spark
    sc.stop()
  }
}
