package com.shanghaiuniversity.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用的Spark实现的词频的统计 使用的是的scala语言
 */
object SparkWordCount {
  def main(args: Array[String]): Unit = {
    //TODO 构建一个spark的对象
    val sc: SparkContext = {
      //创建一个Spark对象设置应用信息
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setAppName("loacal[2]")
      //传递sparkConf对象 创建实例
      SparkContext.getOrCreate(sparkConf)
    }
    //TODO 业务操作
    val inputpath = ""
    //1 读取数据
    val inputRDD: RDD[String] = sc.textFile(inputpath)
    //2处理分析数据的 调用的RDD中的transformation函数
    val resultRDD: RDD[(String, Int)] = inputRDD
      //过滤空数据
      .filter(line => null != line && line.trim.length != 0)
      //分割单词
      .flatMap(line => line.trim.split("\\s+"))
      //转为二元组 表示的是每一个单词的出现的次数
      .map(word => word -> 1)
      //分组聚合
      .reduceByKey((tmp, item) => tmp + item)
    //3结果数据的输出
    resultRDD.foreach(tuple => println(tuple))

    //TODO 关闭spark
    sc.stop()
  }
}
