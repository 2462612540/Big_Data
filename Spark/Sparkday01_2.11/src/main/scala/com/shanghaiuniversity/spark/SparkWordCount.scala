package com.shanghaiuniversity.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 基于Scala语言使用SparkCore编程实现词频统计: wordCount*
 * 从HDFS上读取数据，统计Worddpunt，将结果保存到HDFS上
 */

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    //TODO:1 创建一个SparkContext的实例对象 需要传递的是sparkConf对象 设置的配置的引用信息
    val sparkConf = new SparkConf()
      .setAppName("SparkWordCount")
      .setMaster("local[2]"); //设置运行的本地环境

    val sc: SparkContext = new SparkContext(sparkConf);
    val inputpath = "E:\\GItHub_project\\Big_Data\\Spark\\Sparkday01_2.11\\src\\main\\resources\\data.txt";
    val outputpath = "E:\\GItHub_project\\Big_Data\\Spark\\Sparkday01_2.11\\src\\main\\resources\\result";

    //TODO ：第一步：是读取数据 封装数据到RDD集合中
    val inputRDD: RDD[String] = sc.textFile(inputpath)
    //TODO ：第二步：分析数据 调用RDD的函数
    val resultRDD = inputRDD
      //将每一行的数据按照分割符号进行分割
      .flatMap(line => line.split("\\s+"))
      //转换为二元组 表示的每一个单出现的一次
      .map(word => (word, 1))
      //按照单词的word 分组 在进行组内的聚合
      .reduceByKey((tmp, item) => tmp + item)
    //TODO ：第三步：保存数据 将最后的RDD结果数据保存在外部的存储系统中
    resultRDD.foreach(tuple => println(tuple));
    resultRDD.saveAsTextFile(outputpath)
    Thread.sleep(1000000000);
    sc.stop();
  }
}
