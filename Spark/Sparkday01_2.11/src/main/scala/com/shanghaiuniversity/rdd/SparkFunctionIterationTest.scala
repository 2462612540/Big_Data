package com.shanghaiuniversity.rdd

import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

object SparkFunctionIterationTest {
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
    println(s"inputRDD的分区数目$inputRDD.getNumPartitions")
    //TODO 增加分区数目
    val etlRDD = inputRDD.repartition(numPartitions = 4)
    println(s"etlDD分区数$etlRDD.getNumPartitions")

    //2处理分析数据的 调用的RDD中的transformation函数
    val resultRDD: RDD[(String, Int)] = etlRDD
      //过滤空数据
      .filter(line => null != line && line.trim.length != 0)
      //分割单词
      .flatMap(line => line.trim.split("\\s+"))
      //转为二元组 表示的是每一个单词的出现的次数
      .mapPartitions { iter =>
        //TODO 针对分区的操作 转化为二元素表示每一个单词只出现一次
        //val xx:Iterable[String] = iter
        iter.map(word => word -> 1)
      }
      //分组聚合
      .reduceByKey((tmp, item) => tmp + item)
    //3结果数据的输出
    //resultRDD.foreach(tuple => println(tuple))
    resultRDD
      //TODO 降低分区的数目的函数
      .coalesce(numPartitions = 2)
      .foreachPartition { iter =>
        //获取分区操作的
        val partitionId: Int = TaskContext.getPartitionId()
        //TODO 针对分区的操作的星将结果打印
        //val xx:Iterable[(String,Int)] =iter
        iter.foreach(tuple => println(s"${partitionId} :$tuple"))
      }

    //TODO 关闭spark
    sc.stop()
  }
}
