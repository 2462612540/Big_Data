package com.shanghaiuniversity.rdd

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

import scala.collection.mutable.ListBuffer

/**
 * RDD的聚合函数 如何进行使用以及底层原理
 * reduce /fold
 * aggregate
 * groupBykey / reduceBykey / foldByKey / aggregate  /combinBykey
 *
 */
object SparkAggTest {
  def main(args: Array[String]): Unit = {
    //构建Spark Application 应用的入口实例
    val sc: SparkContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
    }

    //TODO RDD中的reduce 和fold函数
    val dataRDD = sc.parallelize(seq = 1 to 10, numSlices = 2)
    dataRDD.foreachPartition { iter =>
      val paritionID: Int = TaskContext.getPartitionId()
      iter.foreach(iter => println(s"$paritionID"))
    }
    println("=====================================================")
    //使用的reduce函数
    val resultRDD = dataRDD.reduce { (tmp, item) =>
      val partiionID: Int = TaskContext.getPartitionId()
      println(s"$partiionID:tmp =$tmp,item=$item,sum=${tmp + item}")
      tmp + item
    }
    println(s"RDD reduce=$resultRDD")
    println("=====================================================")
    /**
     * def aggregateru: classTag]
     * // TOD:表示娶合函数中间临时变量初始值( zeroValue: U)
     * (zerovalue:U)
     * (
     * 分区内数据娶合时使用娶合函数
     * seq0p: (U，T) =>U,
     * 分区间娶合数据娶合时使用娶合函数
     * comb0p: (U，U)=>u
     * )
     * ):U
     */
    /**
     * 需求是获取两个分区的最大的数据
     *
     * 1、定义聚合中间临时变量个数，类型
     * ListBuffer
     * 2、初始化中间临时变量值
     * 空集合
     */
    val resultRDD1 = dataRDD.aggregate(new ListBuffer[Int]())(
      (tmp: ListBuffer[Int], item: Int) => {
        tmp += item
        tmp.sorted.takeRight(2)
      },
      (tmp: ListBuffer[Int], item: ListBuffer[Int]) => {
        tmp ++= item
        tmp.sorted.takeRight(2)
      }
    )

    println(s"top2:${resultRDD1.toList.mkString(",")}")

    //关闭应用
    sc.stop()
  }
}
