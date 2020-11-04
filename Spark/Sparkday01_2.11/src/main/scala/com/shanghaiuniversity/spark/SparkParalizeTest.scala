package com.shanghaiuniversity.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkParalizeTest {
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

    //TODO :创建一个本地的集合 创建RDD
    val seq: Seq[Int] = Seq(1, 2, 3, 4, 5, 6, 7, 8)

    /**
     * def parallelize[T: ClassTag](
     * seq: Seq[T]，
     * numslices: Int = defaultParallelism   表示的分区数
     * ): RDD[T]
     */
    val inputRDD = sc.parallelize(seq, numSlices = 2)
    inputRDD.foreach(item => print(item))

    //关闭的spark
    sc.stop()
  }
}
