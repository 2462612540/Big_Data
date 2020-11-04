package com.shanghaiuniversity.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkParalizeTest {
  def main(args: Array[String]): Unit = {
    //����һ��spark Content ����
    val sc: SparkContext = {
      // a ����sparkConf����
      val Sparkconf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      // b ����sparkConf���� ����ʵ��
      val context = SparkContext.getOrCreate(Sparkconf)
      //c ����ʵ������
      context
    }

    //TODO :����һ�����صļ��� ����RDD
    val seq: Seq[Int] = Seq(1, 2, 3, 4, 5, 6, 7, 8)

    /**
     * def parallelize[T: ClassTag](
     * seq: Seq[T]��
     * numslices: Int = defaultParallelism   ��ʾ�ķ�����
     * ): RDD[T]
     */
    val inputRDD = sc.parallelize(seq, numSlices = 2)
    inputRDD.foreach(item => print(item))

    //�رյ�spark
    sc.stop()
  }
}
