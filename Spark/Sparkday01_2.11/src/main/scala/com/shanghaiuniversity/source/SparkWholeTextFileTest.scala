package com.shanghaiuniversity.source

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * ���õ���SparkContextWholeTextFiles()������ȡС�ļ�
 * ʵ����Ŀ�У�������ʹ��wholeTextFiles������ȡ����T�����ʵ�RDD�������ٽ����ݱ��浽�ļ�ϵͳ���Ա����Ӧ�ö�ȡ��������������ܡ�
 */
object SparkWholeTextFileTest {
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

    /**
     * def wholeTextFiles(
     * path: String,
     * minPartitions: Int = defaultMinPartitions
     * ): RDD[(String,String)]
     */
    val filepath = ""
    //TODO :��ȡС�ļ�������  wholetextFiles����
    val inputRDD: RDD[(String, String)] = sc.wholeTextFiles(filepath, minPartitions = 2)
    println(s"RDD ������Ŀ=${
      inputRDD.getNumPartitions
    }")

    //��ӡ��������
    inputRDD.take(10).foreach(item => println(item))
    //�ر�spark
    sc.stop()
  }
}
