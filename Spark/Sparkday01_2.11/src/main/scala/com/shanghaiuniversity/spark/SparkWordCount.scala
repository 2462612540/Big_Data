package com.shanghaiuniversity.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * ����Scala����ʹ��SparkCore���ʵ�ִ�Ƶͳ��: wordCount*
 * ��HDFS�϶�ȡ���ݣ�ͳ��Worddpunt����������浽HDFS��
 */

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    //TODO:1 ����һ��SparkContext��ʵ������ ��Ҫ���ݵ���sparkConf���� ���õ����õ�������Ϣ
    val sparkConf = new SparkConf()
      .setAppName("SparkWordCount")
      .setMaster("local[2]"); //�������еı��ػ���

    val sc: SparkContext = new SparkContext(sparkConf);
    val inputpath = "E:\\GItHub_project\\Big_Data\\Spark\\Sparkday01_2.11\\src\\main\\resources\\data.txt";
    val outputpath = "E:\\GItHub_project\\Big_Data\\Spark\\Sparkday01_2.11\\src\\main\\resources\\result";

    //TODO ����һ�����Ƕ�ȡ���� ��װ���ݵ�RDD������
    val inputRDD: RDD[String] = sc.textFile(inputpath)
    //TODO ���ڶ������������� ����RDD�ĺ���
    val resultRDD = inputRDD
      //��ÿһ�е����ݰ��շָ���Ž��зָ�
      .flatMap(line => line.split("\\s+"))
      //ת��Ϊ��Ԫ�� ��ʾ��ÿһ�������ֵ�һ��
      .map(word => (word, 1))
      //���յ��ʵ�word ���� �ڽ������ڵľۺ�
      .reduceByKey((tmp, item) => tmp + item)
    //TODO ������������������ ������RDD������ݱ������ⲿ�Ĵ洢ϵͳ��
    resultRDD.foreach(tuple => println(tuple));
    resultRDD.saveAsTextFile(outputpath)
    Thread.sleep(1000000000);
    sc.stop();
  }
}
