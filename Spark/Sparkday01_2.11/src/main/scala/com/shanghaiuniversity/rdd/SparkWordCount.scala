package com.shanghaiuniversity.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * ʹ�õ�Sparkʵ�ֵĴ�Ƶ��ͳ�� ʹ�õ��ǵ�scala����
 */
object SparkWordCount {
  def main(args: Array[String]): Unit = {
    //TODO ����һ��spark�Ķ���
    val sc: SparkContext = {
      //����һ��Spark��������Ӧ����Ϣ
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setAppName("loacal[2]")
      //����sparkConf���� ����ʵ��
      SparkContext.getOrCreate(sparkConf)
    }
    //TODO ҵ�����
    val inputpath = ""
    //1 ��ȡ����
    val inputRDD: RDD[String] = sc.textFile(inputpath)
    //2����������ݵ� ���õ�RDD�е�transformation����
    val resultRDD: RDD[(String, Int)] = inputRDD
      //���˿�����
      .filter(line => null != line && line.trim.length != 0)
      //�ָ��
      .flatMap(line => line.trim.split("\\s+"))
      //תΪ��Ԫ�� ��ʾ����ÿһ�����ʵĳ��ֵĴ���
      .map(word => word -> 1)
      //����ۺ�
      .reduceByKey((tmp, item) => tmp + item)
    //3������ݵ����
    resultRDD.foreach(tuple => println(tuple))

    //TODO �ر�spark
    sc.stop()
  }
}
