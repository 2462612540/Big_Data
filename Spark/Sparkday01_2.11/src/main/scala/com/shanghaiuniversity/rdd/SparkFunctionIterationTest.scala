package com.shanghaiuniversity.rdd

import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

object SparkFunctionIterationTest {
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
    println(s"inputRDD�ķ�����Ŀ$inputRDD.getNumPartitions")
    //TODO ���ӷ�����Ŀ
    val etlRDD = inputRDD.repartition(numPartitions = 4)
    println(s"etlDD������$etlRDD.getNumPartitions")

    //2����������ݵ� ���õ�RDD�е�transformation����
    val resultRDD: RDD[(String, Int)] = etlRDD
      //���˿�����
      .filter(line => null != line && line.trim.length != 0)
      //�ָ��
      .flatMap(line => line.trim.split("\\s+"))
      //תΪ��Ԫ�� ��ʾ����ÿһ�����ʵĳ��ֵĴ���
      .mapPartitions { iter =>
        //TODO ��Է����Ĳ��� ת��Ϊ��Ԫ�ر�ʾÿһ������ֻ����һ��
        //val xx:Iterable[String] = iter
        iter.map(word => word -> 1)
      }
      //����ۺ�
      .reduceByKey((tmp, item) => tmp + item)
    //3������ݵ����
    //resultRDD.foreach(tuple => println(tuple))
    resultRDD
      //TODO ���ͷ�������Ŀ�ĺ���
      .coalesce(numPartitions = 2)
      .foreachPartition { iter =>
        //��ȡ����������
        val partitionId: Int = TaskContext.getPartitionId()
        //TODO ��Է����Ĳ������ǽ������ӡ
        //val xx:Iterable[(String,Int)] =iter
        iter.foreach(tuple => println(s"${partitionId} :$tuple"))
      }

    //TODO �ر�spark
    sc.stop()
  }
}
