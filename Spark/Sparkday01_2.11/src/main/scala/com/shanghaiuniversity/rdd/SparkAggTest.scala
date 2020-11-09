package com.shanghaiuniversity.rdd

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

import scala.collection.mutable.ListBuffer

/**
 * RDD�ľۺϺ��� ��ν���ʹ���Լ��ײ�ԭ��
 * reduce /fold
 * aggregate
 * groupBykey / reduceBykey / foldByKey / aggregate  /combinBykey
 *
 */
object SparkAggTest {
  def main(args: Array[String]): Unit = {
    //����Spark Application Ӧ�õ����ʵ��
    val sc: SparkContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      SparkContext.getOrCreate(sparkConf)
    }

    //TODO RDD�е�reduce ��fold����
    val dataRDD = sc.parallelize(seq = 1 to 10, numSlices = 2)
    dataRDD.foreachPartition { iter =>
      val paritionID: Int = TaskContext.getPartitionId()
      iter.foreach(iter => println(s"$paritionID"))
    }
    println("=====================================================")
    //ʹ�õ�reduce����
    val resultRDD = dataRDD.reduce { (tmp, item) =>
      val partiionID: Int = TaskContext.getPartitionId()
      println(s"$partiionID:tmp =$tmp,item=$item,sum=${tmp + item}")
      tmp + item
    }
    println(s"RDD reduce=$resultRDD")
    println("=====================================================")
    /**
     * def aggregateru: classTag]
     * // TOD:��ʾȢ�Ϻ����м���ʱ������ʼֵ( zeroValue: U)
     * (zerovalue:U)
     * (
     * ����������Ȣ��ʱʹ��Ȣ�Ϻ���
     * seq0p: (U��T) =>U,
     * ������Ȣ������Ȣ��ʱʹ��Ȣ�Ϻ���
     * comb0p: (U��U)=>u
     * )
     * ):U
     */
    /**
     * �����ǻ�ȡ������������������
     *
     * 1������ۺ��м���ʱ��������������
     * ListBuffer
     * 2����ʼ���м���ʱ����ֵ
     * �ռ���
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

    //�ر�Ӧ��
    sc.stop()
  }
}
