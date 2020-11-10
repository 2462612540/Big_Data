package chapter10

import scala.collection.mutable.ArrayBuffer

object ArrayBufferDemo01 {
  def main(args: Array[String]): Unit = {
    //创建ArrayBuffer
    val arr01 = ArrayBuffer[Any](3, 2, 5)

    //访问，查询
    //通过下标访问元素
    println("arr01(1)=" + arr01(1)) // arr01(1) = 2
    //遍历
    for (i <- arr01) {
      println(i)
    }
    println(arr01.length) //3
    println("arr01.hash=" + arr01.hashCode())


    //修改 [修改值，动态增加]
    //使用append 追加数据 ,append支持可变参数
    //可以理解成java的数组的扩容
    arr01.append(90.0, 13) // (3,2,5,90.0,13)
    println("arr01.hash=" + arr01.hashCode())


    println("===================")


    arr01(1) = 89 //修改 (3,89,5,90.0,13)
    println("--------------------------")
    for (i <- arr01) {
      println(i)
    }

    //删除...
    //删除,是根据下标来说

    arr01.remove(0) // (89,5,90.0,13)
    println("--------删除后的元素遍历---------------")
    for (i <- arr01) {
      println(i)
    }
    println("最新的长度=" + arr01.length) // 4

  }
}


