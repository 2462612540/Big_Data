package chapter10

import scala.collection.mutable.ArrayBuffer

object Array22ArrayBuffer {
  def main(args: Array[String]): Unit = {

    val arr2 = ArrayBuffer[Int]()
    // 追加值
    arr2.append(1, 2, 3)
    println(arr2)

    //说明
    //1. arr2.toArray 调用 arr2的方法 toArray
    //2. 将 ArrayBuffer ---> Array
    //3. arr2本身没有任何变化
    val newArr = arr2.toArray
    println(newArr)

    //说明
    //1. newArr.toBuffer 是把 Array->ArrayBuffer
    //2. 底层的实现
    /*
     override def toBuffer[A1 >: A]: mutable.Buffer[A1] = {
    val result = new mutable.ArrayBuffer[A1](size)
    copyToBuffer(result)
    result
  }
     */
    //3. newArr本身没变化
    val newArr2 = newArr.toBuffer
    newArr2.append(123)
    println(newArr2)
    //    //案例演示+说明

  }
}
