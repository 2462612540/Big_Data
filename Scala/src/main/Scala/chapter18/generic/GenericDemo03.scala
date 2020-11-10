package com.atguigu.chapter18.generic

object GenericDemo03 {
  def main(args: Array[String]): Unit = {
    val list1 = List("hello", "dog", "world")
    val list2 = List(90, 10, 23)
    println(midList[String](list1)) // "dog"
    println(midList[Int](list2)) // 10
  }

  /*
要求
定义一个函数，可以获取各种类型的 List 的中间index的值
使用泛型完成

 */
  def midList[E](l: List[E]): E = {
    l(l.length / 2)
  }
}

