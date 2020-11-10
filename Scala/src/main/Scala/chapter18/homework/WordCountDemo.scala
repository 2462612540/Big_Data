package com.atguigu.chapter18.homework

object WordCountDemo {
  def main(args: Array[String]): Unit = {

    /*
    课后练习3-大数据中经典的wordcount案例
val lines = List("atguigu han hello ", "atguigu han aaa aaa aaa ccc ddd uuu")
使用映射集合，list中，各个单词出现的次数，并按出现次数排序

     */
    val lines = List("atguigu han hello ", "atguigu han aaa aaa aaa ccc ddd uuu")
    //    //1. 步骤1
    //    val res1 = lines.flatMap(_.split(" "))
    //    println("res1=" + res1)
    //    val res2 = res1.map((_,1))
    //    println("res2=" + res2)
    //    //分组
    //    val res3 = res2.groupBy(_._1)
    //    println("res3=" + res3)
    //    //继续返回
    //    val res4 = res3.map(x=>(x._1,x._2.size))
    //    println("res4="+res4) //Map(han->1)
    //
    //    val res5 = res4.toList.sortBy(_._2).reverse
    //    println("res5=" + res5)

    //合在一起,函数不停的进行集合的处理.
    println(lines.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).map(x => (x._1, x._2.size)).toList.sortBy(_._2).reverse)


  }
}
