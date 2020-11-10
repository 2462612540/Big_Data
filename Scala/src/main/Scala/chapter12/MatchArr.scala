package chapter12

import scala.collection.mutable.ArrayBuffer

object MatchArr {
  def main(args: Array[String]): Unit = {

    //    val arrs = Array(Array(0), Array(1, 0), Array(0, 1, 0),
    //      Array(1, 1, 0), Array(1, 1, 0, 1))
    //
    //    for (arr <- arrs ) {
    //      val result = arr match {
    //        case Array(0) => "0"
    //        case Array(x, y) => x + "=" + y
    //        case Array(0, _*) => "以0开头和数组"
    //        case _ => "什么集合都不是"
    //      }
    //      // result = 0
    //      // result = 1 = 0
    //      // result = 以0开头和数组
    //      // result = 什么集合都不是
    //      // result = 什么集合都不是
    //      println("result = " + result)
    //    }

    //给你一个数组集合，如果该数组时  Array(10,20) , 请使用默认匹配，返回Array(20,10)


    val arrs2 = Array(Array(0), Array(1, 0), Array(0, 1, 0),
      Array(1, 1, 0), Array(1, 1, 0, 1))

    for (arr <- arrs2) {
      val result = arr match {
        //case Array(0) => "0"
        case Array(x, y) => ArrayBuffer(y, x) //Array(y,x).toBuffer //? ArrayB(y,x)
        //case Array(0, _*) => "以0开头和数组"
        case _ => "不处理~~"
      }

      println("res=" + result) //ArrayBuffer(0,1)
    }

  }
}
