package chapter04.myfor

object ForUntilDemo02 {
  def main(args: Array[String]): Unit = {
    //输出10句 "hello,尚硅谷!"
    val start = 1
    val end = 11
    //循环的范围是  start --- (end-1)左边闭合右边开区间
    for (i <- start until end) {
      println("hello, 尚硅谷" + i)
    }
  }
}
