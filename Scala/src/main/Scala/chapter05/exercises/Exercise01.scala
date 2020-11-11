package chapter05.exercises

import scala.io.StdIn

object Exercise01 {
  def main(args: Array[String]): Unit = {
    println("请输入数字(1-9)之间")
    val n = StdIn.readInt()
    //调用函数
    print99(n)

    def f1 = "1254555"

    println(f1)
  }

  def f1 = "125455"

  def f2() = {
    "125455"
  }

  //编写一个函数，输出99乘法表
  def print99(n: Int) = {

    for (i <- 1 to n) {
      for (j <- 1 to i) {
        printf("%d * %d = %d\t", j, i, j * i)
      }
      println()
    }
  }
}
