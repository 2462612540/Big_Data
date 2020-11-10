package chapter04.ifesle

//import scala.io.StdIn //单独的引入一个StdIn

import scala.io._ // _表示将 scala.io 包的所有内容一起引入

object Demo01 {
  def main(args: Array[String]): Unit = {
    println("输入年龄")
    val age = StdIn.readInt()
    if (age > 18) {
      println("age > 18")
    }

    //小的技巧，如何查看某个包下包含的内容
    //1.比如我们想看 scala.io 包有什么内容
    //2.将光标放在 io上即可，输入ctrl +b
    //3.将光标放在 StdIn上即可，输入ctrl +b,看的是StdIn源码
    scala.io.StdIn

  }
}
