package chapter06.homework

object Exercise04 {
  def main(args: Array[String]): Unit = {

    val n = 3
    val res1 = (0 to n).reverse
    println(res1) //返回了 Range(3, 2, 1, 0)

    //foreach
    //foreach 函数  (f: Int => U),即接收一个输入参数为 Int,输出为U的函数
    //说明
    //1. 将res1的每个元素依次遍历出来，传递给println(x)
    //res1.foreach(println)
    //2. 将res1的每个元素依次遍历出来，传递给myShow(x)
    res1.foreach(myShow)

    //一步搞定
    println("--------------------------")
    (0 to n).reverse.foreach(println)

    //java
    //函数（参数）
    //集合.函数(函数).函数(函数).函数(函数) //符号大数据
  }

  //自己写一个myShow
  def myShow(n: Int): Unit = {
    println("xxxxxxxxxx")
    println(n)
  }

}
