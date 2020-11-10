package chapter05.fundetails

object VarParameters {
  def main(args: Array[String]): Unit = {
    //编写一个函数sum ,可以求出  1到多个int的和
    println(sum(10, 30, 10, 3, 45, 7))
  }


  def sum(n1: Int, args: Int*): Int = {
    println("args.length" + args.length)
    //遍历
    var sum = n1
    for (item <- args) {
      sum += item
    }
    sum
  }

  //可变参数需要放在最后
  //  def sum2(args: Int*,n1: Int): Int = {
  //    1
  //  }

  def test(): Unit = { //过程

  }
}
