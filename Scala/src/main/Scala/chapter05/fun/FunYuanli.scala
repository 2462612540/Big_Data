package chapter05.fun

object FunYuanli {
  def main(args: Array[String]): Unit = {
    val n1 = 1
    val n2 = 3
    val res = sum(n1, n2)
    println("res=" + res)
  }

  def sum(n1: Int, n2: Int): Int = {
    return n1 + n2
  }

}
