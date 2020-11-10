package chapter05.fun

object FunDemo01 {
  def main(args: Array[String]): Unit = {

    val n1 = 10
    val n2 = 20
    println("res=" + getRes(1, 2, ')'))

  }

  //定义函数/方法
  def getRes(n1: Int, n2: Int, oper: Char) = {
    if (oper == '+') {
      n1 + n2 //返回
    } else if (oper == '-') {
      n1 - n2
    } else {
      //返回null
      null
    }
  }
}
