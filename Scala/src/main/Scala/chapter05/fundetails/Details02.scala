package chapter05.fundetails

object Details02 {
  def main(args: Array[String]): Unit = {

    println(getSum2(10, 30)) // ()

    println(getSum3(9, 9)) //()

  }

  //如果返回值这里什么什么都没有写，即表示该函数没有返回值
  //这时return无效
  def getSum2(n1: Int, n2: Int) {
    return n1 + n2
  }

  //如果函数明确声明无返回值（声明Unit），那么函数体中即使使用return关键字也不会有返回值
  def getSum3(n1: Int, n2: Int): Unit = {
    return n1 + n2
  }

  //如果写了return ,返回值类型就不能省略
  def getSum(n1: Int, n2: Int): Int = {
    return n1 + n2
  }
}

