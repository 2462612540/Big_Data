package chapter13

object FunParameter {
  def main(args: Array[String]): Unit = {
    def plus(x: Int) = 3 + x

    //说明
    val result1 = Array(1, 2, 3, 4).map(plus(_))
    println(result1.mkString(",")) //(4,5,6,7)

    //说明
    //1. 在scala中，函数也是有类型，比如plus就是 <function1>
    println("puls的函数类型function1" + (plus _))

  }
}
