package chapter11

object ReduceExercise01 {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5)

    def minus(num1: Int, num2: Int): Int = {
      num1 - num2
    }

    // (((1-2) - 3) - 4) - 5 = -13
    println(list.reduceLeft(minus)) // 输出? -13
    // 1 - (2 - (3 -(4 - 5))) = 3
    println(list.reduceRight(minus)) //输出? 3
    // reduce 等价于 reduceLeft
    println(list.reduce(minus))

    println("minval=" + list.reduceLeft(min)) // 1
  }

  //求出最小值
  def min(n1: Int, n2: Int): Int = {
    if (n1 > n2) n2 else n1
  }
}
