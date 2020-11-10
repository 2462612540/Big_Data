package chapter13

object CurryDemo01 {
  def main(args: Array[String]): Unit = {

    def mulCurry2(x: Int)(y: Int) = x * y

    println(mulCurry2(10)(8))


  }
}
