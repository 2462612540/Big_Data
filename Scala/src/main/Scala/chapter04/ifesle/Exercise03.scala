package chapter04.ifesle

object Exercise03 {
  def main(args: Array[String]): Unit = {

    var sumVal = 90
    val result =
      if (sumVal > 20) {
        "结果大于20" //()
      }
    println("res=" + result) //返回的是() 即 Unit
  }
}
