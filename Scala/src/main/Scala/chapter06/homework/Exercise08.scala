package chapter06.homework

object Exercise08 {
  def main(args: Array[String]): Unit = {

    val str = "Hello"
    val substr1 = str.take(1)
    println(substr1) // "H"
    var subStr2 = str.drop(1)
    println(subStr2) // "ello"
  }
}
