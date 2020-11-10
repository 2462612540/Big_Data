package chapter11

object FlatMapDemo01 {
  def main(args: Array[String]): Unit = {

    val names = List("Alice", "Bob", "Nick")

    //需求是将List集合中的所有元素，进行扁平化操作，即把所有元素打散
    val names2 = names.flatMap(upper)
    println("names2=" + names2)

  }

  def upper(s: String): String = {
    s.toUpperCase
  }
}
