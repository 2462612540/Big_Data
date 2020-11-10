package chapter10

object CollectionDemo01 {
  def main(args: Array[String]): Unit = {

    val str = "hello" //字符串在scala就是Char的集合 IndexedSeq
    for (item <- str) {
      println(item)
    }

    println(str(2)) // l
  }
}
