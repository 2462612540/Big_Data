package chapter12

object MatchTypeDemo01 {
  def main(args: Array[String]): Unit = {
    val a = 8
    //说明 obj 实例的类型 根据 a 的值来返回
    val obj = if (a == 1) 1
    else if (a == 2) "2"
    else if (a == 3) BigInt(3)
    else if (a == 4) Map("aa" -> 1)
    else if (a == 5) Map(1 -> "aa")
    else if (a == 6) Array(1, 2, 3)
    else if (a == 7) Array("aa", 1)
    else if (a == 8) Array("aa")

    //说明
    //1. 根据  obj 的类型来匹配
    // 返回值
    val result = obj match {

      case a: Int => a
      case b: Map[String, Int] => "对象是一个字符串-数字的Map集合"
      case c: Map[Int, String] => "对象是一个数字-字符串的Map集合"
      case d: Array[String] => d //"对象是一个字符串数组"
      case e: Array[Int] => "对象是一个数字数组"
      case f: BigInt => Int.MaxValue
      case y: Float => println("xx")
      case _ => "啥也不是"
    }

    println(result)

  }
}
