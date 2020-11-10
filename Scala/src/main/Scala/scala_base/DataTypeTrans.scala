package scala_base

/**
 * 这是在scala中一切都是的对象 包括了的这个这个变量 和基本的类型都是的 所以能够直接调用的函数 其他的就是的在使用的是函数 就是说这样的都实现的对函数封装
 *
 * 例如：
 * final abstract class Byte private extends AnyVal {
 * def toByte: Byte
 * def toShort: Short
 * def toChar: Char
 * def toInt: Int
 * def toLong: Long
 * def toFloat: Float
 * def toDouble: Double
 * }
 */
object DataTypeTrans {
  def main(args: Array[String]): Unit = {
    val b: Byte = 10
    println(b.toInt)
    val d: Double = 1.123
    println(d.toByte)
  }
}
