package scala_base

/**
 * Scala 与 Java 有着相同的数据类型，在 Scala 中数据类型都是对象，也就是说 scala 没有 java 中的原生类型
 * Scala 数据类型分为两大类 AnyVal(值类型) 和 AnyRef(引用类型)， 注意：不管是 AnyVal 还是 AnyRef 都
 * 是对象
 */
object DataTypeDemo01 {
  def main(args: Array[String]): Unit = {
    //在 scala 中，一切皆为对象 ，比如(Int,Float,Char....)
    var num1: Int = 10
    println(num1.toString)
    println(100.toFloat)
    var sex = '男'
    println(sex.toString)

    val num: Long = 1000L
  }
}
