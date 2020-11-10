package scala_base

/**
 * 1) 字符常量是用单引号(‘ ’ )括起来的单个字符。例如： var c1 = 'a‘ var c2 = '中‘ var c3 = '9'
 * 2) Scala 也允许使用转义字符‘\’来将其后的字符转变为特殊字符型常量。例如： var c3 = ‘\n’ // '\n'表示换行符
 * 3)可以直接给 Char 赋一个整数，然后输出时，会按照对应的 unicode 字符输出 ['\u0061' 97]
 * 4) Char 类型是可以进行运算的，相当于一个整数，因为它都对应有 Unicode 码
 */
object CharType {
  def main(args: Array[String]): Unit = {
    var char1: Char = 'c'
    var char2: Char = 97
    println("cha1=" + char1 + "  char2=" + char2)
    var char3: Char = 'a' //原因是‘a’+ 1 =>97 + 1 =>Int
    var char4: Char = 97 //原因:运算就会有类型 Int=>char
    var char5: Char = 98 //原因，没有运算，编译器只判断范围有没有越界

    println(char3)
  }
}
