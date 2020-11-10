package chapter02.datatype

object CharDemo {
  def main(args: Array[String]): Unit = {
    var char1: Char = 97
    //当我们输出一个char类型是，他会输出该数字对应的字符(码值表 unicode)//unicode 码值表包括ascii
    println("char1=" + char1) // a

    //char 可以当做数字进行运行
    var char2: Char = 'a'
    var num = 10 + char2
    println("num=" + num) // 107

    //原因和分析
    //1. 当把一个计算的结果赋值一个变量，则编译器会进行类型转换及判断（即会看范围+类型）
    //2. 当把一个字面量赋值一个变量，则编译器会进行范围的判定
    //    var c2: Char = 'a' + 1
    //    var c3: Char = 97 + 1
    //    var c4: Char = 98

  }
}
