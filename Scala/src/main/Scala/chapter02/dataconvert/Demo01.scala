package chapter02.dataconvert

object Demo01 {
  def main(args: Array[String]): Unit = {
    var n1 = 10
    var n2 = 1.1f
    //1,有多种类型的数据混合运算时，系统首先自动将所有数据转换成容量最大的那种数据类型，然后再进行计算
    var n3 = n1 + n2
    //2.(byte, short) 和 char 之间不会自动的转换类型
    var n4: Byte = 10
    //var char1 : Char = n4 // 错误，因为byte 不能自动转换char
  }
}
