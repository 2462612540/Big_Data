package chapter02.datatype

object TyepDemo03 {
  def main(args: Array[String]): Unit = {
    println("long的最大值" + Long.MaxValue + "~" + Long.MinValue)

    var i = 10 //i Int
    var j = 10l //j Long
    var e = 9223372036854775807l //说 9223372036854775807 超过int

    //2.2345678912f  , 2.2345678912
    var num1: Float = 2.2345678912f
    var num2: Double = 2.2345678912
    println("num1=" + num1 + "num2=" + num2)
  }


  def test() = {
    var s: Short = 5 // ok
    //s = s - 2 //  error  Int -> Short
    var b: Byte = 3 // ok
    //    b = b + 4 // error Int ->Byte
    //b = (b + 4).toByte // 错误
    var c: Char = 'a' //ok
    var i: Int = 5 //ok
    var d: Float = .314F //ok
    var result: Double = c + i + d //ok Float->Double
    //    var b: Byte = 5 // ok
    //    var s: Short = 3 //ok
    //    var t: Short = s + b // error Int->Short
    //    var t2 = s + b // ok, 使用类型推导

  }
}
