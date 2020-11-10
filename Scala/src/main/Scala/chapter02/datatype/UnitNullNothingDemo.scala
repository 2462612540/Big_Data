package chapter02.datatype

object UnitNullNothingDemo {
  def main(args: Array[String]): Unit = {

    val res = sayHello()
    println("res=" + res)

    //Null类只有一个实例对象，null，类似于Java中的null引用。null可以赋值给任意引用类型(AnyRef)，但是不能赋值给值类型(AnyVal: 比如 Int, Float, Char, Boolean, Long, Double, Byte, Short)
    val dog: Dog = null
    //错误
    //    val char1: Char = null
    //    println("ok~~~")

  }

  //Unit等价于java的void,只有一个实例值()
  def sayHello(): Unit = {

  }


}

class Dog {

}
