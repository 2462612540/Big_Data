package chapter01

//只要以后看到有 object TestScala ，你应该有这样一个认识
//1. object TestScala 对应的是一个  TestScala$的一个静态对象 MODULE$
//2. 在我们的程序中，是一个单例
object TestScala {
  def main(args: Array[String]): Unit = {
    println("hello,scala, idea...")
    var num1: Int = 10
    var num2: Int = 20
    println(num1 + num2)

    println("名字\t年龄\t邮件\t性别")
  }
}
