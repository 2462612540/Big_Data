package chapter03.inputcon

import scala.io.StdIn

object Demo01 {


  def main(args: Array[String]): Unit = {
    /*
    要求：可以从控制台接收用户信息，【姓名，年龄，薪水】
     */
    println("请输入姓名")
    val name = StdIn.readLine()
    println("请输入年龄")
    val age = StdIn.readInt()
    println("请输入薪水")
    val sal = StdIn.readDouble()
    printf("用户的信息为 name=%s age=%d sal=%.2f", name, age, sal)


    //Cat.sayHi()
    //Cat.sayHello()
    //Cat.sayHi()
    hi()

  }

  //定义方法
  def hi(): Unit = {
    println("hi")
  }
}

//声明了一个对象(伴生对象), 讲解oop时，还要深入系统的讲解
object Cat extends AAA {
  //方法
  def sayHi(): Unit = {
    println("小狗汪汪叫....")
  }
}

trait AAA { //AAA是特质，等价于java中的interface + abstract class
  def sayHello(): Unit = {
    println("AAA sayHello")
  }
}
