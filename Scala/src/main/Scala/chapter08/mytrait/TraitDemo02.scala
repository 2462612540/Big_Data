package chapter08.mytrait

object TraitDemo02 {
  def main(args: Array[String]): Unit = {
    val c = new C()
    val f = new F()
    c.getConnect() // 连接mysql数据库...
    f.getConnect() // 连接oracle数据库..
  }
}

//按照要求定义一个trait
trait Trait01 {
  //定义一个规范
  def getConnect()
}

//先将六个类的关系写出
class A {}

class B extends A {}

class C extends A with Trait01 {
  override def getConnect(): Unit = {
    println("连接mysql数据库...")
  }
}

class D {}

class E extends D {}

class F extends D with Trait01 {
  override def getConnect(): Unit = {
    println("连接oracle数据库..")
  }
}