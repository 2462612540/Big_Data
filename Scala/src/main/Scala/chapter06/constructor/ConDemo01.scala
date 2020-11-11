package chapter06.constructor

object ConDemo01 {
  def main(args: Array[String]): Unit = {
    //    val p1 = new Person("jack", 20)
    //    println(p1)
    //    val a = new A
    //    val a2 = new A()
    //下面这句话就会调用def this(name:String)
    val p2 = new Person("tom")
    println(p2)
  }
}

//构造器的快速入门
//创建Person对象的同时初始化对象的age属性值和name属性值
class Person(inName: String, inAge: Int) {
  var name: String = inName
  var age: Int = inAge
  age += 10
  println("~~~~~~~~~~")

  def this(name: String) {
    //辅助构造器，必须在第一行显式调用主构造器(可以是直接，也可以是间接)
    this("jack", 10)
    //this
    this.name = name //重新赋值
  }

  //重写了toString，便于输出对象的信息
  override def toString: String = {
    "name=" + this.name + "\t age" + this.age
  }

  println("ok~~~~~")
  println("age=" + age)
}

class A() {

}
