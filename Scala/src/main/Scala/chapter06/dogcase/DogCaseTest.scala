package chapter06.dogcase

object DogCaseTest {
  def main(args: Array[String]): Unit = {
    val dog = new Dog
    dog.name = "tomcat"
    dog.age = 2
    dog.weigth = 6
    println(dog.say())
  }
}

/*
小狗案例

编写一个Dog类，包含name(String)、age(Int)、weight(Double)属性
类中声明一个say方法，返回String类型，方法返回信息中包含所有属性值。

在另一个DogCaseTest类中的main方法中，创建Dog对象，并访问say方法和所有属性，将调用结果打印输出。

 */
class Dog {
  var name = ""
  var age = 0
  var weigth = 0.0

  def say(): String = {
    "小狗信息如下: name=" + this.name + "\t age=" +
      this.age + " weight=" + this.weigth
  }
}
