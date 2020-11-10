package chapter06.method

object MethodDemo01 {
  def main(args: Array[String]): Unit = {
    //使用一把
    val dog = new Dog
    println(dog.cal(10, 20))
  }
}

class Dog {
  var food: String = _
  private var sal: Double = _

  //方法
  def cal(n1: Int, n2: Int): Int = {
    return n1 + n2
  }
}

