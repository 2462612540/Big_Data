package chapter08

object ApplyDemo01 {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 5)
    println(list)

    val pig = new Pig("小花")

    //使用apply方法来创建对象
    val pig2 = Pig("小黑猪") //自动  apply(pName: String)
    val pig3 = Pig() // 自动触发 apply()

    println("pig2.name=" + pig2.name) //小黑猪
    println("pig3.name=" + pig3.name) //匿名猪猪
  }
}

//案例演示apply方法.
class Pig(pName: String) {
  var name: String = pName
}

object Pig {
  //编写一个apply
  def apply(pName: String): Pig = new Pig(pName)

  def apply(): Pig = new Pig("匿名猪猪")
}
