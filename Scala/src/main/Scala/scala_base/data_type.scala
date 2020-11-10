package scala_base

object data_type {
  def main(args: Array[String]): Unit = {
    var num: Int = 10
    //因为是Int 是一个类 因此它的一个实例及时可以使用很多的方法
    println(num.toDouble + "---" + num.toString)

    sayhi()
  }

  //在scala中的如果有一个方法 没有形参 则是可以省略的
  def sayhi(): Unit = {
    println("hello word")
  }
}
