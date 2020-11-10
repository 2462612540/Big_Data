package chapter13

object PartialFun03 {
  def main(args: Array[String]): Unit = {

    //可以将前面的案例的偏函数简写
    def partialFun2: PartialFunction[Any, Int] = {
      //简写成case 语句
      case i: Int => i + 1
      case j: Double => (j * 2).toInt
    }

    val list = List(1, 2, 3, 4, 1.2, 2.4, 1.9f, "hello")
    val list2 = list.collect(partialFun2)
    println("list2=" + list2)

    //第二种简写形式
    val list3 = list.collect {
      case i: Int => i + 1
      case j: Double => (j * 2).toInt
      case k: Float => (k * 3).toInt
    }
    println("list3=" + list3) // (2,3,4,5)

  }
}
