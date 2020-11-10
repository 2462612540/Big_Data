package chapter05.fundetails

object Details01 {
  def main(args: Array[String]): Unit = {
    //形参列表和返回值列表的数据类型可以是值类型和引用类型
    val tiger = new Tiger
    val tiger2 = test01(10, tiger)
    println(tiger2.name) // jack
    println(tiger.name) // jack
    println(tiger.hashCode() + " " + tiger2.hashCode())

  }

  def test01(n1: Int, tiger: Tiger): Tiger = {
    println("n1=" + n1)
    tiger.name = "jack"
    tiger
  }
}

class Tiger {
  //一个名字属性
  var name = ""
}
