package chapter08.homework

object Exercise02 {
  def main(args: Array[String]): Unit = {
    //测试
    val point = Point(3, 4) //触发apply
    println(point)
  }
}

//class Point(x: Int = 0, y: Int = 0) extends java.awt.Point(x, y)
class Point(x: Int = 0, y: Int = 0) { //Point类
  var x1 = x //属性
  var y1 = y //属性
}

object Point {
  def apply(x: Int = 0, y: Int = 0) = new Point(x, y)
}
