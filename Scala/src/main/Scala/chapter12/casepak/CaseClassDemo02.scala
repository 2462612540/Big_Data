package chapter12.casepak

object CaseClassDemo02 {
  def main(args: Array[String]): Unit = {
    //该案例的作用就是体验使用样例类方式进行对象匹配简洁性
    for (amt <- Array(Dollar2(1000.0), Currency2(1000.0, "RMB"), NoAmount2)) {
      val result = amt match {
        //说明
        case Dollar2(v) => "$" + v // $1000.0
        //说明
        case Currency2(v, u) => v + " " + u // 1000.0 RMB
        case NoAmount2 => "" // ""
      }
      println(amt + ": " + result)
    }

  }
}

abstract class Amount2

case class Dollar2(value: Double) extends Amount2 //样例类
case class Currency2(value: Double, unit: String) extends Amount2 //样例类
case object NoAmount2 extends Amount2 //样例类