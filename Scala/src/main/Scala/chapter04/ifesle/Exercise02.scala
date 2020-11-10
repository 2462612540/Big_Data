package chapter04.ifesle

import scala.math._ // _ 表示将scala.math 的所有内容导入
object Exercise02 {
  def main(args: Array[String]): Unit = {

    /*
    求ax2+bx+c=0方程的根。a,b,c分别为函数的参数，如果：b2-4ac>0，则有两个解；
b2-4ac=0，则有一个解；b2-4ac<0，则无解； [a=3 b=100 c=6]
提示1：x1=(-b+sqrt(b2-4ac))/2a
               X2=(-b-sqrt(b2-4ac))/2a

提示2：sqrt(num)  在 scala 包中(默认引入的) 的math 的包对象有很多方法直接可用.

   思路的分析
   1. 定义三个变量a,b,c
   2. 使用多分支完成
   3. 因为 b2-4ac会多次使用，因此我们可以先计算，并保持到变量中
   4. 判断，写逻辑
     */
    val a = 3
    val b = 100
    val c = 6
    val m = b * b - 4 * a * c
    var x1 = 0.0
    var x2 = 0.0
    if (m > 0) {
      x1 = (-b + sqrt(m)) / 2 * a
      x2 = (-b - sqrt(m)) / 2 * a
      println("有两个解 x1=" + x1.formatted("%.2f") + "x2=" + x2.formatted("%.2f"))
    } else if (m == 0) {
      x1 = (-b + sqrt(m)) / 2 * a
      println("有一个解 x1=" + x1)
    } else {
      println("无解..")
    }
  }
}
