package chapter04.mywhile

object WhileDemo01 {
  def main(args: Array[String]): Unit = {
    //输出10句 hello,尚硅谷
    //1. 定义循环变量
    var i = 0
    //2. i < 10 条件
    while (i < 10) {
      println("hello,尚硅谷" + i) //循环体
      //循环变量迭代
      i += 1
    }
  }
}
