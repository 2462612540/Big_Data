package chapter13

object CurryDemo02 {
  def main(args: Array[String]): Unit = {

    //这是一个函数，可以接收两个字符串，比较是否相等
    def eq(s1: String, s2: String): Boolean = {
      s1.equals(s2)
    }

    //隐式类
    implicit class TestEq(s: String) {
      //体现了将比较字符串的事情，分解成两个任务完成
      //1. checkEq 完转换大小写
      //2. f函数完成比较任务
      def checkEq(ss: String)(f: (String, String) => Boolean): Boolean = {
        f(s.toLowerCase, ss.toLowerCase)
      }
    }
    val str1 = "hello"
    println(str1.checkEq("HeLLO")(eq))

    //在看一个简写形式
    println(str1.checkEq("HeLLO")(_.equals(_)))


  }
}
