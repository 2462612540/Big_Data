package chapter09

object ImplicitValDemo03 {
  def main(args: Array[String]): Unit = {

    implicit val str1: String = "jack~" //这个就是隐式值

    //implicit name: String ：name就是隐式参数
    def hello(implicit name: String): Unit = {
      println(name + " hello")
    }

    hello //底层 hello$1(str1);

  }
}
