package chapter01

object LookSourceCode {
  def main(args: Array[String]): Unit = {

    //看Array的源码
    var arr = new Array[Int](10)
    //对代码进行格式化的快捷键ctrl+alt+l
    for (item <- arr) {
      println("item=" + item)
    }
  }
}
