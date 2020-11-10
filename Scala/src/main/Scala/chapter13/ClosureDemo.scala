package chapter13

object ClosureDemo {
  def main(args: Array[String]): Unit = {

    /*
    请编写一个程序，具体要求如下
    1.编写一个函数 makeSuffix(suffix: String)  可以接收一个文件后缀名(比如.jpg)，     并返回一个闭包
     2.调用闭包，可以传入一个文件名，如果该文件名没有指定的后缀(比如.jpg) ,则返回 文件名.jpg , 如果已经有.jpg后缀，则返回原文件名。
     比如 文件名 是 dog =>dog.jpg
     比如  文件名 是 cat.jpg => cat.jpg
    3.要求使用闭包的方式完成
      提示：String.endsWith(xx)

     */
    //使用并测试
    val f = makeSuffix(".jpg")
    println(f("dog.jpg")) // dog.jpg
    println(f("cat")) // cat.jpg

  }

  def makeSuffix(suffix: String) = {
    //返回一个匿名函数，回使用到suffix
    (filename: String) => {
      if (filename.endsWith(suffix)) {
        filename
      } else {
        filename + suffix
      }
    }
  }
}
