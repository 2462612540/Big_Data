package chapter08.extendtrait

object ExtendTraitDemo01 {
  def main(args: Array[String]): Unit = {
    println("haha~~")
  }
}

//说明
//1. LoggedException 继承了 Exception
//2. LoggedException 特质就可以  Exception 功能
trait LoggedException extends Exception {
  def log(): Unit = {
    println(getMessage()) // 方法来自于Exception类
  }
}

//因为 UnhappyException 继承了 LoggedException
//而 LoggedException 继承了  Exception
//UnhappyException 就成为 Exception子类
class UnhappyException extends LoggedException {
  // 已经是Exception的子类了，所以可以重写方法
  override def getMessage = "错误消息！"
}

// 如果混入该特质的类，已经继承了另一个类(A类)，则要求A类是特质超类的子类，
// 否则就会出现了多继承现象，发生错误。


class UnhappyException2 extends IndexOutOfBoundsException with LoggedException {
  // 已经是Exception的子类了，所以可以重写方法
  override def getMessage = "错误消息！"
}

class CCC {}

//错误的原因是 CCC 不是 Exception子类
//class UnhappyException3 extends CCC with LoggedException{
//  // 已经是Exception的子类了，所以可以重写方法
//  override def getMessage = "错误消息！"
//}

