package chapter05.myexception

object ThrowDemo {
  def main(args: Array[String]): Unit = {
    //    val res = test()
    //    println(res.toString)
    //如果我们希望在test() 抛出异常后，代码可以继续执行，则我们需要处理
    try {
      test()
    } catch {
      case ex: Exception => {
        println("捕获到异常" + ex.getMessage)
        println("xxxx")
      }
      case ex: ArithmeticException => println("得到一个算术异常~~")
    } finally {
      //写上对try{} 中的资源的分配
    }
    println("ok~~~~~")
  }

  def test(): Nothing = {
    throw new ArithmeticException("算术异常") //Exception("异常NO1出现~")
  }
}
