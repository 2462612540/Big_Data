package chapter11

object ParDemo02 {
  def main(args: Array[String]): Unit = {

    val result1 = (0 to 100).map { case _ => Thread.currentThread.getName }.distinct
    val result2 = (0 to 100).par.map { case _ => Thread.currentThread.getName }.distinct
    println(result1) //非并行
    println("--------------------------------------------")
    println(result2) //并行
  }
}
