package chapter11

object ParDemo01 {
  def main(args: Array[String]): Unit = {

    (1 to 5).foreach(println(_))
    println()
    //这里输出的结果是无序的，说明是将println任务分配给不同cpu
    (1 to 5).par.foreach(println(_))


  }
}
