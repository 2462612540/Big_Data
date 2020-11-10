package chapter04.mutlfor

object Exercise03 {
  def main(args: Array[String]): Unit = {
    //3.打印出九九乘法表
    //思路分析
    //(1) 使用两层循环, 有9行, 每1行的列数在增加
    //(2) 根据逻辑，我们可以编写代码
    val num = 9
    for (i <- 1 to num) { //确定行数
      for (j <- 1 to i) { //确定列数
        printf("%d * %d = %d\t", j, i, i * j)
      }
      println()
    }
  }
}
