package scala_base

object outputDemo {
  def main(args: Array[String]): Unit = {
    val name: String = "Tom"
    val age: Int = 10
    val sal: Double = 7.58

    //格式化输出
    //sal=%.2f 保留小数点两位的方式输出(四舍五入)
    printf("name=%s age=%d sal=%.2f\n", name, age, sal)
    //字符串通过$引用(类似 PHP）。
    println(s"name=$name age=${age + 1} sal=$sal sum2=${sum2(23, 90)}")
  }

  /**
   * @example
   * n1=10 n2=10 返回是n1-n2=-10
   * @param n1 形参
   * @param n2 形参
   * @return 返回值
   */
  def sum2(n1: Int, n2: Int): Int = {
    return n1 + n2
  }
}
