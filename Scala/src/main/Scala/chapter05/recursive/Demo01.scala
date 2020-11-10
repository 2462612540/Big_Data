package chapter05.recursive

object Demo01 {
  def main(args: Array[String]): Unit = {
    test(4) //输出
  }

  def test(n: Int) {
    if (n > 2) {
      test(n + 1)
    }
    println("n=" + n) //
  }

  def test2(n: Int) {
    if (n > 2) {
      test2(n - 1)
    } else {
      println("n=" + n)
    }
  }
}
